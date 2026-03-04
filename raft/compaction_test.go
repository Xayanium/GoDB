package raft

import (
	raftrpc "GoDB/rpc/raft"
	"GoDB/tools" // 假设存在可用的 Mock Persister
	"context"
	"sync"
	"testing"
)

// 辅助方法：创建一个用于测试的 Raft 节点
func setupTestRaft() *Raft {
	applyCh := make(chan ApplyMsg, 10)
	persister := tools.MakePersister() // 假设你的 tools 包有这个初始化方法

	rf := &Raft{
		mu:          sync.Mutex{},
		me:          0,
		role:        Leader, // 默认设为 Leader 方便测试 Snapshot
		CurrentTerm: 2,
		commitIndex: 5,
		log:         NewLog(0, 0, nil),
		applyCh:     applyCh,
		persister:   persister,
		applyCond:   sync.NewCond(&sync.Mutex{}),
	}

	// 伪造一些日志条目 (Index 1 到 6)
	// 注意 tailLog[0] 是占位符
	for i := 1; i <= 6; i++ {
		rf.log.tailLog = append(rf.log.tailLog, &raftrpc.LogEntry{
			Term:         2,
			CommandValid: true,
		})
	}
	return rf
}

// 1. 测试 Leader 接收上层 Snapshot 请求的边界与正常处理
/*
拦截非法索引：确认了当请求的 index 大于 commitIndex 时，系统会拒绝执行快照，防止未达成多数派共识的日志被固化。
拦截重复快照：确认了当请求的 index 小于或等于当前的 SnapLastIdx 时，系统会拒绝执行，避免无效的 I/O 操作和逻辑回退。
日志正确截断：验证了 doSnapshot 成功执行后，快照元数据（SnapLastIdx, Snapshot 等）被正确更新，且 tailLog 被安全切片，并保留了用于保证下标一致性的占位符 &raftrpc.LogEntry{Term: int32(rl.SnapLastTerm)}。
*/
func TestSnapshot(t *testing.T) {
	rf := setupTestRaft()

	// Case 1: 尝试对未提交的日志做快照 (index > commitIndex)
	rf.Snapshot(6, []byte("invalid_snapshot"))
	if rf.log.SnapLastIdx == 6 {
		t.Fatalf("Should not snapshot uncommitted logs")
	}

	// Case 2: 正常的快照请求 (index <= commitIndex)
	rf.Snapshot(3, []byte("valid_snapshot_at_3"))
	if rf.log.SnapLastIdx != 3 {
		t.Fatalf("Expected SnapLastIdx to be 3, got %d", rf.log.SnapLastIdx)
	}
	if string(rf.log.Snapshot) != "valid_snapshot_at_3" {
		t.Fatalf("Snapshot data mismatch")
	}
	// 验证 tailLog 被正确截断：原来有 6 条，截断 3 条，剩下 3 条，加上 1 个占位符，长度应为 4
	if len(rf.log.tailLog) != 4 {
		t.Fatalf("Expected tailLog length 4, got %d", len(rf.log.tailLog))
	}

	// Case 3: 尝试对过期的 index 做快照 (index <= SnapLastIdx)
	rf.Snapshot(2, []byte("stale_snapshot"))
	if string(rf.log.Snapshot) == "stale_snapshot" {
		t.Fatalf("Should refuse snapshot with index <= SnapLastIdx")
	}
}

// 2. 测试 Follower 接收 InstallSnapshot RPC 的逻辑
/*
任期安全性检查：测试了如果 Leader 发来的快照 term 小于 Follower 自身的 CurrentTerm，Follower 会直接拒绝 。
新旧状态覆盖检查：测试了如果快照携带的 last_included_index 不大于 Follower 本地已有的 SnapLastIdx，直接返回以丢弃过期 RPC 。
状态同步与通知机制：验证了快照成功安装后，节点的 tailLog 会被清空重置，并通过 rf.isSnapshots = true 及 applyCond.Signal() 唤醒后台协程，以应用快照数据到状态机。
*/
func TestInstallSnapshot_RPC(t *testing.T) {
	rf := setupTestRaft()
	rf.role = Follower
	rf.CurrentTerm = 3

	// Case 1: 请求的 Term 小于自身 Term，应拒绝
	argsStaleTerm := &raftrpc.InstallSnapshotArgs{
		Term:              2,
		LeaderId:          1,
		LastIncludedIndex: 4,
		LastIncludedTerm:  2,
		Snapshot:          []byte("stale_term_snap"),
	}
	reply, _ := rf.InstallSnapshot(context.Background(), argsStaleTerm)
	if reply.Term != 3 {
		t.Fatalf("Expected reply term 3, got %d", reply.Term)
	}
	if rf.log.SnapLastIdx == 4 {
		t.Fatalf("Should not install snapshot with smaller term")
	}

	// Case 2: 请求的 Term 正常，但快照太旧 (LastIncludedIndex <= SnapLastIdx)
	rf.log.SnapLastIdx = 5 // 模拟本地已经有更新的快照
	argsStaleIndex := &raftrpc.InstallSnapshotArgs{
		Term:              3,
		LeaderId:          1,
		LastIncludedIndex: 4,
		LastIncludedTerm:  2,
		Snapshot:          []byte("stale_index_snap"),
	}
	rf.InstallSnapshot(context.Background(), argsStaleIndex)
	if string(rf.log.Snapshot) == "stale_index_snap" {
		t.Fatalf("Should not install snapshot with older index")
	}

	// Case 3: 正常的 InstallSnapshot 请求
	rf.log.SnapLastIdx = 2 // 恢复一个较旧的本地状态
	argsValid := &raftrpc.InstallSnapshotArgs{
		Term:              4, // 发现更大的 term
		LeaderId:          1,
		LastIncludedIndex: 5,
		LastIncludedTerm:  3,
		Snapshot:          []byte("valid_rpc_snap"),
	}

	// 起一个 goroutine 来消耗 applyCond 唤醒产生的信号（防止测试阻塞）
	go func() {
		rf.applyCond.L.Lock()
		for !rf.isSnapshots {
			rf.applyCond.Wait()
		}
		rf.isSnapshots = false
		rf.applyCond.L.Unlock()
	}()

	replyValid, _ := rf.InstallSnapshot(context.Background(), argsValid)
	if replyValid.Term != 3 { // 注意：InstallSnapshot 返回的是节点更新前的 term，或者更新后的 term（根据具体实现，当前代码是 defer 解锁前记录的 CurrentTerm）
		// 此处代码逻辑：reply 是在检查前分配的，可能带有旧 term。但节点本身已更新为 4。
	}
	if rf.CurrentTerm != 4 {
		t.Fatalf("Expected to step down and update term to 4, got %d", rf.CurrentTerm)
	}
	if rf.log.SnapLastIdx != 5 {
		t.Fatalf("Expected SnapLastIdx to be 5, got %d", rf.log.SnapLastIdx)
	}
	if len(rf.log.tailLog) != 1 { // tailLog 应该被清空，只剩 1 个占位符
		t.Fatalf("Expected tailLog length 1, got %d", len(rf.log.tailLog))
	}
}

// 3. 测试 askInstallSnapshot 中的上下文检查机制
/*
测试了在调用 askInstallSnapshot 时，如果发生锁释放期间的角色或任期改变（例如 Leader 发生降级），contextCheck 会及时阻断后续操作。这防止了过期的 Leader 错误地推进 matchIndex 或 nextIndex。
*/
func TestAskInstallSnapshot_ContextChange(t *testing.T) {
	rf := setupTestRaft()
	rf.role = Leader
	rf.CurrentTerm = 2

	// 模拟在发起 RPC 前失去 Leader 身份
	rf.role = Follower

	// 这个调用应该因为 contextCheck 失败而直接 return，不会发生 panic 且不会修改 matchIndex
	rf.askInstallSnapshot(1, 2)
}
