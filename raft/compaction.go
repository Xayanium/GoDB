package raft

import (
	raftrpc "GoDB/rpc/raft"
	"context"
	"fmt"
)

// ============================================================================
// Raft 对外（上层服务）接口
// ============================================================================

// Snapshot 告诉 Raft层：上层已将状态机做到了 index 快照，Raft层 可将 <=index 的日志删除掉（节省存储空间）
// 快照中存的是上层状态机的数据，Raft 只负责管理快照元数据，并进行快照内容持久化
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// index 检查，不能对未提交的日志做快照，不能重复快照
	if index > rf.commitIndex {
		LOG(rf.me, rf.CurrentTerm, DSnap, fmt.Sprintf("Peer_%d(Leader) refused doSnapshot, snapshotIdx: %d < commitIndex: %d", rf.me, index, rf.commitIndex))
		return
	}
	if index <= rf.log.SnapLastIdx {
		LOG(rf.me, rf.CurrentTerm, DSnap, fmt.Sprintf("Peer_%d(Leader) refused doSnapshot, snapshotIdx: %d <= lastSnapIdx: %d", rf.me, index, rf.log.SnapLastIdx))
		return
	}

	// leader 安装快照
	rf.doSnapshot(index, snapshot)
}

// ============================================================================
// Raft 快照相关方法
// ============================================================================

// Leader 向某个 Follower 发送 InstallSnapshot 请求，成功后更新 matchIndex/nextIndex
func (rf *Raft) askInstallSnapshot(peer int, term int) {
	rf.mu.Lock()
	// 上下文检查，由于 askInstallSnapshot 调用前后锁不连续，节点的角色/任期可能已发生变化，只有在上下文未改变的情况下才发起 RPC 请求
	if !rf.contextCheck(Leader, term) {
		LOG(rf.me, rf.CurrentTerm, DWarn, fmt.Sprintf("Peer_%d Leader context changed, stop Install Snapshot to Peer_%d", rf.me, peer))
		rf.mu.Unlock()
		return
	}

	args := &raftrpc.InstallSnapshotArgs{
		Term:              int32(rf.CurrentTerm),
		LeaderId:          int32(rf.me),
		LastIncludedIndex: int32(rf.log.SnapLastIdx),
		LastIncludedTerm:  int32(rf.log.SnapLastTerm),
		Snapshot:          rf.log.Snapshot,
	}
	client := rf.peers[peer]
	rf.mu.Unlock()

	// 1. 发送 RPC请求，收到回复后加锁
	reply, err := client.InstallSnapshot(context.Background(), args)
	if err != nil {
		LOG(rf.me, rf.CurrentTerm, DError, fmt.Sprintf("Peer_%d Failed to send InstallSnapshot RPC to Peer_%d: %v", rf.me, peer, err))
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 2. 任期检查，如果发现回复中的 term 比自己高，自己退为 follower
	if int(reply.Term) > rf.CurrentTerm {
		rf.becomeFollower(int(reply.Term))
		LOG(rf.me, rf.CurrentTerm, DWarn, fmt.Sprintf("Found higher Term_%d from Peer_%d", args.Term, args.LeaderId))
		return
	}

	// 3. 上下文检查，由于发送RPC前后锁不连续，节点的角色/任期可能已发生变化，只有在上下文未改变的情况下才处理 RPC 响应
	if !rf.contextCheck(Leader, term) {
		LOG(rf.me, rf.CurrentTerm, DWarn, fmt.Sprintf("Peer_%d Leader context changed, ignore InstallSnapshot reply from Peer_%d", rf.me, peer))
		return
	}

	// 4. 更新 Leader 保存的 Follower 的日志复制进度（matchIndex/nextIndex）
	lastSnapIdx := int(args.LastIncludedIndex)
	// 由于 follower 已经安装了 覆盖到 LastIncludedIndex 的快照，leader 可将 matchIndex 设置到 snapshot 的边界
	// 后续 leader 调用 AppendEntries 的时候可直接发送从快照之后的日志
	if lastSnapIdx > rf.matchIndex[peer] {
		LOG(rf.me, rf.CurrentTerm, DInfo, fmt.Sprintf(
			"Peer_%d update matchIndex: %d -> %d, nextIndex: %d -> %d for Peer_%d fter InstallSnapshot",
			rf.me, peer, rf.matchIndex[peer], lastSnapIdx, rf.nextIndex[peer], lastSnapIdx+1))
		rf.matchIndex[peer] = lastSnapIdx
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
	}
}

// InstallSnapshot Leader 发送快照RPC请求时，Follower 如何处理
func (rf *Raft) InstallSnapshot(ctx context.Context, args *raftrpc.InstallSnapshotArgs) (reply *raftrpc.InstallSnapshotReply, err error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply = &raftrpc.InstallSnapshotReply{Term: int32(rf.CurrentTerm)}

	// 任期检查，确保 leader term 不小于自己，自己处于 follower 状态
	if int(args.Term) < rf.CurrentTerm {
		return
	} else {
		rf.becomeFollower(int(args.Term))
	}

	// 检查Leader要求安装的快照是否比自己新
	if int(args.LastIncludedIndex) <= rf.log.SnapLastIdx {
		return
	}

	// follower 安装快照
	rf.installSnapshot(args)
	return
}

// ============================================================================
// Raft 快照相关辅助方法
// ============================================================================

// leader 安装快照（更新 log 中快照相关信息，删除已快照的日志部分，并持久化）
func (rf *Raft) doSnapshot(index int, snapshot []byte) {
	rl := rf.log
	rl.Snapshot = snapshot
	rl.SnapLastTerm = int(rl.get(index).Term)

	// leader 安装上层传递的快照后，删除本地已快照的部分
	newLog := make([]*raftrpc.LogEntry, 0)
	newLog = append(newLog, &raftrpc.LogEntry{Term: int32(rl.SnapLastTerm)})

	newLog = append(newLog, rl.tailLog[rl.idx(index)+1:]...)
	rl.tailLog = newLog
	rl.SnapLastIdx = index // 由于 rl.idx() 和 rl.get() 都会用上SnapLastIdx，所以最后更改这个值

	rf.persist()

	// leader 的 Raft 层接收上层状态机传递的快照，直接安装到本地并按需同步给follower，不需要再让上层状态机应用
}

// follower 安装快照（更新 log 中快照相关信息，并持久化，同时通过 applyCh 通知上层应用快照内容）
func (rf *Raft) installSnapshot(args *raftrpc.InstallSnapshotArgs) {
	rl := rf.log
	rl.Snapshot = args.Snapshot
	rl.SnapLastIdx = int(args.LastIncludedIndex)
	rl.SnapLastTerm = int(args.LastIncludedTerm)
	// follower 安装 leader 快照后，为保证一致性，删除自己本地的 tailLog，等待下次 leader 发送 AppendEntries 时再追加快照之后的日志
	newLog := make([]*raftrpc.LogEntry, 0)
	newLog = append(newLog, &raftrpc.LogEntry{Term: args.LastIncludedTerm})
	rl.tailLog = newLog

	rf.persist()

	// follower 的 Raft 层必须通知上层服务应用这个新的快照，使上层状态机与Leader的状态保持一致
	rf.isSnapshots = true
	rf.applyCond.Signal()
}
