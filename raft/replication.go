package raft

import (
	"GoDB/config"
	raftrpc "GoDB/rpc/raft"
	"context"
	"fmt"
	"sort"
	"time"
)

// 日志复制 ticker，Leader 当选后启动该协程循环，定期向所有 follower 进行心跳或日志复制，并更新相关字段（nextIndex、matchIndex）
func (rf *Raft) replicationTicker(term int) {
	cfg := config.Get()
	for !rf.isKilled() {
		rf.startReplication(term)
		time.Sleep(time.Duration(cfg.Raft.HeartbeatInterval) * time.Millisecond) // 心跳间隔
	}
}

// ============================================================================
// Raft 日志复制相关方法
// ============================================================================

// Leader 并发向所有 peer 进行 日志复制RPC 请求，更新 nextIndex、matchIndex 字段、判断日志是否复制到多数节点（日志状态变为Commit）
func (rf *Raft) startReplication(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. 上下文检查，由于 askVote 和 startReplication 锁不连续，节点的角色/任期可能已发生变化，只有在上下文未改变的情况下才发起 RPC 请求
	if !rf.contextCheck(Leader, term) {
		LOG(rf.me, rf.CurrentTerm, DWarn, fmt.Sprintf("Peer_%d Leader context changed, stop Replication", rf.me))
		return
	}

	// 2. 对每个 peer 并发发送 AppendEntries RPC 请求
	for peer := 0; peer < len(rf.peers); peer++ {
		// Leader 不需要向自己发送日志复制请求，直接更改 nextIndex 和 matchIndex 字段
		if peer == rf.me {
			rf.matchIndex[peer] = rf.log.size()
			rf.nextIndex[peer] = rf.log.size() + 1
			continue
		}

		// 如果要发给 Follower 的日志，在 Leader 中已经合入了快照，Leader 只能发送快照给 Follower 进行安装，而不能发送日志复制请求
		lastFollowerIdx := rf.nextIndex[peer] - 1 // Leader 认为已同步给 Follower 的最后一条日志（在Leader本地日志中的全局Index）
		if lastFollowerIdx <= rf.log.SnapLastIdx {
			go rf.askInstallSnapshot(peer, rf.CurrentTerm) // 调用 InstallSnapshot RPC 方法，发送快照给 Follower 进行安装
			continue
		}

		// 正常发送日志复制请求
		args := &raftrpc.AppendEntriesArgs{
			Term:         int32(rf.CurrentTerm),
			LeaderId:     int32(rf.me),
			PrevLogIndex: int32(lastFollowerIdx),
			PrevLogTerm:  rf.log.get(lastFollowerIdx).Term,
			Entries:      rf.log.tail(lastFollowerIdx),
			LeaderCommit: int32(rf.commitIndex),
		}
		go rf.askReplication(peer, args, term)
	}
}

/*
情况1：
index:         1 2 3 4 5 6 7
Leader term:   1 1 2 2 3 3 4
Follower term: 1 1 2
nextIndex = 6, PrevLogIndex = 5, PrevLogTerm  = 3
ConflictTerm = InvalidTerm, ConflictIndex = 4
更新 nextIndex = 4

情况2.1：
index:          1 2 3 4 5 6 7 8
Leader term:    1 1 2 2 3 3 4 4
Follower term： 1 1 2 2 3 3 3
nextIndex = 8， PrevLogIndex = 7, PrevLogTerm  = 4
ConflictTerm = 3, ConflictIndex = 5
leader 存在 term=3，更新 nextIndex = 5

情况2.2：
index:         1 2 3 4 5 6 7
Leader term:   1 1 2 2 4 4 5
Follower term: 1 1 2 2 3 3 3
nextIndex = 7, PrevLogIndex = 6, PrevLogTerm  = 4
ConflictTerm = 3, ConflictIndex = 5
leader 无 term=3，更新 nextIndex = 5
*/
// 给单个 peer 进行日志复制RPC，并处理返回结果，用于startReplication()
func (rf *Raft) askReplication(peer int, args *raftrpc.AppendEntriesArgs, term int) {
	// 1. 发送 AppendEntries RPC 请求
	client := rf.peers[peer]
	reply, err := client.AppendEntries(context.Background(), args)
	if err != nil {
		LOG(rf.me, rf.CurrentTerm, DError, fmt.Sprintf("Peer_%d Failed to send AppendEntries RPC to Peer_%d: %v", rf.me, peer, err))
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 2. 任期检查
	if int(reply.Term) > rf.CurrentTerm {
		rf.becomeFollower(int(reply.Term))
		LOG(rf.me, rf.CurrentTerm, DWarn, fmt.Sprintf("Found higher Term_%d from Peer_%d", args.Term, args.LeaderId))
		return
	}

	// 3. 上下文检查，由于 askReplication 和 startReplication 锁不连续，节点的角色/任期可能已发生变化，只有在上下文未改变的情况下才处理 RPC 响应
	if !rf.contextCheck(Leader, term) {
		LOG(rf.me, rf.CurrentTerm, DWarn, fmt.Sprintf("Peer_%d Leader context changed, ignore AppendEntries reply from Peer_%d", rf.me, peer))
		return
	}

	// 4.1 如果 leader 的 prevIndex 落在 follower 的快照截断区域，需要进行快照安装（快照安装后自动更新 nextIndex）
	if reply.NeedSnapshot {
		go rf.askInstallSnapshot(peer, rf.CurrentTerm)
		return
	}

	// 4.2 如果日志复制失败，利用响应的 conflictTerm 和 conflictIndex 回退 leader 的 nextIndex，等待下一次 replicationTicker 进行重试
	if !reply.Success {
		var newNextIndex int

		if reply.ConflictTerm == InvalidTerm {
			// 情况1：follower 日志太短，无 leader 请求的 prevLogIndex 处的日志
			// leader 需从 follower 最后一条日志之后开始复制，回退 nextIndex 到 follower lastIndex+1 的位置
			newNextIndex = int(reply.ConflictIndex + 1) // 此时 reply.ConflictIndex 是 follower lastIndex
		} else {
			// 情况2：follower 存在 leader 请求的 prevLogIndex 处的日志，但两者日志 term 不同（follower 的日志被另一个已经失效的 leader 更新过）
			// follower 返回自己冲突日志的 index 和 term（ConflictIndex、ConflictTerm）
			firstIndex := rf.log.firstFor(int(reply.ConflictTerm)) // 此时的 reply.ConflictIndex 是 follower 日志中第一条任期为 conflictTerm 的日志 index

			if firstIndex != InvalidIndex {
				// 情况2.1：leader 存在任期为 follower conflictTerm 的日志（两者在 prevLogIndex 处日志冲突，但 conflictTerm 内有部分日志相同）
				// 回退 nextIndex 到 leader 第一条任期为 conflictTerm 的日志
				newNextIndex = firstIndex
			} else {
				// 情况2.2：leader 不存在任期为 follower conflictTerm 的日志（两者在 prevLogIndex 处日志冲突，且 conflictTerm 内无相同日志）
				//（follower曾跟过一个已经失效的 leader，这个 leader 的日志没有成功复制到多数节点）
				// follower 从 conflictTerm 之后的所有日志都需被删除（follower幽灵任期），回退 nextIndex 到 conflictIndex 位置
				newNextIndex = int(reply.ConflictIndex) // 此时的 reply.ConflictIndex 是 follower 日志中第一条任期为 conflictTerm 的日志 index
			}
		}

		// 避免因乱序接收到的多个 reply.ConflictIndex，错误更新 nextIndex
		// （同时发多轮 AppendEntries 给 同一个 peer，晚发的回复已将 nextIndex 回退到更小位置，早发的回复才到，可能将 nextIndex 更新到更大位置）
		if newNextIndex < rf.nextIndex[peer] {
			rf.nextIndex[peer] = newNextIndex
		}

		LOG(rf.me, rf.CurrentTerm, DWarn, fmt.Sprintf("Peer_%d Replication to Peer_%d failed, back to nextIndex_%d; "+
			"conflictTerm: %d, conflictIndex: %d", rf.me, peer, rf.nextIndex[peer], reply.ConflictTerm, reply.ConflictIndex))
		return
	}

	// 4.3 如果日志复制成功，更新 nextIndex 和 matchIndex 字段，并判断日志是否复制到多数节点
	rf.matchIndex[peer] = int(args.PrevLogIndex) + len(args.Entries)
	rf.nextIndex[peer] = rf.matchIndex[peer] + 1

	// leader 只有在当前任期的日志条目被多数节点复制后才可推进 commitIndex
	majorIdx := rf.getMajorityIdx()
	if majorIdx > rf.commitIndex && rf.log.get(majorIdx).Term == int32(rf.CurrentTerm) {
		// 如果日志复制到多数节点（成功提交），更新 commitIndex 字段，通过 applyCh 通知Leader自己的上层服务应用集群新提交的日志
		LOG(rf.me, rf.CurrentTerm, DCommit, fmt.Sprintf("Index Commit: index_%d -> index_%d", rf.commitIndex, majorIdx))
		rf.commitIndex = majorIdx
		rf.applyCond.Signal()
	}
}

// AppendEntries peer 接收到 Leader 的 AppendEntries RPC 请求，并进行处理（作为 RPC Server 端，处理 Client 端的请求）
func (rf *Raft) AppendEntries(ctx context.Context, args *raftrpc.AppendEntriesArgs) (*raftrpc.AppendEntriesReply, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply := &raftrpc.AppendEntriesReply{
		Term:          int32(rf.CurrentTerm),
		Success:       false,
		ConflictTerm:  InvalidTerm,
		ConflictIndex: InvalidIndex,
	}

	// 1. 任期检查：Leader term 更小则拒绝请求，term 更大则更新节点自己 term，并将角色变为 follower
	if int(args.Term) < rf.CurrentTerm {
		LOG(rf.me, rf.CurrentTerm, DWarn, fmt.Sprintf("Peer_%d Replication Failed, Term_%d > Leader Term_%d", rf.me, rf.CurrentTerm, args.Term))
		return reply, nil
	}
	if int(args.Term) > rf.CurrentTerm {
		rf.becomeFollower(int(args.Term))
	}

	// 2. 日志匹配检查
	// 情况A：Follower 日志太短，PrevLogIndex 超过 Follower 日志末尾
	if int(args.PrevLogIndex) > rf.log.size() {
		reply.ConflictTerm = InvalidTerm
		index, _ := rf.log.last()
		reply.ConflictIndex = int32(index)
		return reply, nil
	}

	// 情况B：Leader 的 PrevLogIndex 落在 Follower 已快照截断的区域
	if int(args.PrevLogIndex) <= rf.log.SnapLastIdx {
		reply.ConflictTerm = int32(rf.log.SnapLastTerm)
		reply.ConflictIndex = int32(rf.log.SnapLastIdx)
		reply.NeedSnapshot = true // 告诉 Leader 需要发送快照给 Follower 进行安装
		return reply, nil
	}

	// 情况C：Follower 在 PrevLogIndex 有条目，但 term 不等于 Leader 认为的 term
	entry := rf.log.get(int(args.PrevLogIndex))
	if entry.Term != args.PrevLogTerm {
		reply.ConflictTerm = entry.Term                               // follower 日志中与 leader 冲突的 term
		reply.ConflictIndex = int32(rf.log.firstFor(int(entry.Term))) // follower 日志中第一条任期为 ConflictTerm 的日志 index
		return reply, nil
	}

	// 3. 通过检查，追加日志（或覆盖冲突日志），持久化相关字段，
	reply.Success = true
	rf.log.append(int(args.PrevLogIndex), args.Entries)
	rf.persist()

	// 4. 利用 Leader commitIndex 推进自己的 commitIndex，并通过 applyCh 通知Follower自己对应的上层服务应用集群新提交的日志
	if int(args.LeaderCommit) > rf.commitIndex {
		rf.commitIndex = int(args.LeaderCommit)
		rf.applyCond.Signal()
	}

	// 5. 重置选举计时器（防止自己再次参加选举）
	rf.resetElectionTimer()

	LOG(rf.me, rf.CurrentTerm, DLog, fmt.Sprintf("Peer_%d Replication from Leader Peer_%d in Term_%d", rf.me, args.LeaderId, rf.CurrentTerm))
	return reply, nil
}

// ============================================================================
// 日志复制相关辅助方法
// ============================================================================

// 利用 matchIndex[] 算出多数 peer 已复制到的最高日志 index（全局index）
func (rf *Raft) getMajorityIdx() int {
	matchIndex := make([]int, len(rf.peers))
	copy(matchIndex, rf.matchIndex)
	// 从大到小排序，处于 len(matchIndex)/2 下标处的 index 代表大多数节点已复制到的最高 index
	sort.Slice(matchIndex, func(i, j int) bool {
		return matchIndex[i] > matchIndex[j]
	})
	return matchIndex[len(matchIndex)/2] // 多数节点已复制到的最高日志 index
}
