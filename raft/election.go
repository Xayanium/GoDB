package raft

import (
	"GoDB/config"
	raftrpc "GoDB/rpc/raft"
	"context"
	"fmt"
	"math/rand"
	"time"
)

// 选举 ticker，Raft 节点初始化时会启动该协程循环，等待选举定时器超时触发，检查节点角色并决定是否发起选举
func (rf *Raft) electionTicker() {
	for !rf.isKilled() {
		<-rf.electionTimer.C // 等待选举定时器触发

		rf.mu.Lock()
		if rf.role != Leader {
			// 更改角色为 Candidate，重置超时计时器，向其他节点发起选举
			rf.becomeCandidate()
			rf.resetElectionTimer()
			go rf.startElection(rf.CurrentTerm)
		}
		rf.mu.Unlock()
	}
}

// ============================================================================
// Raft 选举相关方法
// ============================================================================

// Candidate 并发向所有 peer 进行投票RPC请求，统计结果并判断是否当选 leader（每次收到RPC响应后都应判断一次）
func (rf *Raft) startElection(term int) {
	votes := 1
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 上下文检查，由于 electionTicker 和 startElection 锁不连续，节点的角色/任期可能已发生变化，只有在上下文未改变的情况下才发起 RPC 请求
	//（比如期间选出 leader，并收到其心跳而变为follower）
	if !rf.contextCheck(Candidate, term) {
		LOG(rf.me, rf.CurrentTerm, DWarn, fmt.Sprintf("Peer_%d Candidate context changed, stop Election", rf.me))
		return
	}

	logIndex, logTerm := rf.log.last()
	args := &raftrpc.RequestVoteArgs{
		Term:         int32(rf.CurrentTerm),
		CandidateId:  int32(rf.me),
		LastLogIndex: int32(logIndex),
		LastLogTerm:  int32(logTerm),
	}

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			continue
		}
		go rf.askVote(peer, args, &votes, term)
	}
}

// Candidate 向 peer 发送 RequestVote RPC 请求，接收并处理 RPC 响应，用于 startElection()
func (rf *Raft) askVote(peer int, args *raftrpc.RequestVoteArgs, votes *int, term int) {
	// 1. 发送 RequestVote RPC 请求
	client := rf.peers[peer]
	reply, err := client.RequestVote(context.Background(), args)
	if err != nil {
		LOG(rf.me, rf.CurrentTerm, DError, fmt.Sprintf("Peer_%d Failed to send RequestVote RPC to Peer_%d: %v", rf.me, peer, err))
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 2. Term 检查，判断自己是否是合法 Candidate
	if int(reply.Term) > rf.CurrentTerm {
		LOG(rf.me, rf.CurrentTerm, DWarn, fmt.Sprintf("Found higher Term_%d from Peer_%d", args.Term, args.CandidateId))
		rf.becomeFollower(int(reply.Term)) // 发现更大的 term，切换为 Follower
		return
	}

	// 3. 上下文检查，由于 startElection 和 askVote 锁不连续，节点的角色/任期可能已发生变化，只有在上下文未改变的情况下才继续统计投票结果
	if !rf.contextCheck(Candidate, term) {
		LOG(rf.me, rf.CurrentTerm, DWarn, fmt.Sprintf("Peer_%d Candidate context changed, ignore RequestVote reply from Peer_%d", rf.me, peer))
		return
	}

	// 4. 统计投票结果，决定节点是否当选 leader（每次收到RPC响应后都应判断一次）
	if reply.VoteGranted {
		*votes++
		if *votes >= len(rf.peers)/2+1 {
			rf.becomeLeader()
			go rf.replicationTicker(term) // Leader 当选后立即开启协程循环，定期对其他节点进行日志同步或心跳（AppendEntries RPC）
		}
	}

}

// RequestVote peer 接收到 Candidate 的 RequestVote RPC 请求，并进行处理（作为 RPC Server 端，处理 Client 端的请求）
func (rf *Raft) RequestVote(ctx context.Context, args *raftrpc.RequestVoteArgs) (reply *raftrpc.RequestVoteReply, err error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply = &raftrpc.RequestVoteReply{}
	reply.VoteGranted = false
	reply.Term = int32(rf.CurrentTerm)

	// 1. 任期检查：候选人 term 更小则拒绝投票，term 更大则更新节点自己 term，并将角色变为 follower
	if int(args.Term) < rf.CurrentTerm {
		LOG(rf.me, rf.CurrentTerm, DWarn, fmt.Sprintf("Peer_%d Voted Failed, Term_%d > Candidate Term_%d  ", rf.me, rf.CurrentTerm, args.Term))
		return
	}
	if int(args.Term) > rf.CurrentTerm {
		rf.becomeFollower(int(args.Term))
	}

	// 2. 投票检查：节点是否在该 term 已投过票（Raft规定每个节点在同一任期最多投一票）
	if rf.VotedFor != -1 {
		LOG(rf.me, rf.CurrentTerm, DWarn, fmt.Sprintf("Peer_%d Voted Failed, already voted for Peer_%d", rf.me, rf.VotedFor))
		return
	}

	// 3. 候选人日志检查：是否不旧于当前节点的日志（检查确保了投票只会授予那些拥有至少和当前节点相同日志的候选人）
	if !rf.logCheck(int(args.LastLogTerm), int(args.LastLogIndex)) {
		LOG(rf.me, rf.CurrentTerm, DWarn,
			fmt.Sprintf("Peer_%d Voted Failed, Candidate log is older: %s", rf.me,
				fmt.Sprintf("Candidate LastLogIndex: %d, LastLogTerm: %d; Peer LastLogIndex: %d, LastLogTerm: %d",
					args.LastLogIndex, args.LastLogTerm, rf.log.size(), rf.log.get(rf.log.size()).Term)))
		return
	}

	// 4. 检查通过，投票，持久化相关字段，重置选举计时器（防止自己再次参加选举）
	rf.VotedFor = int(args.CandidateId)
	reply.VoteGranted = true
	rf.persist()
	rf.resetElectionTimer()

	LOG(rf.me, rf.CurrentTerm, DVote, fmt.Sprintf("Peer_%d Voted for Candidate Peer_%d in Term_%d", rf.me, args.CandidateId, rf.CurrentTerm))
	return
}

// ============================================================================
// 选举相关辅助方法
// ============================================================================

// 选举超时：Follower/Candidate 超过随机选举超时时间未收到 Leader 心跳，转为 Candidate 发起新一轮选举
//func (rf *Raft) isTimeout() bool {
//	return time.Since(rf.electionStart) > rf.electionTimeout
//}

// 重置选举计时器（需在锁内调用，防止多个 goroutine 并发 Reset/Stop 造成timer无效或反复触发）
func (rf *Raft) resetElectionTimer() {
	conf := config.Get().Raft

	// 随机选举超时时间，通常在 [150ms, 300ms] 范围内
	randRange := int64(conf.ElectionTimeoutMax - conf.ElectionTimeoutMin)
	electionTimeout := time.Duration(int64(conf.ElectionTimeoutMin)+rand.Int63()%randRange) * time.Millisecond

	// 先 Stop 取消计时，再 Reset（防止计时器在 第一次超时之后、Reset之前 再次超时，向 timer.C 中再次传入事件，计时器被重置后立即触发，导致连续选举）
	if !rf.electionTimer.Stop() {
		// 如果 Stop 失败，说明 timer.C 中可能已存在事件，需要清理
		select {
		case <-rf.electionTimer.C: // 清理 timer.C 中可能存在的事件
		default: // 防止 timer.C 中没有事件时阻塞
		}
	}
	rf.electionTimer.Reset(electionTimeout)
}

// 节点投票时，检查候选人日志不可比自己旧（Term更新或Index号更大），否则拒绝投票
func (rf *Raft) logCheck(candidateTerm int, candidateIndex int) bool {
	lastIndex, lastTerm := rf.log.last()
	if candidateTerm != lastTerm {
		return candidateTerm > lastTerm
	}
	return candidateIndex >= lastIndex
}
