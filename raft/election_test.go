package raft

import (
	"GoDB/config"
	raftrpc "GoDB/rpc/raft"
	"GoDB/tools"
	"context"
	"google.golang.org/grpc"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

//
// ============================
// Fake RPC client for RequestVote
// ============================
//

type fakeRaftClient struct {
	mu    sync.Mutex
	reply *raftrpc.RequestVoteReply
	err   error

	calls int
	args  []*raftrpc.RequestVoteArgs
	raftrpc.UnimplementedRaftServiceServer
}

func (c *fakeRaftClient) AppendEntries(ctx context.Context, in *raftrpc.AppendEntriesArgs, opts ...grpc.CallOption) (*raftrpc.AppendEntriesReply, error) {
	//TODO implement me
	panic("implement me")
}

func (c *fakeRaftClient) InstallSnapshot(ctx context.Context, in *raftrpc.InstallSnapshotArgs, opts ...grpc.CallOption) (*raftrpc.InstallSnapshotReply, error) {
	//TODO implement me
	panic("implement me")
}

func (c *fakeRaftClient) RequestVote(ctx context.Context, args *raftrpc.RequestVoteArgs, opts ...grpc.CallOption) (*raftrpc.RequestVoteReply, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	c.args = append(c.args, args)
	if c.err != nil {
		return nil, c.err
	}
	cp := *c.reply
	return &cp, nil
}

//
// ============================
// Test helpers
// ============================
//

// 初始化配置
func setTestConfig() {
	// election.go 的 resetElectionTimer() 会使用 config.Get().Raft
	cfg := config.Get()
	cfg.Raft.ElectionTimeoutMin = 250
	cfg.Raft.ElectionTimeoutMax = 400
	cfg.Raft.HeartbeatInterval = 30
	cfg.Raft.ApplyInterval = 10
	cfg.Raft.SnapshotThreshold = 1000
	cfg.Raft.MaxLogSize = 10 * 1024 * 1024
}

func makeRaftForElectionTests(t *testing.T, peerN int) *Raft {
	t.Helper()
	setTestConfig()

	rf := &Raft{}
	rf.me = 0
	rf.role = Follower
	rf.CurrentTerm = 2
	rf.VotedFor = -1
	rf.persister = tools.MakePersister()

	// 初始化日志：让 last() 可用（raft_log.go 已提供 NewLog）
	rf.log = NewLog(0, 0, nil)
	// 追加一些日志，构造 lastIndex=3 lastTerm=2
	rf.log.tailLog = append(rf.log.tailLog,
		&raftrpc.LogEntry{Term: 1},
		&raftrpc.LogEntry{Term: 1},
		&raftrpc.LogEntry{Term: 2},
	)

	// peers
	rf.peers = make([]raftrpc.RaftServiceClient, peerN)

	// nextIndex/matchIndex 被 becomeLeader 使用
	rf.nextIndex = make([]int, peerN)
	rf.matchIndex = make([]int, peerN)

	// electionTimer 被 resetElectionTimer 使用
	rf.electionTimer = time.NewTimer(24 * time.Hour)
	if !rf.electionTimer.Stop() {
		select {
		case <-rf.electionTimer.C:
		default:
		}
	}
	rf.electionTimer.Reset(24 * time.Hour)

	// applyCond 在其它模块可能会用（这里不强依赖）
	rf.applyCond = sync.NewCond(&rf.mu)

	return rf
}

//
// ============================
// logCheck() tests
// ============================
//

// 验证 logCheck() 的日志比较逻辑：候选人日志不可比自己旧（Term更新或Index号更大），否则拒绝投票
func TestLogCheck(t *testing.T) {
	rf := makeRaftForElectionTests(t, 3)
	// 当前节点 lastIndex=3 lastTerm=2

	cases := []struct {
		name           string
		candidateTerm  int
		candidateIndex int
		want           bool
	}{
		{"term higher => ok", 3, 0, true},
		{"term lower => reject", 1, 100, false},
		{"same term, index higher => ok", 2, 10, true},
		{"same term, index equal => ok", 2, 3, true},
		{"same term, index lower => reject", 2, 2, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := rf.logCheck(tc.candidateTerm, tc.candidateIndex); got != tc.want {
				t.Fatalf("logCheck(%d,%d)=%v want=%v", tc.candidateTerm, tc.candidateIndex, got, tc.want)
			}
		})
	}
}

//
// ============================
// RequestVote() tests
// ============================
//

// 如果候选人 args.Term < rf.CurrentTerm，必须拒绝投票（VoteGranted=false），reply.Term 应该返回当前 term（让对方更新）
func TestRequestVote_RejectLowerTerm(t *testing.T) {
	rf := makeRaftForElectionTests(t, 3)

	reply, err := rf.RequestVote(context.Background(), &raftrpc.RequestVoteArgs{
		Term:         1, // < rf.CurrentTerm(2)
		CandidateId:  1,
		LastLogIndex: 3,
		LastLogTerm:  2,
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if reply.VoteGranted {
		t.Fatalf("expected VoteGranted=false")
	}
	if int(reply.Term) != 2 {
		t.Fatalf("expected reply.Term=2, got %d", reply.Term)
	}
}

// 如果在同一个 term 里已经投过票，必须拒绝
func TestRequestVote_RejectAlreadyVoted(t *testing.T) {
	rf := makeRaftForElectionTests(t, 3)
	rf.VotedFor = 9

	reply, err := rf.RequestVote(context.Background(), &raftrpc.RequestVoteArgs{
		Term:         int32(rf.CurrentTerm),
		CandidateId:  1,
		LastLogIndex: 3,
		LastLogTerm:  2,
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if reply.VoteGranted {
		t.Fatalf("expected VoteGranted=false because already voted")
	}
	if rf.VotedFor != 9 {
		t.Fatalf("expected VotedFor unchanged=9, got %d", rf.VotedFor)
	}
}

// 候选人的日志落后，必须拒绝投票
func TestRequestVote_RejectOlderCandidateLog(t *testing.T) {
	rf := makeRaftForElectionTests(t, 3)

	reply, err := rf.RequestVote(context.Background(), &raftrpc.RequestVoteArgs{
		Term:         int32(rf.CurrentTerm),
		CandidateId:  1,
		LastLogIndex: 2, // older index with same term
		LastLogTerm:  2,
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if reply.VoteGranted {
		t.Fatalf("expected VoteGranted=false because candidate log is older")
	}
	if rf.VotedFor != -1 {
		t.Fatalf("expected VotedFor unchanged=-1, got %d", rf.VotedFor)
	}
}

//
// ============================
// startElection()/askVote() tests
// ============================
//

// 模拟 3 节点集群，自己先有 1 票，另外两个 peer 都返回 VoteGranted=true，达到多数（>=2）后应该 becomeLeader()
func TestAskVote_MajorityBecomesLeader(t *testing.T) {
	rf := makeRaftForElectionTests(t, 3)

	rf.mu.Lock()
	rf.role = Candidate
	rf.CurrentTerm = 10
	rf.VotedFor = rf.me
	rf.mu.Unlock()

	atomic.StoreInt32(&rf.dead, 1) // 尽量让 replicationTicker 快速退出（若其内部检查 isKilled）

	term := rf.CurrentTerm

	c1 := &fakeRaftClient{reply: &raftrpc.RequestVoteReply{Term: int32(term), VoteGranted: true}}
	c2 := &fakeRaftClient{reply: &raftrpc.RequestVoteReply{Term: int32(term), VoteGranted: true}}

	// peers[0] 不用（自己）
	rf.peers[1] = c1
	rf.peers[2] = c2

	args := &raftrpc.RequestVoteArgs{
		Term:         int32(term),
		CandidateId:  int32(rf.me),
		LastLogIndex: 3,
		LastLogTerm:  2,
	}

	votes := 1
	rf.askVote(1, args, &votes, term)
	rf.askVote(2, args, &votes, term)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader {
		t.Fatalf("expected role=Leader, got %v", rf.role)
	}
}

// 如果 peer 返回 reply.Term > rf.CurrentTerm，Candidate 必须立刻退回 follower，自己的 term 更新为更大的 term
func TestAskVote_HigherTermReplyBecomesFollower(t *testing.T) {
	rf := makeRaftForElectionTests(t, 2)

	rf.mu.Lock()
	rf.role = Candidate
	rf.CurrentTerm = 5
	rf.mu.Unlock()

	term := 5
	c := &fakeRaftClient{reply: &raftrpc.RequestVoteReply{Term: 999, VoteGranted: false}}
	rf.peers[1] = c

	args := &raftrpc.RequestVoteArgs{
		Term:         int32(term),
		CandidateId:  int32(rf.me),
		LastLogIndex: 3,
		LastLogTerm:  2,
	}

	votes := 1
	rf.askVote(1, args, &votes, term)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Follower {
		t.Fatalf("expected role=Follower, got %v", rf.role)
	}
	if rf.CurrentTerm != 999 {
		t.Fatalf("expected CurrentTerm=999, got %d", rf.CurrentTerm)
	}
}
