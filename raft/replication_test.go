package raft

import (
	"context"
	"sync"
	"testing"
	"time"

	raftrpc "GoDB/rpc/raft"

	"google.golang.org/grpc"
)

type mockRaftClient struct {
	appendFn      func(ctx context.Context, in *raftrpc.AppendEntriesArgs) (*raftrpc.AppendEntriesReply, error)
	requestVoteFn func(ctx context.Context, in *raftrpc.RequestVoteArgs) (*raftrpc.RequestVoteReply, error)
	installSnapFn func(ctx context.Context, in *raftrpc.InstallSnapshotArgs) (*raftrpc.InstallSnapshotReply, error)
}

func (m mockRaftClient) AppendEntries(ctx context.Context, in *raftrpc.AppendEntriesArgs, opts ...grpc.CallOption) (*raftrpc.AppendEntriesReply, error) {
	if m.appendFn == nil {
		return &raftrpc.AppendEntriesReply{}, nil
	}
	return m.appendFn(ctx, in)
}

func (m mockRaftClient) RequestVote(ctx context.Context, in *raftrpc.RequestVoteArgs, opts ...grpc.CallOption) (*raftrpc.RequestVoteReply, error) {
	if m.requestVoteFn == nil {
		return &raftrpc.RequestVoteReply{}, nil
	}
	return m.requestVoteFn(ctx, in)
}

func (m mockRaftClient) InstallSnapshot(ctx context.Context, in *raftrpc.InstallSnapshotArgs, opts ...grpc.CallOption) (*raftrpc.InstallSnapshotReply, error) {
	if m.installSnapFn == nil {
		return &raftrpc.InstallSnapshotReply{}, nil
	}
	return m.installSnapFn(ctx, in)
}

// newTestRaft constructs a minimal Raft instance suitable for unit testing replication logic.
// NOTE: We intentionally avoid the "success path" of AppendEntries, because it calls persist()/resetElectionTimer()
// which are defined elsewhere in the full project.
func newTestRaft(numPeers int, me int) *Raft {
	rf := &Raft{
		me:          me,
		role:        Leader,
		CurrentTerm: 1,
		commitIndex: 0,
		log:         NewLog(0, 0, nil),
		nextIndex:   make([]int, numPeers),
		matchIndex:  make([]int, numPeers),
	}
	rf.peers = make([]raftrpc.RaftServiceClient, numPeers)
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.electionTimer = time.NewTimer(time.Hour)
	return rf
}

func mustEntry(term int32) *raftrpc.LogEntry {
	return &raftrpc.LogEntry{Term: term}
}

// 测试了 Leader 如何根据 matchIndex 数组计算出当前大多数节点已经复制的日志索引。
func TestGetMajorityIdx(t *testing.T) {
	rf := newTestRaft(5, 0)
	rf.matchIndex = []int{7, 5, 6, 4, 6}

	got := rf.getMajorityIdx()
	// sorted desc: 7,6,6,5,4 => index 2 => 6
	if got != 6 {
		t.Fatalf("getMajorityIdx()=%d, want 6", got)
	}
}

// 测试了如果发送请求的 Leader 任期（Term）比 Follower 当前的任期小，Follower 会明确拒绝该请求（返回 Success: false）
func TestAppendEntries_TermTooSmallRejected(t *testing.T) {
	rf := newTestRaft(3, 0)
	rf.CurrentTerm = 3

	reply, err := rf.AppendEntries(context.Background(), &raftrpc.AppendEntriesArgs{
		Term:     2,
		LeaderId: 1,
	})
	if err != nil {
		t.Fatalf("AppendEntries returned error: %v", err)
	}
	if reply.Success {
		t.Fatalf("expected Success=false when leader term is smaller")
	}
	if reply.Term != int32(3) {
		t.Fatalf("reply.Term=%d, want %d", reply.Term, 3)
	}
}

// 测试了当 Follower 的日志太短，根本没有到达 Leader 要求的 PrevLogIndex 时，Follower 会拒绝追加，并返回 ConflictTerm 为无效值，同时将 ConflictIndex 设为自己日志的末尾位置。
func TestAppendEntries_LogTooShortConflict(t *testing.T) {
	rf := newTestRaft(3, 0)
	// Build follower log with size=3 (index 1..3)
	rf.log.append(0, []*raftrpc.LogEntry{mustEntry(1), mustEntry(1), mustEntry(2)})

	reply, err := rf.AppendEntries(context.Background(), &raftrpc.AppendEntriesArgs{
		Term:         int32(rf.CurrentTerm),
		LeaderId:     1,
		PrevLogIndex: 5,
		PrevLogTerm:  2,
	})
	if err != nil {
		t.Fatalf("AppendEntries returned error: %v", err)
	}
	if reply.Success {
		t.Fatalf("expected Success=false when follower log is too short")
	}
	if reply.ConflictTerm != InvalidTerm {
		t.Fatalf("ConflictTerm=%d, want InvalidTerm(%d)", reply.ConflictTerm, InvalidTerm)
	}
	// ConflictIndex should be follower lastIndex (3)
	if reply.ConflictIndex != 3 {
		t.Fatalf("ConflictIndex=%d, want 3", reply.ConflictIndex)
	}
}

// 测试了当 Leader 发送的 PrevLogIndex 已经落入了 Follower 本地被压缩的快照区域（<= SnapLastIdx）时，Follower 会拒绝请求并标记 NeedSnapshot=true，提示 Leader 改为发送
func TestAppendEntries_PrevIndexInSnapshotNeedSnapshot(t *testing.T) {
	rf := newTestRaft(3, 0)
	// Pretend follower has snapshot up to index 10 (term 4)
	rf.log = NewLog(10, 4, []byte("snap"))
	// plus some tail entries (size becomes 12)
	rf.log.append(10, []*raftrpc.LogEntry{mustEntry(5), mustEntry(5)})

	reply, err := rf.AppendEntries(context.Background(), &raftrpc.AppendEntriesArgs{
		Term:         int32(rf.CurrentTerm),
		LeaderId:     1,
		PrevLogIndex: 9, // <= SnapLastIdx
		PrevLogTerm:  4,
	})
	if err != nil {
		t.Fatalf("AppendEntries returned error: %v", err)
	}
	if reply.Success {
		t.Fatalf("expected Success=false when prevLogIndex is in snapshot region")
	}
	if !reply.NeedSnapshot {
		t.Fatalf("expected NeedSnapshot=true")
	}
	if int(reply.ConflictIndex) != rf.log.SnapLastIdx {
		t.Fatalf("ConflictIndex=%d, want SnapLastIdx=%d", reply.ConflictIndex, rf.log.SnapLastIdx)
	}
	if int(reply.ConflictTerm) != rf.log.SnapLastTerm {
		t.Fatalf("ConflictTerm=%d, want SnapLastTerm=%d", reply.ConflictTerm, rf.log.SnapLastTerm)
	}
}

// 测试了当 Follower 在 PrevLogIndex 处有日志，但是该日志的任期与 Leader 发来的 PrevLogTerm 不匹配时，Follower 会拒绝追加，并返回冲突的任期（ConflictTerm）以及该冲突任期在自己日志中第一次出现的索引位置（ConflictIndex）。
func TestAppendEntries_PrevLogTermMismatchConflictTermIndex(t *testing.T) {
	rf := newTestRaft(3, 0)
	// Build follower log: idx1 term1, idx2 term1, idx3 term2, idx4 term2, idx5 term3
	rf.log.append(0, []*raftrpc.LogEntry{
		mustEntry(1), mustEntry(1), mustEntry(2), mustEntry(2), mustEntry(3),
	})

	reply, err := rf.AppendEntries(context.Background(), &raftrpc.AppendEntriesArgs{
		Term:         int32(rf.CurrentTerm),
		LeaderId:     1,
		PrevLogIndex: 4,
		PrevLogTerm:  999, // mismatch
	})
	if err != nil {
		t.Fatalf("AppendEntries returned error: %v", err)
	}
	if reply.Success {
		t.Fatalf("expected Success=false when prevLogTerm mismatches")
	}
	if reply.ConflictTerm != 2 {
		t.Fatalf("ConflictTerm=%d, want 2", reply.ConflictTerm)
	}
	// firstFor(2) in this Log implementation returns slice index in tailLog; with placeholder at 0,
	// terms: [0]=snap, [1]=1, [2]=1, [3]=2, [4]=2, [5]=3 => firstFor(2)=3
	if reply.ConflictIndex != 3 {
		t.Fatalf("ConflictIndex=%d, want 3", reply.ConflictIndex)
	}
}

// 测试了当 Follower 因为日志太短而拒绝时（ConflictTerm 为无效值），Leader 会将该 Follower 的 nextIndex 直接回退到 Follower 返回的 ConflictIndex + 1。
func TestAskReplication_BackoffCase1_LogTooShort(t *testing.T) {
	rf := newTestRaft(2, 0)
	peer := 1
	rf.role = Leader
	rf.CurrentTerm = 3

	// nextIndex is large; expect to backoff to ConflictIndex+1
	rf.nextIndex[peer] = 10

	rf.peers[peer] = mockRaftClient{
		appendFn: func(ctx context.Context, in *raftrpc.AppendEntriesArgs) (*raftrpc.AppendEntriesReply, error) {
			return &raftrpc.AppendEntriesReply{
				Term:          int32(rf.CurrentTerm),
				Success:       false,
				ConflictTerm:  InvalidTerm,
				ConflictIndex: 4, // follower lastIndex
			}, nil
		},
	}

	args := &raftrpc.AppendEntriesArgs{Term: int32(rf.CurrentTerm)}
	rf.askReplication(peer, args, rf.CurrentTerm)

	if rf.nextIndex[peer] != 5 {
		t.Fatalf("nextIndex=%d, want 5", rf.nextIndex[peer])
	}
}

// 测试了当 Follower 返回了冲突任期，且 Leader 本地日志中包含该冲突任期时，Leader 会将 nextIndex 回退到本地日志中该冲突任期第一次出现的位置（firstFor）。
func TestAskReplication_BackoffCase2_1_LeaderHasConflictTerm(t *testing.T) {
	rf := newTestRaft(2, 0)
	peer := 1
	rf.role = Leader
	rf.CurrentTerm = 3

	// Leader log contains term=7 at slice index 3 (see note below)
	rf.log.append(0, []*raftrpc.LogEntry{
		mustEntry(1), mustEntry(2), mustEntry(7), mustEntry(7), mustEntry(8),
	})
	// In this Log implementation firstFor(7) returns tailLog slice index:
	// [0]=snap, [1]=1,[2]=2,[3]=7,[4]=7,[5]=8 => firstFor(7)=3
	wantFirst := rf.log.firstFor(7)
	if wantFirst != 3 {
		t.Fatalf("sanity: firstFor(7)=%d, want 3", wantFirst)
	}

	rf.nextIndex[peer] = 20
	rf.peers[peer] = mockRaftClient{
		appendFn: func(ctx context.Context, in *raftrpc.AppendEntriesArgs) (*raftrpc.AppendEntriesReply, error) {
			return &raftrpc.AppendEntriesReply{
				Term:          int32(rf.CurrentTerm),
				Success:       false,
				ConflictTerm:  7,
				ConflictIndex: 5,
			}, nil
		},
	}

	args := &raftrpc.AppendEntriesArgs{Term: int32(rf.CurrentTerm)}
	rf.askReplication(peer, args, rf.CurrentTerm)

	if rf.nextIndex[peer] != wantFirst {
		t.Fatalf("nextIndex=%d, want %d(firstFor(conflictTerm))", rf.nextIndex[peer], wantFirst)
	}
}

// 测试了当 Follower 返回了冲突任期，但 Leader 本地日志中不包含该任期时，Leader 会直接将 nextIndex 回退到 Follower 返回的 ConflictIndex。
func TestAskReplication_BackoffCase2_2_LeaderMissingConflictTerm(t *testing.T) {
	rf := newTestRaft(2, 0)
	peer := 1
	rf.role = Leader
	rf.CurrentTerm = 3

	// Leader log does NOT contain term=9
	rf.log.append(0, []*raftrpc.LogEntry{
		mustEntry(1), mustEntry(2), mustEntry(7),
	})

	rf.nextIndex[peer] = 20
	rf.peers[peer] = mockRaftClient{
		appendFn: func(ctx context.Context, in *raftrpc.AppendEntriesArgs) (*raftrpc.AppendEntriesReply, error) {
			return &raftrpc.AppendEntriesReply{
				Term:          int32(rf.CurrentTerm),
				Success:       false,
				ConflictTerm:  9,
				ConflictIndex: 6, // follower first index of term 9
			}, nil
		},
	}

	args := &raftrpc.AppendEntriesArgs{Term: int32(rf.CurrentTerm)}
	rf.askReplication(peer, args, rf.CurrentTerm)

	if rf.nextIndex[peer] != 6 {
		t.Fatalf("nextIndex=%d, want 6(conflictIndex)", rf.nextIndex[peer])
	}
}

// 测试了当 Follower 明确回复需要快照（NeedSnapshot: true）时，Leader 会保持 nextIndex 不变，因为接下来的操作应当是发送快照而不是继续回退日志索引。
func TestAskReplication_NeedSnapshot_NoNextIndexChange(t *testing.T) {
	rf := newTestRaft(2, 0)
	peer := 1
	rf.role = Leader
	rf.CurrentTerm = 3

	rf.nextIndex[peer] = 10

	rf.peers[peer] = mockRaftClient{
		appendFn: func(ctx context.Context, in *raftrpc.AppendEntriesArgs) (*raftrpc.AppendEntriesReply, error) {
			return &raftrpc.AppendEntriesReply{
				Term:         int32(rf.CurrentTerm),
				Success:      false,
				NeedSnapshot: true,
			}, nil
		},
	}

	args := &raftrpc.AppendEntriesArgs{Term: int32(rf.CurrentTerm)}
	rf.askReplication(peer, args, rf.CurrentTerm)

	if rf.nextIndex[peer] != 10 {
		t.Fatalf("nextIndex changed to %d, want unchanged 10 when NeedSnapshot=true", rf.nextIndex[peer])
	}
}
