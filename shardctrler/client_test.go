package shardctrler

import (
	"GoDB/rpc/shardctrler"
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc"
)

// mockServer 模拟 ShardCtrlerClient
type mockServer struct {
	isLeader bool
	timeout  bool
	crash    bool
	config   *shardctrler.Config
}

func (m *mockServer) Join(ctx context.Context, in *shardctrler.JoinRequest, opts ...grpc.CallOption) (*shardctrler.JoinResponse, error) {
	if m.crash {
		return nil, errors.New("rpc error: server crashed")
	}
	if !m.isLeader {
		return &shardctrler.JoinResponse{Err: ErrWrongLeader}, nil
	}
	if m.timeout {
		return &shardctrler.JoinResponse{Err: ErrTimeout}, nil
	}
	return &shardctrler.JoinResponse{Err: ""}, nil
}

func (m *mockServer) Leave(ctx context.Context, in *shardctrler.LeaveRequest, opts ...grpc.CallOption) (*shardctrler.LeaveResponse, error) {
	if m.crash {
		return nil, errors.New("rpc error: server crashed")
	}
	if !m.isLeader {
		return &shardctrler.LeaveResponse{Err: ErrWrongLeader}, nil
	}
	if m.timeout {
		return &shardctrler.LeaveResponse{Err: ErrTimeout}, nil
	}
	return &shardctrler.LeaveResponse{Err: ""}, nil
}

func (m *mockServer) Move(ctx context.Context, in *shardctrler.MoveRequest, opts ...grpc.CallOption) (*shardctrler.MoveResponse, error) {
	if m.crash {
		return nil, errors.New("rpc error: server crashed")
	}
	if !m.isLeader {
		return &shardctrler.MoveResponse{Err: ErrWrongLeader}, nil
	}
	if m.timeout {
		return &shardctrler.MoveResponse{Err: ErrTimeout}, nil
	}
	return &shardctrler.MoveResponse{Err: ""}, nil
}

func (m *mockServer) Query(ctx context.Context, in *shardctrler.QueryRequest, opts ...grpc.CallOption) (*shardctrler.QueryResponse, error) {
	if m.crash {
		return nil, errors.New("rpc error: server crashed")
	}
	if !m.isLeader {
		return &shardctrler.QueryResponse{Err: ErrWrongLeader}, nil
	}
	if m.timeout {
		return &shardctrler.QueryResponse{Err: ErrTimeout}, nil
	}
	return &shardctrler.QueryResponse{Err: "", Config: m.config}, nil
}

func TestClerk_Query(t *testing.T) {
	servers := []shardctrler.ShardCtrlerClient{
		&mockServer{crash: true},
		&mockServer{isLeader: false},
		&mockServer{isLeader: true, config: &shardctrler.Config{Num: 1}},
	}
	ck := MakeClerk(servers)

	cfg := ck.Query(-1)
	if cfg.Num != 1 {
		t.Fatalf("expected config num 1, got %v", cfg.Num)
	}
}

func TestClerk_Join(t *testing.T) {
	servers := []shardctrler.ShardCtrlerClient{
		&mockServer{crash: true},
		&mockServer{isLeader: false},
		&mockServer{isLeader: true},
	}
	ck := MakeClerk(servers)

	ck.Join(map[int][]string{
		1: {"server1"},
	})
	if ck.seqId != 1 {
		t.Fatalf("expected seqId to be advanced to 1, got %v", ck.seqId)
	}
}

func TestClerk_Leave(t *testing.T) {
	servers := []shardctrler.ShardCtrlerClient{
		&mockServer{crash: true},
		&mockServer{isLeader: false},
		&mockServer{isLeader: true},
	}
	ck := MakeClerk(servers)

	ck.Leave([]int{1})
	if ck.seqId != 1 {
		t.Fatalf("expected seqId to be advanced to 1, got %v", ck.seqId)
	}
}

func TestClerk_Move(t *testing.T) {
	servers := []shardctrler.ShardCtrlerClient{
		&mockServer{crash: true},
		&mockServer{isLeader: false},
		&mockServer{isLeader: true},
	}
	ck := MakeClerk(servers)

	ck.Move(1, 1)
	if ck.seqId != 1 {
		t.Fatalf("expected seqId to be advanced to 1, got %v", ck.seqId)
	}
}
