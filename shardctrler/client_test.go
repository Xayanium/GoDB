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
	config   *ctrlerrpc.Config
}

func (m *mockServer) Join(ctx context.Context, in *ctrlerrpc.JoinRequest, opts ...grpc.CallOption) (*ctrlerrpc.JoinResponse, error) {
	if m.crash {
		return nil, errors.New("rpc error: server crashed")
	}
	if !m.isLeader {
		return &ctrlerrpc.JoinResponse{Err: ErrWrongLeader}, nil
	}
	if m.timeout {
		return &ctrlerrpc.JoinResponse{Err: ErrTimeout}, nil
	}
	return &ctrlerrpc.JoinResponse{Err: ""}, nil
}

func (m *mockServer) Leave(ctx context.Context, in *ctrlerrpc.LeaveRequest, opts ...grpc.CallOption) (*ctrlerrpc.LeaveResponse, error) {
	if m.crash {
		return nil, errors.New("rpc error: server crashed")
	}
	if !m.isLeader {
		return &ctrlerrpc.LeaveResponse{Err: ErrWrongLeader}, nil
	}
	if m.timeout {
		return &ctrlerrpc.LeaveResponse{Err: ErrTimeout}, nil
	}
	return &ctrlerrpc.LeaveResponse{Err: ""}, nil
}

func (m *mockServer) Move(ctx context.Context, in *ctrlerrpc.MoveRequest, opts ...grpc.CallOption) (*ctrlerrpc.MoveResponse, error) {
	if m.crash {
		return nil, errors.New("rpc error: server crashed")
	}
	if !m.isLeader {
		return &ctrlerrpc.MoveResponse{Err: ErrWrongLeader}, nil
	}
	if m.timeout {
		return &ctrlerrpc.MoveResponse{Err: ErrTimeout}, nil
	}
	return &ctrlerrpc.MoveResponse{Err: ""}, nil
}

func (m *mockServer) Query(ctx context.Context, in *ctrlerrpc.QueryRequest, opts ...grpc.CallOption) (*ctrlerrpc.QueryResponse, error) {
	if m.crash {
		return nil, errors.New("rpc error: server crashed")
	}
	if !m.isLeader {
		return &ctrlerrpc.QueryResponse{Err: ErrWrongLeader}, nil
	}
	if m.timeout {
		return &ctrlerrpc.QueryResponse{Err: ErrTimeout}, nil
	}
	return &ctrlerrpc.QueryResponse{Err: "", Config: m.config}, nil
}

func TestClerk_Query(t *testing.T) {
	servers := []ctrlerrpc.ShardCtrlerClient{
		&mockServer{crash: true},
		&mockServer{isLeader: false},
		&mockServer{isLeader: true, config: &ctrlerrpc.Config{Num: 1}},
	}
	ck := MakeClerk(servers)

	cfg := ck.Query(-1)
	if cfg.Num != 1 {
		t.Fatalf("expected config num 1, got %v", cfg.Num)
	}
}

func TestClerk_Join(t *testing.T) {
	servers := []ctrlerrpc.ShardCtrlerClient{
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
	servers := []ctrlerrpc.ShardCtrlerClient{
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
	servers := []ctrlerrpc.ShardCtrlerClient{
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
