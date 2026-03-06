package shardctrler

import (
	"testing"
)

func TestStateMachine_JoinAndLeave(t *testing.T) {
	sm := NewCtrlerStateMachine()

	// Initial query
	cfg, err := sm.Query(-1)
	if err != OK {
		t.Fatalf("Expected OK, got %v", err)
	}
	if len(cfg.Groups) != 0 {
		t.Fatalf("Expected 0 groups initially")
	}

	// Join group 1
	sm.Join(map[int][]string{1: {"a", "b", "c"}})
	cfg, _ = sm.Query(-1)
	if len(cfg.Groups) != 1 {
		t.Fatalf("Expected 1 group, got %d", len(cfg.Groups))
	}
	for i := 0; i < len(cfg.Shards); i++ {
		if cfg.Shards[i] != 1 {
			t.Fatalf("Expected shard %d to be assigned to group 1, got %d", i, cfg.Shards[i])
		}
	}

	// Join group 2
	sm.Join(map[int][]string{2: {"d", "e", "f"}})
	cfg, _ = sm.Query(-1)

	count1 := 0
	count2 := 0
	for i := 0; i < len(cfg.Shards); i++ {
		if cfg.Shards[i] == 1 {
			count1++
		} else if cfg.Shards[i] == 2 {
			count2++
		}
	}
	if count1 == 0 || count2 == 0 || (count1+count2 != len(cfg.Shards)) {
		t.Fatalf("Expected shards to be rebalanced, got counts: %d, %d", count1, count2)
	}

	// Leave group 1
	sm.Leave([]int{1})
	cfg, _ = sm.Query(-1)
	if len(cfg.Groups) != 1 {
		t.Fatalf("Expected 1 group, got %d", len(cfg.Groups))
	}
	for i := 0; i < len(cfg.Shards); i++ {
		if cfg.Shards[i] != 2 {
			t.Fatalf("Expected shard %d to be assigned to group 2, got %d", i, cfg.Shards[i])
		}
	}

	// Leave group 2 (all groups)
	sm.Leave([]int{2})
	cfg, _ = sm.Query(-1)
	if len(cfg.Groups) != 0 {
		t.Fatalf("Expected 0 groups")
	}
	for i := 0; i < len(cfg.Shards); i++ {
		if cfg.Shards[i] != 0 {
			t.Fatalf("Expected shard %d to be assigned to group 0, got %d", i, cfg.Shards[i])
		}
	}
}

func TestStateMachine_Move(t *testing.T) {
	sm := NewCtrlerStateMachine()
	sm.Join(map[int][]string{1: {"a"}, 2: {"b"}})

	sm.Move(0, 2)
	cfg, _ := sm.Query(-1)
	if cfg.Shards[0] != 2 {
		t.Fatalf("Expected shard 0 to be moved to group 2")
	}
}
