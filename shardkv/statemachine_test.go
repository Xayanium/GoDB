package shardkv

import (
	kvrpc "GoDB/rpc/shardkv"
	"testing"
)

func TestMemoryKVStateMachine_BasicOps(t *testing.T) {
	mkv := NewMemoryKVStateMachine()

	// 1. 初始化状态
	if mkv.Status != Normal {
		t.Errorf("Expected initialized status to be Normal, got %v", mkv.Status)
	}

	// 2. Put & Get
	err := mkv.Put("key1", "val1")
	if err != kvrpc.ErrorCode_OK {
		t.Errorf("Expected OK, got %v", err)
	}

	val, err := mkv.Get("key1")
	if err != kvrpc.ErrorCode_OK || val != "val1" {
		t.Errorf("Expected (val1, OK), got (%v, %v)", val, err)
	}

	// 3. Get 不存在的 Key
	val, err = mkv.Get("non_existent")
	if err != kvrpc.ErrorCode_ERR_NO_KEY || val != "" {
		t.Errorf("Expected ('', ERR_NO_KEY), got (%v, %v)", val, err)
	}

	// 4. Append 已存在的 Key
	err = mkv.Append("key1", "_appended")
	if err != kvrpc.ErrorCode_OK {
		t.Errorf("Expected OK, got %v", err)
	}
	val, _ = mkv.Get("key1")
	if val != "val1_appended" {
		t.Errorf("Expected 'val1_appended', got %v", val)
	}

	// 5. Append 不存在的 Key (相当于自动创建)
	err = mkv.Append("key2", "val2")
	if err != kvrpc.ErrorCode_OK {
		t.Errorf("Expected OK, got %v", err)
	}
	val, _ = mkv.Get("key2")
	if val != "val2" {
		t.Errorf("Expected 'val2', got %v", val)
	}
}

func TestMemoryKVStateMachine_DeepCopy(t *testing.T) {
	original := NewMemoryKVStateMachine()
	original.Status = MoveIn
	original.Put("color", "red")
	original.Put("number", "1")

	copied := original.deepCopy()

	// 1. 验证基础拷贝是否一致
	if copied.Status != original.Status {
		t.Errorf("Expected status to be copied as %v, got %v", original.Status, copied.Status)
	}
	val, _ := copied.Get("color")
	if val != "red" {
		t.Errorf("Data not copied completely")
	}

	// 2. 验证是否是深度拷贝（修改拷贝件不影响原件）
	copied.Status = GC
	copied.Put("color", "blue")
	copied.Put("new_key", "new_val")

	if original.Status != MoveIn {
		t.Errorf("Original status was modified by copy!")
	}
	val, _ = original.Get("color")
	if val != "red" {
		t.Errorf("Original map was modified by copy! (DeepCopy failed, shared same map pointer)")
	}
	_, err := original.Get("new_key")
	if err != kvrpc.ErrorCode_ERR_NO_KEY {
		t.Errorf("Original map received newly added key from copy")
	}
}
