package shardkv

import (
	ctrlerrpc "GoDB/rpc/shardctrler"
	kvrpc "GoDB/rpc/shardkv"
	"GoDB/shardctrler"
	"testing"
)

func TestServer_ExecConfigChange(t *testing.T) {
	// 初始化一个 Server 用于本地函数测试（不启动后台协程）
	s := &ShardKV{
		me:             0,
		gid:            100, // 当前分配 Group ID
		shards:         make(map[int64]*MemoryKVStateMachine),
		duplicateTable: make(map[int64]LastOperationInfo),
		prevConfig:     &ctrlerrpc.Config{Num: 0, Shards: make([]int32, shardctrler.NShards)},
		currentConfig:  &ctrlerrpc.Config{Num: 1, Shards: make([]int32, shardctrler.NShards)},
	}

	// 模拟配置变更:
	// Num: 2
	// 分片 0,1 原来是 0组(未分配) -> 变为100组(当前组)
	// 分片 2,3 原来是 100组(当前组) -> 变为200组(其他组)
	// 分片 4 原来是 200组(其他组) -> 变为100组(当前组)

	// 配置当前分属
	for i := 0; i < shardctrler.NShards; i++ {
		s.currentConfig.Shards[i] = 0 // 默认设为未分配
	}
	s.currentConfig.Shards[2] = 100
	s.currentConfig.Shards[3] = 100
	s.currentConfig.Shards[4] = 200

	newConf := &ctrlerrpc.Config{
		Num:    2,
		Shards: make([]int32, shardctrler.NShards),
		Groups: make(map[int32]*ctrlerrpc.ServerList),
	}
	for i := 0; i < shardctrler.NShards; i++ {
		newConf.Shards[i] = s.currentConfig.Shards[i]
	}
	newConf.Shards[0] = 100
	newConf.Shards[1] = 100
	newConf.Shards[2] = 200
	newConf.Shards[3] = 200
	newConf.Shards[4] = 100

	// 1. 开始调用应用新配置的函数
	reply := s.execConfigChange(newConf)

	if reply.Err != kvrpc.ErrorCode_OK {
		t.Fatalf("Expected OK, got %v", reply.Err)
	}

	// 2. 检查分片 0,1 : 因为是未分配0组直接转移到100组，其数据可以本地直接托管产生，无需走MoveIn状态，不需要向外拉取数据。
	if s.shards[0].Status != Normal {
		t.Errorf("Shard 0 status should be Normal, got %v", s.shards[0].Status)
	}
	
	// 3. 检查分片 2,3 : 旧配置是当前组，新配置是200组，故要进入 MoveOut 状态，准备向外迁移
	if s.shards[2].Status != MoveOut {
		t.Errorf("Shard 2 status should be MoveOut, got %v", s.shards[2].Status)
	}

	// 4. 检查分片 4 : 旧配置是200，新配置是100当前组，必须进入 MoveIn 状态找200的人拿数据
	if s.shards[4].Status != MoveIn {
		t.Errorf("Shard 4 status should be MoveIn, got %v", s.shards[4].Status)
	}
}

func TestServer_ExecShardGC(t *testing.T) {
	s := &ShardKV{
		me:            0,
		gid:           100,
		shards:        make(map[int64]*MemoryKVStateMachine),
		currentConfig: &ctrlerrpc.Config{Num: 5},
	}

	// 设置一些基础的 shard 状态机器
	
	// ShardId: 1, 作为刚刚完从别组拉取完数据的属于本机的新分片，当前处于 GC 状态
	s.shards[1] = NewMemoryKVStateMachine()
	s.shards[1].Put("k1", "v1")
	s.shards[1].Status = GC

	// ShardId: 2, 作为刚刚被别组拉取完成属于别机的新分片，当前处于 MoveOut 状态
	s.shards[2] = NewMemoryKVStateMachine()
	s.shards[2].Put("k2", "v2")
	s.shards[2].Status = MoveOut

	// ShardId: 3, 正常服务分片 (本应不受影响)
	s.shards[3] = NewMemoryKVStateMachine()
	s.shards[3].Put("k3", "v3")
	s.shards[3].Status = Normal

	req := &kvrpc.ShardOperationRequest{
		ConfigNum: 5,
		ShardIds:  []int32{1, 2},
	}

	// 触发执行 GC （应用 Raft 日志的同步回调）
	reply := s.execShardGC(req)
	if reply.Err != kvrpc.ErrorCode_OK {
		t.Fatalf("Expected OK, got %v", reply.Err)
	}

	// 开始核心校验

	// Shard 1 的 GC 状态务必被恢复至 Normal，并且内部的数据一定安好
	if s.shards[1].Status != Normal {
		t.Errorf("Shard 1 status should be restored back to Normal")
	}
	if v, _ := s.shards[1].Get("k1"); v != "v1" {
		t.Errorf("Shard 1 data should NOT be deleted!")
	}

	// Shard 2 处于 MoveOut 被要求 GC，它的数据实体字典必须被彻底清空清理
	if s.shards[2].Status != Normal {
		t.Errorf("Shard 2 status should be clean/Normal after pointer re-instantiation, got %v", s.shards[2].Status)
	}
	if _, err := s.shards[2].Get("k2"); err != kvrpc.ErrorCode_ERR_NO_KEY {
		t.Errorf("Shard 2 data was not swept clean! There's a memory leak / pointer reassignment bug!")
	}

	// Shard 3 必须一切安好没被触及
	if v, _ := s.shards[3].Get("k3"); v != "v3" {
		t.Errorf("Shard 3 should not be touched")
	}
}