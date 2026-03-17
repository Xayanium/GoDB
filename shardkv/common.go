package shardkv

import (
	"GoDB/config"
	ctrlerrpc "GoDB/rpc/shardctrler"
	kvrpc "GoDB/rpc/shardkv"
	"GoDB/shardctrler"
	"hash/fnv"
)

// ============================================================================
// client、server 使用字段
// ============================================================================

type OpType uint8

const (
	OpGet OpType = iota
	OpPut
	OpAppend
)

// ============================================================================
// server 使用字段
// ============================================================================

var (
	ClientTimeout = config.Get().ShardKV.ClientReqTimeout
	FetchConfigInterval = config.Get().ShardKV.FetchConfigInterval
	ShardMigrationInterval = config.Get().ShardKV.ShardMigrationInterval
	ShardGCInterval = config.Get().ShardKV.ShardGcInterval
)

type OpReply struct {
	Value string
	Err   kvrpc.ErrorCode
}

// LastOperationInfo 客户端最后一次操作的 seqId 和返回客户端的RPC响应结果
type LastOperationInfo struct {
	SeqId int64
	Reply *OpReply
}

// RaftCommandType 区分提交到 Raft 日志中的命令属于哪一类操作（ShardKV 中，需要通过 Raft 达成共识的操作不只有客户端的读写请求，还包括集群内部的管理操作）
type RaftCommandType uint8

const (
	ClientOp       RaftCommandType = iota // 客户端发来的 Get / Put / Append 请求
	ConfigChange                          // 进行配置变更，表示 shardctrler 下发了新的分片分配方案
	ShardMigration                        // 进行分片数据迁移，表示从其他 Group 拉取到的分片数据已就绪，需要写入本 Group。
	ShardGC                               // 进行分片垃圾回收，表示分片迁移完成，源 Group 需删除已迁走的数据。
)

// Op KV数据库相关操作（属于客户端请求一类）（由于Raft中使用gob进行序列化，首字母要大写）
type Op struct {
	Optype OpType // KV数据库操作类型：Get、Put、Append
	Key    string
	Value  string

	// 用于去重表去重
	ClientId int64
	SeqId    int64
}

// RaftLog 提交到 Raft 日志中的命令，包含命令类型和具体内容（由于Raft中使用gob进行序列化，首字母要大写）
type RaftLog struct {
	CommandType RaftCommandType // 该命令属于哪一类操作（客户端请求、配置变更、分片迁移、分片GC）
	Data        interface{}     // 具体的命令内容，根据 CommandType 的不同，Data 的类型也不同
}

// ShardKVSnapshot 序列化server结构体中相关字段，制作成快照，并传给 Raft
type ShardKVSnapshot struct {
	Shards         map[int64]*MemoryKVStateMachine
	DuplicateTable map[int64]LastOperationInfo
	CurrentConfig  *ctrlerrpc.Config
	PrevConfig     *ctrlerrpc.Config
}

// ============================================================================
// statemachine 使用字段
// ============================================================================

type KVStatus uint8

const (
	Normal KVStatus = iota
	MoveIn
	MoveOut
	GC
)

// ============================================================================
// 辅助函数
// ============================================================================

// 将 key 映射到 shard 编号，来决定哪个 shard group 负责这个 key 的 Get/Put/Append 请求
func key2shard(key string) int64 {
	if shardctrler.NShards <= 0 {
		return 0
	}

	// 使用 FNV-1a 哈希
	h := fnv.New64a()
	h.Write([]byte(key))
	hashValue := h.Sum64()
	return int64(hashValue % uint64(shardctrler.NShards))
}
