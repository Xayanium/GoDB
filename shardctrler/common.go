package shardctrler

import (
	"GoDB/config"
	"GoDB/rpc/shardctrler"
)

type OpType uint8

type Err string

const (
	OpQuery OpType = iota
	OpJoin
	OpLeave
	OpMove
)

const (
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrDuplicate   = "ErrDuplicate"
	OK             = "OK"
)

var (
	NShards       = config.Get().ShardCtrler.NShards
	ClientTimeout = config.Get().ShardCtrler.ClientReqTimeout
)

// Op 记录 client 发起的 Query、Join、Leave、Move 操作的请求，server 传给 Raft层 进行日志同步 和 server的状态机执行
type Op struct {
	optype OpType

	// server 根据操作类型，应用到状态机
	servers map[int][]string // Join 操作应用到状态机，记录新加入的 groupId 和对应的服务器列表
	gids    []int            // Leave 操作应用到状态机，记录要删除的 groupId 列表
	shard   int              // Move 操作应用到状态机，记录要移动的 shard 编号
	gid     int              // Move 操作应用到状态机，记录要移动到的 groupId
	num     int              // Query 操作应用到状态机，记录要查询的配置编号（-1 代表最新配置）

	// 用于去重表去重
	clientId int64
	seqId    int64
}

// OpReply 记录 server 中 Query、Join、Leave、Move 操作的结果，用于 server 回复 client 的 RPC 响应
type OpReply struct {
	ControllerConfig *ctrlerrpc.Config
	Err              Err
}

// LastOperationInfo 记录 clientId 对应的最后一次操作信息（seqId + reply），用于去重
type LastOperationInfo struct {
	SeqId int64
	Reply *OpReply
}
