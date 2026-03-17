package shardkv

import (
	"GoDB/config"
	ctrlerrpc "GoDB/rpc/shardctrler"
	kvrpc "GoDB/rpc/shardkv"
	"GoDB/shardctrler"
	"crypto/rand"
	"math/big"
	"time"

	"golang.org/x/net/context"
)

type Clerk struct {
	ctrler *shardctrler.Clerk // shardControler client，用于从 controler拉最新配置

	// 本地缓存的controler配置
	// config.Shards[shard] -> gid（这个 shard 当前属于哪个 group）
	// config.Groups[gid] -> []serverName（该 group 的服务器列表）
	config *ctrlerrpc.Config

	// shardkv 客户端需要向服务端构造并发送请求，但客户端只能通过 shardctrler 中的配置得到服务端服务器名（字符串）
	getRPCClient func(string) kvrpc.ShardKVClient // 函数根据 config.Group[gid] 获取到服务器名，构造出 rpc client
	leaderIds    map[int]int                      // gid -> leader 索引缓存，避免下一次请求的时候去轮询查找 Leader

	// clientID+seqId 确定一个唯一的命令
	clientId int64
	seqId    int64
}

func MakeClerk(ctrler *shardctrler.Clerk, getRPCClient func(string) kvrpc.ShardKVClient) *Clerk {
	return &Clerk{
		ctrler:       ctrler,
		getRPCClient: getRPCClient,
		leaderIds:    make(map[int]int),
		clientId:     nrand(),
		seqId:        1,
	}
}

// ============================================================================
// 作为 client 实例被其他服务调用（如数据库连接进程等），和 server 端进行 RPC 通信
// ============================================================================

// Get 读取 key 对应 value，key不存在返回空字符串
func (ck *Clerk) Get(key string) string {
	return ck.rpcRequest(key, "", OpGet)
}

// Put 将 key 对应的 value 更新为 value，如果 key 不存在则创建
func (ck *Clerk) Put(key string, value string) {
	ck.rpcRequest(key, value, OpPut)
}

// Append 将 value 追加到 key 原有的 value 后面，如果 key 不存在则创建
func (ck *Clerk) Append(key string, value string) {
	ck.rpcRequest(key, value, OpAppend)
}

// 向 Server 进行 RPC 请求，根据 op 类型区分 Get、Put、Append 操作
func (ck *Clerk) rpcRequest(key string, value string, opType OpType) string {
	for {
		if ck.config == nil {
			ck.config = ck.ctrler.Query(-1)
		}

		// 1. 通过 key2shard 确定 shard
		gid := ck.config.Shards[int32(key2shard(key))]

		// 2. 从本地存的 shardControler 配置中 找负责这个 shard 的 group，获取到 group 中所有的服务器列表
		if serverList, ok := ck.config.Groups[gid]; ok {
			if _, exist := ck.leaderIds[int(gid)]; !exist {
				ck.leaderIds[int(gid)] = 0 // 初始化该 group 中的 leader 为0号
			}
			curLeader := ck.leaderIds[int(gid)] // 缓存当前 leaderId，当 group 中所有节点都不能作为leader时，重新拉取controler配置
			oldLeader := curLeader

			for {
				// 2. 获取到 group 中 leader 对应的服务器名称，构造 RPC client
				server := ck.getRPCClient(serverList.Servers[curLeader])

				switch opType {
				case OpGet:
					args := &kvrpc.GetRequest{Key: key}
					reply, err := server.Get(context.Background(), args)
					if err == nil {
						if reply.Err == kvrpc.ErrorCode_OK || reply.Err == kvrpc.ErrorCode_ERR_NO_KEY {
							return reply.Value
						}
						if reply.Err == kvrpc.ErrorCode_ERR_WRONG_GROUP {
							break
						}
					}

				case OpPut, OpAppend:
					args := &kvrpc.PutAppendRequest{
						Key:      key,
						Value:    value,
						Op:       int32(opType),
						ClientId: ck.clientId,
						SeqId:    ck.seqId,
					}
					reply, err := server.PutAppend(context.Background(), args)
					if err == nil {
						if reply.Err == kvrpc.ErrorCode_OK {
							ck.seqId++ // 写操作成功时要进行幂等
							return ""
						}
						if reply.Err == kvrpc.ErrorCode_ERR_WRONG_GROUP {
							break
						}
					}
				}

				// 上述逻辑执行失败后（可能发生了 leader 的改变），对 group 中的所有服务器均尝试进行 RPC，直到成功 或 已尝试过所有服务器
				curLeader = (curLeader + 1) % len(serverList.Servers)
				ck.leaderIds[int(gid)] = curLeader // 更新 leaderId 缓存
				if curLeader == oldLeader {
					break
				}
			}
		}

		// 如果shard不属于当前组，或对组内所有节点尝试RPC均失败，需要重新拉取 controler 配置，更新本地缓存的配置
		ck.config = ck.ctrler.Query(-1)
		time.Sleep(time.Duration(config.Get().ShardKV.GetConfInterval) * time.Millisecond)
	}
}

// ============================================================================
// 辅助函数
// ============================================================================

// 为 clientId 生成全局唯一的随机 int64
func nrand() int64 {
	mx := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, mx)
	x := bigx.Int64()
	return x
}
