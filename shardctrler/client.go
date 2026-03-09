package shardctrler

import (
	ctrlerrpc "GoDB/rpc/shardctrler"
	"context"
	"crypto/rand"
	"math/big"
)

type Clerk struct {
	servers  []ctrlerrpc.ShardCtrlerClient // 所有可联系的 shardctrler 服务器 RPC 端点列表
	leaderId int                           // client 认为当前的leader的下标（使用 servers[leaderId] 获取 leader 服务实例）
	clientId int64                         // client 的唯一ID，用于 server 识别哪个 client 发的请求
	seqId    int64                         // 请求序号
	// todo: log 日志
}

func MakeClerk(servers []ctrlerrpc.ShardCtrlerClient) *Clerk {
	return &Clerk{
		servers:  servers,
		leaderId: 0,
		clientId: nrand(),
		seqId:    0,
	}
}

// ============================================================================
// 作为 client 实例被其他服务调用（shardKV），和 server 端进行 RPC 通信
// ============================================================================

// Query 向 shardctrler server 查询指定编号的配置，编号为-1则返回最新配置（外部接口）
func (ck *Clerk) Query(num int) *ctrlerrpc.Config {
	for {
		args := &ctrlerrpc.QueryRequest{Num: int32(num)}
		// 先给当前 leaderId server 发送 ShardCtrler.Query RPC
		reply, err := ck.servers[ck.leaderId].Query(context.Background(), args)
		if err != nil || reply.Err != "" {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}

		// 如果 RPC 成功则返回相关配置
		return reply.Config
	}

}

// Join 向 shardctrler server 申请添加新的 replica group（group 中有很多的 ShardKV 服务）
func (ck *Clerk) Join(servers map[int][]string) {
	for {
		res := make(map[int32]*ctrlerrpc.ServerList)
		for k, v := range servers {
			res[int32(k)] = &ctrlerrpc.ServerList{Servers: v}
		}

		args := &ctrlerrpc.JoinRequest{
			Servers:  res,
			ClientId: ck.clientId,
			SeqId:    ck.seqId,
		}
		// 先给当前 leaderId server 发送 ShardCtrler.Join RPC
		reply, err := ck.servers[ck.leaderId].Join(context.Background(), args)
		if err != nil || reply.Err != "" {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}

		//如果 RPC 成功则将 seqId 推进到下一个（ClientId 和 SeqId 用于 server 中去重幂等）
		ck.seqId++
		return
	}
}

// Leave 向 shardctrler server 申请从配置中删除某些 replica group
func (ck *Clerk) Leave(gids []int) {
	for {
		var gids32 []int32
		for _, gid := range gids {
			gids32 = append(gids32, int32(gid))
		}

		args := &ctrlerrpc.LeaveRequest{
			Gids:     gids32,
			ClientId: ck.clientId,
			SeqId:    ck.seqId,
		}
		// 先给当前 leaderId server 发送 ShardCtrler.Leave RPC
		reply, err := ck.servers[ck.leaderId].Leave(context.Background(), args)
		if err != nil || reply.Err != "" {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}

		// 如果 RPC 成功则将 seqId 推进到下一个（ClientId 和 SeqId 用于 server 中去重幂等）
		ck.seqId++
		return
	}
}

// Move 向 shardctrler server 申请将某个 shard 指派给指定的 replica group
func (ck *Clerk) Move(shard int, gid int) {
	for {
		args := &ctrlerrpc.MoveRequest{
			Shard:    int32(shard),
			Gid:      int32(gid),
			ClientId: ck.clientId,
			SeqId:    ck.seqId,
		}
		// 先给当前 leaderId server 发送 ShardCtrler.Move RPC
		reply, err := ck.servers[ck.leaderId].Move(context.Background(), args)
		if err != nil || reply.Err != "" {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}

		// 如果 RPC 成功则将 seqId 推进到下一个（ClientId 和 SeqId 用于 server 中去重幂等）
		ck.seqId++
		return
	}
}

// 生成足够大的随机 int64，用作 clientId
func nrand() int64 {
	mx := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, mx)
	x := bigx.Int64()
	return x
}
