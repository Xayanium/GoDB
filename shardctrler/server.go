package shardctrler

import (
	"GoDB/raft"
	raftrpc "GoDB/rpc/raft"
	"GoDB/rpc/shardctrler"
	"GoDB/tools"
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type ShardCtrler struct {
	mu             sync.Mutex
	me             int
	rf             *raft.Raft                   // Raft 实例
	applyCh        chan raft.ApplyMsg           // Raft 用于返回其已提交日志的通道
	dead           int32                        // Kill 标记
	lastApplied    int                          // 防止重复 应用 ApplyCh 中传递的日志
	stateMachine   *CtrlerStateMachine          // 控制状态机（真正执行 Join/Leave/Move/Query 并维护 Config 列表的地方）
	notifyChans    map[int]chan *OpReply        // 传递每条 client 操作的处理结果（client 的RPC请求封装为 Op 结构体并作为日志传入 Raft，等待 Raft 达成日志共识 和 server状态机执行后返回相关的结果）
	duplicateTable map[int64]*LastOperationInfo // 去重表，存clientId对应的最后一次操作信息（seqId + reply）

	shardctrler.UnimplementedShardCtrlerServer
}

var _ shardctrler.ShardCtrlerServer = (*ShardCtrler)(nil)

// StartServer 初始化 ShardCtrler 结构体，启动 Raft 实例和 applyTask 协程循环
func StartServer(servers []raftrpc.RaftServiceClient, me int, persister *tools.Persister) *ShardCtrler {
	sc := &ShardCtrler{}
	sc.me = me

	// 初始化 raft 相关字段
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// 初始化 shardctrler 相关字段
	sc.dead = 0
	sc.lastApplied = 0
	sc.stateMachine = NewCtrlerStateMachine()
	sc.notifyChans = make(map[int]chan *OpReply)
	sc.duplicateTable = make(map[int64]*LastOperationInfo)

	go sc.applyTask() // 启动协程循环，处理 Raft ApplyCh 中传回的信息
	return sc
}

// ============================================================================
// RPC Server 端实现，接收来自 client 的 Join/Leave/Move/Query 请求，封装成 Op 结构体，交给 Raft 同步日志，并等待 Raft 达成共识、状态机执行后得到结果并回复 client
// ============================================================================

func (sc *ShardCtrler) Join(ctx context.Context, req *shardctrler.JoinRequest) (*shardctrler.JoinResponse, error) {
	servers := make(map[int][]string)
	for k, v := range req.Servers {
		servers[int(k)] = v.Servers
	}
	reply := sc.command(Op{
		optype:   OpJoin,
		servers:  servers,
		clientId: req.ClientId,
		seqId:    req.SeqId,
	})
	return &shardctrler.JoinResponse{
		Err: string(reply.Err),
	}, nil
}

func (sc *ShardCtrler) Leave(ctx context.Context, req *shardctrler.LeaveRequest) (*shardctrler.LeaveResponse, error) {
	gid := make([]int, len(req.Gids))
	for i, v := range req.Gids {
		gid[i] = int(v)
	}
	reply := sc.command(Op{
		optype:   OpLeave,
		gids:     gid,
		clientId: req.ClientId,
		seqId:    req.SeqId,
	})
	return &shardctrler.LeaveResponse{
		Err: string(reply.Err),
	}, nil
}

func (sc *ShardCtrler) Move(ctx context.Context, req *shardctrler.MoveRequest) (*shardctrler.MoveResponse, error) {
	reply := sc.command(Op{
		optype:   OpMove,
		shard:    int(req.Shard),
		gid:      int(req.Gid),
		clientId: req.ClientId,
		seqId:    req.SeqId,
	})

	return &shardctrler.MoveResponse{
		Err: string(reply.Err),
	}, nil
}

func (sc *ShardCtrler) Query(ctx context.Context, req *shardctrler.QueryRequest) (*shardctrler.QueryResponse, error) {
	reply := sc.command(Op{
		optype: OpQuery,
		num:    int(req.Num),
	})
	return &shardctrler.QueryResponse{
		Err:    string(reply.Err),
		Config: reply.ControllerConfig,
	}, nil
}

// 通用的辅助方法，Op 结构体交给 Raft 同步日志，并等待 Raft 达成共识、状态机执行后得到结果并回复 client
func (sc *ShardCtrler) command(op Op) *OpReply {
	reply := &OpReply{}

	// 1. 写请求幂等性检验
	if op.optype != OpQuery {
		sc.mu.Lock() // 操作 map，加锁
		if info, ok := sc.duplicateTable[op.clientId]; ok {
			reply.ControllerConfig = info.Reply.ControllerConfig
			reply.Err = info.Reply.Err
			sc.mu.Unlock()
			return reply
		}
		sc.mu.Unlock()
	}

	// 2. 将 Op 结构体交给 Raft 同步日志
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader { // 只有 leader server 可以处理 client 的请求（leader 节点才能进行 Raft 同步）
		reply.Err = ErrWrongLeader
		return reply
	}

	// 3. 等待 Raft 达成共识、状态机执行后得到结果并回复 client
	sc.mu.Lock() // 操作 map，加锁
	if _, ok := sc.notifyChans[index]; !ok {
		sc.notifyChans[index] = make(chan *OpReply, 1)
	}
	notifyCh := sc.notifyChans[index]
	sc.mu.Unlock()

	select {
	case res := <-notifyCh:
		reply.ControllerConfig = res.ControllerConfig
		reply.Err = res.Err
	case <-time.After(time.Duration(ClientTimeout) * time.Millisecond):
		reply.Err = ErrTimeout
	}

	// 4. 无论成功或失败，都要清理 notifyChans 中对应 index 的通道，防止内存泄漏
	go func() {
		sc.mu.Lock()
		delete(sc.notifyChans, index)
		sc.mu.Unlock()
	}()

	return reply
}

// ============================================================================
// 将 Raft层 提交的日志应用到状态机，并通过 notifyChans 通道通知等待的 RPC Server 端
// ============================================================================

// 协程循环，处理 Raft ApplyCh 中传递的已提交日志(日志复制达成共识)，执行状态机，并将结果通过 notifyChans 通道通知等待的 RPC Server 端
func (sc *ShardCtrler) applyTask() {
	for !sc.isKilled() {
		select {
		case msg := <-sc.applyCh:
			if msg.CommandValid {
				sc.mu.Lock()
				// 不能重复应用 applyCh 中传来的日志
				if msg.CommandIndex <= sc.lastApplied {
					sc.mu.Unlock()
					continue
				}
				sc.lastApplied = msg.CommandIndex

				op := msg.Command.(Op)
				var reply *OpReply

				if op.optype == OpQuery {
					// 查询请求不需要幂等性处理，直接执行状态机
					reply = sc.applyToStateMachine(&op)
				} else {
					// 写请求幂等性处理：将写请求 clientId 对应的最后一次操作信息（seqId + reply）记录在去重表中
					if info, ok := sc.duplicateTable[op.clientId]; ok && info.SeqId == op.seqId {
						reply = info.Reply // 去重表有结果可直接返回
					} else {
						reply = sc.applyToStateMachine(&op)
						sc.duplicateTable[op.clientId] = &LastOperationInfo{
							SeqId: op.seqId,
							Reply: reply,
						}
					}
				}

				// raft应用结果通过 notifyChan 传回 leader server，用于返回 client RPC
				if _, isLeader := sc.rf.GetState(); isLeader {
					if _, ok := sc.notifyChans[msg.CommandIndex]; !ok {
						sc.notifyChans[msg.CommandIndex] = make(chan *OpReply, 1)
					}
					notifyCh := sc.notifyChans[msg.CommandIndex]

					select {
					case notifyCh <- &OpReply{
						ControllerConfig: reply.ControllerConfig,
						Err:              reply.Err,
					}:
					default:
					}
				}

				sc.mu.Unlock()
			}
		}
	}
}

// 将日志中传递的 Op 操作，在 状态机上执行（配置的 Get/Join/Move/Delete）
func (sc *ShardCtrler) applyToStateMachine(op *Op) *OpReply {
	var cfg *shardctrler.Config
	var err Err
	switch op.optype {
	case OpQuery:
		cfg, err = sc.stateMachine.Query(op.num)
	case OpJoin:
		err = sc.stateMachine.Join(op.servers)
	case OpLeave:
		err = sc.stateMachine.Leave(op.gids)
	case OpMove:
		err = sc.stateMachine.Move(op.shard, op.gid)
	}
	return &OpReply{
		ControllerConfig: cfg,
		Err:              err,
	}
}

// ============================================================================
// 协程循环终止相关方法
// ============================================================================

func (sc *ShardCtrler) isKilled() bool {
	return atomic.LoadInt32(&sc.dead) == 1
}

func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
}
