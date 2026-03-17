package shardkv

import (
	"GoDB/raft"
	raftrpc "GoDB/rpc/raft"
	ctrlerrpc "GoDB/rpc/shardctrler"
	kvrpc "GoDB/rpc/shardkv"
	"GoDB/shardctrler"
	"GoDB/tools"
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type ShardKV struct {
	mu   sync.Mutex
	me   int
	dead int32

	// Raft 相关字段
	rf          *raft.Raft
	applyCh     chan raft.ApplyMsg // KV server 和 Raft 之间通信的 applyCh，当日志被Raft中的多数派提交后（达成共识），通过其将信息传回 server
	lastApplied int                // 记录已应用到状态机的日志 index
	maxLogSize  int                // 触发快照的 Raft 日志大小阈值（单位：字节）

	// 分片处理相关字段
	getRPCClient func(string) kvrpc.ShardKVClient // 分片迁移时，当前Group需主动联系其他Group拉取数据，shardkv 只知道服务器名（字符串），根据其构造出 rpc client
	gid          int                              // 当前服务器所属的 group id（用于判断某个 key 是否属于本 Group、分片迁移时，判断某个 shard 该从哪个 Group 拉取）
	shards       map[int64]*MemoryKVStateMachine  // 每个shard对应一个用于处理shard的本地状态机（KV数据库）
	notifyChs    map[int]chan *OpReply            // server 提交一条日志给 Raft 等待共识，后台协程循环监听 applyCh 共识结果，通过该日志对应的 notifyChans 通知server

	duplicateTable map[int64]LastOperationInfo // 去重表，记录 clientId -> 最后一次已执行请求的信息（seqId + reply），保证写请求幂等

	// 用于迁移/GC时判断shard的归属与历史关系
	currentConfig *ctrlerrpc.Config  // 当前生效的分片配置（用于判断某个请求的 key 是否属于当前 Group）
	prevConfig    *ctrlerrpc.Config  // 上一个版本的分片配置（用于分片迁移，需要知道某个分片原来在哪个 Group，才能去那个 Group 拉取数据）
	mck           *shardctrler.Clerk // shardctrler 客户端

	kvrpc.UnimplementedShardKVServer
}

var _ kvrpc.ShardKVServer = (*ShardKV)(nil)

// StartServer 初始化 ShardKV 结构体，启动 Raft 实例和 applyTask 协程循环
func StartServer(raftServers []raftrpc.RaftServiceClient, me int, persister *tools.Persister, maxLogSize int,
	ctrlerServers []ctrlerrpc.ShardCtrlerClient, gid int, getRPCClient func(string) kvrpc.ShardKVClient) *ShardKV {
	// 1. 初始化 ShardKV 结构体
	s := &ShardKV{
		me:           me,
		dead:         0,
		applyCh:      make(chan raft.ApplyMsg),
		lastApplied:  0,
		maxLogSize:   maxLogSize,
		getRPCClient: getRPCClient,
		gid:          gid,
	}

	s.rf = raft.Make(raftServers, me, persister, s.applyCh)

	s.shards = make(map[int64]*MemoryKVStateMachine)
	s.notifyChs = make(map[int]chan *OpReply)
	s.duplicateTable = make(map[int64]LastOperationInfo)

	s.mck = shardctrler.MakeClerk(ctrlerServers)
	s.prevConfig = &ctrlerrpc.Config{Groups: make(map[int32]*ctrlerrpc.ServerList), Shards: make([]int32, 0)}
	s.currentConfig = &ctrlerrpc.Config{Groups: make(map[int32]*ctrlerrpc.ServerList), Shards: make([]int32, 0)}

	// 2. 从持久化器中读取磁盘中持久化的 snapshot，并从 snapshot 中恢复之前状态
	snapshot, err := persister.ReadSnapshot()
	if err != nil {
		return nil
	}
	s.restoreFromSnapshot(snapshot)

	// 3. 启动后台协程循环
	go s.applyTask()
	go s.fetchConfigTask()
	go s.shardMigrationTask()
	go s.shardGCTask()

	return s
}

// ============================================================================
// RPC Server 端实现，接收来自 client 的 Get/PutAppend 请求，封装成 Op 结构体，交给 Raft 同步日志，并等待 Raft 达成共识、状态机执行后得到结果并回复 client
// ============================================================================

func (s *ShardKV) Get(ctx context.Context, req *kvrpc.GetRequest) (reply *kvrpc.GetResponse, err error) {
	reply = &kvrpc.GetResponse{}

	// 1. 只有 key 所对应 shard 由当前 Group 负责、本地 shardKV 不处于迁移中时 server 才响应（根据当前生效的分片配置 currentConfig）
	s.mu.Lock() // matchGroup 中会访问 map，加锁保护
	if !s.matchGroup(req.Key) {
		s.mu.Unlock()
		reply.Err = kvrpc.ErrorCode_ERR_WRONG_GROUP
		return
	}
	s.mu.Unlock()

	// 3. 封装日志并提交到 Raft，等待 applyTask 处理后通过 notifyCh 传回结果
	opReply := s.command(RaftLog{
		CommandType: ClientOp,
		Data: Op{ // （handleCommand 会断言 Data 为 Op 类型）
			Optype: OpGet,
			Key:    req.Key,
		},
	})
	reply.Err = opReply.Err
	reply.Value = opReply.Value

	return
}

func (s *ShardKV) PutAppend(ctx context.Context, req *kvrpc.PutAppendRequest) (reply *kvrpc.PutAppendResponse, err error) {
	reply = &kvrpc.PutAppendResponse{}

	// 1. 只有 key 所对应 shard 由当前 Group 负责、本地 shardKV 不处于迁移中时 server 才响应（根据当前生效的分片配置 currentConfig）
	s.mu.Lock()
	if !s.matchGroup(req.Key) {
		s.mu.Unlock()
		reply.Err = kvrpc.ErrorCode_ERR_WRONG_GROUP
		return
	}

	// 2. 进行幂等性检验（根据 clientId 和 seqId 判断该请求是否已执行过，已执行过则直接返回上次的结果），duplicate 表在 applyTask 中更新
	if info, ok := s.duplicateTable[req.ClientId]; ok && info.SeqId >= req.SeqId {
		reply.Err = info.Reply.Err
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	// 3. 封装日志并提交到 Raft，等待 applyTask 处理后通过 notifyCh 传回结果
	opReply := s.command(RaftLog{
		CommandType: ClientOp,
		Data: Op{ // （handleCommand 会断言 Data 为 Op 类型）
			Optype:   OpType(req.Op),
			Key:      req.Key,
			Value:    req.Value,
			ClientId: req.ClientId,
			SeqId:    req.SeqId,
		},
	})
	reply.Err = opReply.Err

	return
}

// ShardMigration MoveOut shard 组作为 RPC 服务端为 MoveIn shard 组提供所需数据
func (s *ShardKV) ShardMigration(ctx context.Context, req *kvrpc.ShardOperationRequest) (*kvrpc.ShardOperationResponse, error) {
	// 1. 只有 Leader server 需要响应 RPC 请求
	if _, isLeader := s.rf.GetState(); !isLeader {
		return &kvrpc.ShardOperationResponse{Err: kvrpc.ErrorCode_ERR_WRONG_LEADER}, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// 2. 检验当前配置号是否不小于参数传入的配置号（对方要求我提供 ConfigNum=args.ConfigNum 的迁移数据，但我当前配置比对方请求的配置更旧，表明我当前还没成功应用新配置）
	if s.currentConfig.Num < req.ConfigNum {
		return &kvrpc.ShardOperationResponse{Err: kvrpc.ErrorCode_ERR_NOT_READY}, nil
	}

	// 3. 拷贝 shard 中的 状态机（KV数据库）
	shardData := make(map[int32]*kvrpc.ShardData)
	for _, shardId := range req.ShardIds {
		stateMachine := s.getKVMachine(int64(shardId))
		if stateMachine == nil {
			continue
		}

		kv := make(map[string]string)
		for k, v := range stateMachine.KV {
			kv[k] = v
		}
		shardData[shardId] = &kvrpc.ShardData{
			Kv: kv,
		}
	}

	// 4. 拷贝去重表
	duplicateTable := make(map[int64]*kvrpc.LastOperationInfo)
	for clientId, info := range s.duplicateTable {
		duplicateTable[clientId] = &kvrpc.LastOperationInfo{
			SeqId: info.SeqId,
			Reply: &kvrpc.OpReply{
				Value: info.Reply.Value,
				Err:   info.Reply.Err,
			},
		}
	}

	return &kvrpc.ShardOperationResponse{
		Err:            kvrpc.ErrorCode_OK,
		ConfigNum:      req.ConfigNum,
		ShardData:      shardData,
		DuplicateTable: duplicateTable,
	}, nil
}

// ShardGC MoveOut shard 组作为 RPC 服务端接收 MoveIn shard 组的 GC 请求，并删除对应 shard 数据
func (s *ShardKV) ShardGC(ctx context.Context, req *kvrpc.ShardOperationRequest) (*kvrpc.ShardOperationResponse, error) {
	// 1. 只有 Leader server 需要响应 RPC 请求
	if _, isLeader := s.rf.GetState(); !isLeader {
		return &kvrpc.ShardOperationResponse{Err: kvrpc.ErrorCode_ERR_WRONG_LEADER}, nil
	}

	s.mu.Lock()
	// 2. 检验当前配置号是否大于参数传入的配置号（如果当前节点配置比请求中的配置新，那就说明这些分片已经在当前节点被清理过了，避免重复清理）
	if s.currentConfig.Num > req.ConfigNum {
		return &kvrpc.ShardOperationResponse{Err: kvrpc.ErrorCode_OK}, nil
	}
	s.mu.Unlock()

	// 3. 将删除 shard 的操作提交给 Raft 进行共识同步（Leader server 同步给 Follower），真正的清理发生在 applyShardGC 中，且同一组的所有节点都会执行
	// 传递的参数：删哪些 shard、哪一轮配置删除的
	reply := s.command(RaftLog{
		CommandType: ShardGC,
		Data:        req, // （handleCommand 会断言 Data 为 *kvrpc.ShardOperationRequest 类型）
	})
	return &kvrpc.ShardOperationResponse{
		Err:       reply.Err,
		ConfigNum: req.ConfigNum,
	}, nil
}

// ============================================================================
// server 启动时需要在后台启动的协程循环
// ============================================================================

// 把 Raft 达成共识并通过 applyCh 传回的 日志/快照，交由对应的逻辑处理，并将处理结果通过 notifyCh 返回
func (s *ShardKV) applyTask() {
	for !s.isKilled() {
		// 1. 从 applyCh 读取 ApplyMsg
		select {
		case msg := <-s.applyCh:
			// 2. 如果 Msg 是日志，进行日志相关逻辑处理
			if msg.CommandValid {
				s.mu.Lock()
				// 2.1 根据 lastApplied 判断日志是否已处理过，未处理过则处理并更新 lastApplied
				if s.lastApplied >= msg.CommandIndex {
					s.mu.Unlock()
					continue
				}
				s.lastApplied = msg.CommandIndex

				// 2.2 根据 Msg.CommandType 区分类型，进行不同的处理，得到处理结果
				opReply := s.handleCommand(msg.Command)

				// 2.3 如果当前 server 是 Leader，需将处理结果通过 notifyCh 返回
				if _, isLeader := s.rf.GetState(); isLeader {
					if _, ok := s.notifyChs[msg.CommandIndex]; !ok {
						s.notifyChs[msg.CommandIndex] = make(chan *OpReply, 1)
					}
					notifyCh := s.notifyChs[msg.CommandIndex]
					notifyCh <- opReply
				}

				// 2.4 判断 Raft 中日志是否需要合入快照（根据持久化器中保存的raft state的大小是否超设定的阈值）
				if s.maxLogSize != -1 && s.rf.GetStateSize() >= s.maxLogSize {
					s.makeSnapshot(msg.CommandIndex)
				}

				s.mu.Unlock()
			}

			// 3. 如果 Msg 是快照，进行快照相关逻辑处理
			if msg.SnapshotValid {
				s.mu.Lock()
				// 3.1 从已持久化保存的快照中重新加载状态机相关字段，使得 ShardKV 和 Raft 保存的状态一致
				s.restoreFromSnapshot(msg.Snapshot)

				// 3.2 更新 lastApplied 为快照中最后一条日志的 index
				s.lastApplied = msg.SnapshotIndex
				s.mu.Unlock()
			}
		}
	}
}

// 定期向 shardctrler 拉取新的配置，并把“配置变更”也作为一条日志交给 Raft 达成共识，并在 applyTask 中被应用到状态机
func (s *ShardKV) fetchConfigTask() {
	for !s.isKilled() {
		// 1. 只有 server 为 Leader 时执行操作
		if _, isLeader := s.rf.GetState(); !isLeader {
			time.Sleep(time.Duration(FetchConfigInterval) * time.Millisecond)
			continue
		}

		// 2. 只有当所有分片都处于 Normal 状态时才能拉取新配置（避免出现分片重复迁移/数据丢失/配置冲突）
		needFetch := true
		s.mu.Lock() // 遍历 map 需要加锁保护
		for _, kv := range s.shards {
			if kv.Status != Normal {
				needFetch = false
				break
			}
		}
		curConfNum := s.currentConfig.Num // 锁内获取共享变量，避免并发问题
		s.mu.Unlock()

		// 3. 向 shardctrler 拉取新配置（根据当前生效的配置 currentConfig 的 Num 字段拉取下一个版本的配置）
		if needFetch {
			newConf := s.mck.Query(int(curConfNum) + 1)
			if newConf.Num == curConfNum+1 { // 检查拉取到的配置是否是当前配置的下一个版本，避免重复拉取/处理同一版本配置
				// 4. 若存在新配置且当前没有未完成迁移，将新配置封装成日志交给 Raft 达成共识（Leader同步给Follower），再在 applyTask 中被 execConfigChange 处理

				s.command(RaftLog{
					CommandType: ConfigChange,
					Data:        newConf, // （handleCommand 会断言 Data 为 *ctrlerrpc.Config 类型）
				})
			}
		}
		time.Sleep(time.Duration(FetchConfigInterval) * time.Millisecond)
	}
}

// 当本组发现某些 shard 数据需要“从别的组迁入”，它负责向 迁出 group 拉取这些 shard 的数据并进行数据拷贝。
// （只有当fetchConfigTask完成、Raft完成日志共识，execConfigChange 才会更改迁入组的 shard Status 为 MoveIn，从而被该循环检测到）
func (s *ShardKV) shardMigrationTask() {
	for !s.isKilled() {
		// 1. 只有当前 server 为 Leader 时执行操作
		if _, isLeader := s.rf.GetState(); !isLeader {
			time.Sleep(time.Duration(ShardMigrationInterval) * time.Millisecond)
			continue
		}

		// 2. 找到所有状态为 MoveIn 的 shard，并映射为 gid -> []shardId
		s.mu.Lock() // 对 map 访问需要加锁保护
		gidToShards := s.getGidToShardsByStatus(MoveIn)
		s.mu.Unlock()

		// 3. 在协程中对所有需迁出 shard 的组发送 RPC 请求，拉取对应 shard 数据（包含 shardKV 状态机数据 + duplicate 表中与该 shard 相关的数据）
		for gid, shardIds := range gidToShards {
			go func(servers []string, configNum int32, _shardIds []int32) {
				req := &kvrpc.ShardOperationRequest{
					ConfigNum: configNum,
					ShardIds:  _shardIds,
				}

				for _, server := range servers {
					// 3.1 对其他组的操作
					// 对每个迁出组中所有的服务器均发送 RPC 请求，直到从 Leader server 获取到对应的 shard 数据
					client := s.getRPCClient(server)
					shardData, err := client.ShardMigration(context.Background(), req) // RPC 调用
					if err == nil && shardData.Err == kvrpc.ErrorCode_OK {
						// 3.2 RPC成功后对本组的操作
						// 当前 server 将拉取到的 shard 数据封装成日志交给 Raft 达成共识（Leader同步给Follower），再在 applyTask 中被 execShardMigration 处理
						s.command(RaftLog{
							CommandType: ShardMigration,
							Data: &kvrpc.ShardOperationResponse{ //（handleCommand 会断言 Data 为 *kvrpc.ShardOperationResponse 类型）
								Err:            shardData.Err,
								ConfigNum:      shardData.ConfigNum,
								ShardData:      shardData.ShardData,
								DuplicateTable: shardData.DuplicateTable,
							},
						})
					}
				}
			}(s.prevConfig.Groups[gid].Servers, s.currentConfig.Num, shardIds) // 由于 execConfigChange 更新了 currentConfig，服务器现在真实的 servers 配置被保存在了 preConfig 中
		}

		time.Sleep(time.Duration(ShardMigrationInterval) * time.Millisecond)
	}
}

// 当迁移完成后，迁入 group 通知 迁出 group 可以删除它不再负责的 shard 数据
// （只有当shardMigrationTask完成、Raft完成日志共识，execShardMigration 才会更改 MoveIn shard Status 为 GC）
func (s *ShardKV) shardGCTask() {
	for !s.isKilled() {
		// 1. 只有 server 为 Leader 时执行操作
		if _, isLeader := s.rf.GetState(); !isLeader {
			time.Sleep(time.Duration(ShardGCInterval) * time.Millisecond)
			continue
		}

		// 2. 找到所有状态为 GC 的 shard，并映射为 gid -> []shardId
		s.mu.Lock() // 对 map 访问需要加锁保护
		gidToShards := s.getGidToShardsByStatus(GC)
		s.mu.Unlock()

		// 3. 在协程中对所有 GC 组发送 RPC 请求，请求对方删除相关 shard 数据
		for gid, shardIds := range gidToShards {
			go func(servers []string, configNum int32, _shardIds []int32) {
				req := &kvrpc.ShardOperationRequest{
					ConfigNum: configNum,
					ShardIds:  _shardIds,
				}

				for _, server := range servers {
					// 3.1 对其他组的操作
					// 对每个 GC 组中所有的服务器均发送 RPC 请求，直到从 Leader server 收到响应
					client := s.getRPCClient(server)
					resp, err := client.ShardGC(context.Background(), req) // RPC 调用
					if err == nil && resp.Err == kvrpc.ErrorCode_OK {
						// 3.2 对本组的操作
						// 当前 server 将 GC 操作封装成日志交给 Raft 达成共识（Leader同步给Follower），再在 applyTask 中被 execShardGC 处理
						s.command(RaftLog{
							CommandType: ShardGC,
							Data:        req, //（handleCommand 会断言 Data 为 *kvrpc.ShardOperationRequest 类型）
						})
					}
				}

			}(s.prevConfig.Groups[gid].Servers, s.currentConfig.Num, shardIds) // 由于 execConfigChange 更新了 currentConfig，服务器现在真实的 servers 配置被保存在了 preConfig 中
		}

		time.Sleep(time.Duration(ShardGCInterval) * time.Millisecond)
	}
}

// ============================================================================
// 辅助函数
// ============================================================================

// 封装操作：将日志提交到 Raft，并接收 applyTask 处理后通过 notifyCh 传回的结果
func (s *ShardKV) command(command RaftLog) *OpReply {
	reply := &OpReply{
		Err: kvrpc.ErrorCode_OK,
	}

	// 1. 调用 Raft 提供的 Start 接口，将各操作封装成的日志写入 Raft
	index, _, isLeader := s.rf.Start(command)

	// 2. 只有 Raft Leader 节点进行日志同步
	if !isLeader {
		reply.Err = kvrpc.ErrorCode_ERR_WRONG_LEADER
		return reply
	}

	// 3. 根据 Raft 返回的日志全局 Index ，创建 "等待 Raft 应用日志的结果" 的 notifyCh
	s.mu.Lock()
	if _, ok := s.notifyChs[index]; !ok {
		s.notifyChs[index] = make(chan *OpReply, 1)
	}
	notifyCh := s.notifyChs[index]
	s.mu.Unlock()

	// 4. 阻塞等待结果传回（等待 Raft 日志达成共识后通过 ApplyCh 传回 notifyCh），并返回结果
	select {
	case res := <-notifyCh:
		reply.Err = res.Err
		reply.Value = res.Value
	case <-time.After(time.Duration(ClientTimeout) * time.Millisecond):
		reply.Err = kvrpc.ErrorCode_ERR_TIMEOUT
	}

	// 5. 异步清理 notifyCh，防止内存泄漏
	go func() {
		s.mu.Lock()
		delete(s.notifyChs, index)
		s.mu.Unlock()
	}()

	return reply
}

// 判断 key 所对应 shard 是否由当前 Group 负责，以及判断本地的 shardKV 是否处于迁移中状态
func (s *ShardKV) matchGroup(key string) bool {
	shardId := key2shard(key)
	kv := s.getKVMachine(shardId)
	if kv == nil {
		return false
	}
	// 3. 只有状态为 Normal 和 GC（已拉取到数据但对方还未完全清理删除的老版本兼容支持）时才允许访问数据
	status := kv.Status
	return s.currentConfig.Shards[shardId] == int32(s.gid) && (status == Normal || status == GC)
}

// 根据 shardId 获取对应的 shard 状态机（KV数据库）
func (s *ShardKV) getKVMachine(shardId int64) *MemoryKVStateMachine {
	if kv, ok := s.shards[shardId]; ok {
		return kv
	}
	return nil
}

// 根据 shard Status，获取对应的 shardId，并将对应的 shardId 合并为 gid -> []shardId
func (s *ShardKV) getGidToShardsByStatus(status KVStatus) map[int32][]int32 {
	gidToShards := make(map[int32][]int32)
	for shardId, kv := range s.shards {
		if kv.Status == status {
			// 由于 execConfigChange 更新了 currentConfig，服务器现在真实的 shard -> gid 关系被保存在了 preConfig 中
			gid := s.prevConfig.Shards[shardId]
			if gid == 0 { // gid 为 0 表示该 shard 在配置中未分配给任何组，不需要迁移/GC
				continue
			}

			if _, ok := gidToShards[gid]; !ok {
				gidToShards[gid] = make([]int32, 0)
			}
			gidToShards[gid] = append(gidToShards[gid], int32(shardId))
		}
	}
	return gidToShards
}

// ============================================================================
// applyTask 协程循环中处理 Raft 传回的日志的相关函数
// ============================================================================

// 根据 applyMsg 的 CommandType 区分日志类型，进行不同的处理（已在包含在加锁的环境中，其中的代码不需加锁）
func (s *ShardKV) handleCommand(command interface{}) *OpReply {
	cmd := command.(RaftLog) // 将 Raft 传回的日志断言为 Op 结构体
	switch cmd.CommandType {
	case ClientOp:
		op := cmd.Data.(Op) // 将 Raft 传回的日志中的 Data 字段断言为 Op 结构体
		s.execClientOP(&op) // 当 Raft 对一条 "客户端操作日志"（Get/Put/Append）达成共识并返回结果后执行该函数
	case ConfigChange:
		newConf := cmd.Data.(*ctrlerrpc.Config) // 将 Raft 传回的日志中的 Data 字段断言为 shardctrler 配置结构体
		s.execConfigChange(newConf)             // 当 Raft 对一条 "应用新配置日志" 达成共识并返回结果后执行该函数
	case ShardMigration:
		shardData := cmd.Data.(*kvrpc.ShardOperationResponse) // 将 Raft 传回的日志中的 Data 字段断言为 shard 迁移 RPC 的响应结构体
		s.execShardMigration(shardData)                       // 当 Raft 对一条 "迁移分片数据日志" 达成共识并返回结果后执行该函数
	case ShardGC:
		shardInfo := cmd.Data.(*kvrpc.ShardOperationRequest) // 将 Raft 传回的日志中的 Data 字段断言为 shard GC RPC 的请求结构体
		s.execShardGC(shardInfo)                             // 当 Raft 对一条 "分片垃圾回收日志" 达成共识并返回结果后执行该函数
	}
	return &OpReply{Err: kvrpc.ErrorCode_ERR_INTERNAL}
}

// 将客户端请求的操作作用到本地的 shard 状态机（KV数据库）
func (s *ShardKV) execClientOP(op *Op) *OpReply {
	// 1. 再检查一次 key 对应的 shard 当前是否属于本组、且本地 shardKV 不处于迁移状态
	// （即使最初 leader 收到 RPC 时认为 shard 属于本组，可能在 “提交日志到 Raft 并达成共识” 这段时间内发生配置变化）
	if !s.matchGroup(op.Key) {
		return &OpReply{Err: kvrpc.ErrorCode_ERR_WRONG_GROUP}
	}

	// 2. 对写操作进行去重检验，如果重复直接返回旧 Reply
	if op.Optype != OpGet {
		if info, ok := s.duplicateTable[op.ClientId]; ok && info.SeqId >= op.SeqId {
			return info.Reply
		}
	}

	// 3. 将操作实际应用到 shard 状态机（KV数据库）
	stateMachine := s.getKVMachine(key2shard(op.Key))
	if stateMachine == nil {
		return &OpReply{Err: kvrpc.ErrorCode_ERR_NOT_READY}
	}

	var reply *OpReply
	switch op.Optype {
	case OpGet:
		reply.Value, reply.Err = stateMachine.Get(op.Key)
	case OpPut:
		reply.Err = stateMachine.Put(op.Key, op.Value)
	case OpAppend:
		reply.Err = stateMachine.Append(op.Key, op.Value)
	}

	// 4. 写操作更新 duplicate 表
	if op.Optype != OpGet {
		s.duplicateTable[op.ClientId] = LastOperationInfo{
			SeqId: op.SeqId,
			Reply: reply,
		}
	}

	return reply
}

// 应用一份新的 shardCtrler 配置，并更新每个 shard 的状态（决定哪些 shard 需迁入/迁出），使每组服务器的 shardMigrationTask 协程循环可执行
func (s *ShardKV) execConfigChange(newConf *ctrlerrpc.Config) *OpReply {
	// 1. 检查 newConf 是否是 currentConfig 的下一个版本，避免重复应用同一版本配置导致状态混乱
	if newConf.Num != s.currentConfig.Num+1 {
		return &OpReply{Err: kvrpc.ErrorCode_ERR_WRONG_CONFIG}
	}

	for i := 0; i < shardctrler.NShards; i++ {
		stateMachine := s.getKVMachine(int64(i))
		if stateMachine == nil {
			stateMachine = NewMemoryKVStateMachine()
			s.shards[int64(i)] = stateMachine
		}

		// 2. 如果是要将某个 shard 从其他 Group 迁入当前 Group
		if s.currentConfig.Shards[i] != int32(s.gid) && newConf.Shards[i] == int32(s.gid) {
			// 进行迁入时，如果分片i在旧配置中属于 group_0（未分配分片组），表示将分片i分配给当前组，不需要从其他组迁移数据
			if s.currentConfig.Shards[i] != 0 {
				stateMachine.Status = MoveIn // 更新该 shard 的状态为需要迁入（MoveIn），等待后台协程去拉取数据并应用到状态机
			}
		}

		// 3. 如果是要将某个 shard 从当前 Group 迁出到其他 Group
		if s.currentConfig.Shards[i] == int32(s.gid) && newConf.Shards[i] != int32(s.gid) {
			// 进行迁出时，如果分片i在新配置中属于 group_0（未分配分片组），表示当前组的分片i要被回收，不需要迁移数据到其他组
			if newConf.Shards[i] != 0 {
				stateMachine.Status = MoveOut // 更新该 shard 的状态为需要迁出（MoveOut），等待后台协程去拉取数据并应用到状态机
			}
		}
	}

	// 4. 更新新旧配置
	s.prevConfig = s.currentConfig
	s.currentConfig = newConf

	return &OpReply{Err: kvrpc.ErrorCode_OK}
}

// 将从旧组拉来的 shard 数据安装到本组对应 shard 上，并标记旧组对应的 shard Status 为 GC，使每组服务器的 shardGCTask 协程循环可执行
func (s *ShardKV) execShardMigration(data *kvrpc.ShardOperationResponse) *OpReply {
	// 1. 上下文检验：只有当前配置未发生变化时（当前配置号与 "Raft 达成共识后传回上层进行应用" 的配置号一致时），才能尝试安装旧组的 shard
	if data.ConfigNum != s.currentConfig.Num {
		return &OpReply{Err: kvrpc.ErrorCode_ERR_WRONG_CONFIG}
	}

	// 2. 安装从旧组中获取的 shard 数据，存入KV数据库，并将该 shard 状态置为 GC，等待之后通知旧组删除数据
	for shardId, shard := range data.ShardData {
		stateMachine := s.getKVMachine(int64(shardId))
		if stateMachine == nil {
			stateMachine = NewMemoryKVStateMachine()
			stateMachine.Status = MoveIn // 新建的 shard 状态机在初始化时状态为 Normal，需改为 MoveIn 标记使得后续数据迁移执行
			s.shards[int64(shardId)] = stateMachine
		}
		if stateMachine.Status == MoveIn {
			for key, value := range shard.Kv {
				stateMachine.KV[key] = value
			}
			stateMachine.Status = GC
		}
	}

	// 3. 拷贝旧组中的去重表（对于每个 clientId，如果新组中没有该记录，或 SeqId 更小，则使用旧组中的内容）
	for clientId, info := range data.DuplicateTable {
		table, ok := s.duplicateTable[clientId]
		if !ok || table.SeqId < info.SeqId {
			s.duplicateTable[clientId] = LastOperationInfo{
				SeqId: info.SeqId,
				Reply: &OpReply{
					Value: info.Reply.Value,
					Err:   info.Reply.Err,
				},
			}
		}
	}

	return &OpReply{Err: kvrpc.ErrorCode_OK}
}

// 旧组执行删除已迁出的 shard 数据，新、旧组更改状态为 Normal，使每组服务器的 fetchConfigTask 协程循环检测可执行
func (s *ShardKV) execShardGC(info *kvrpc.ShardOperationRequest) *OpReply {
	// 1. 上下文检验：只有当前配置未发生变化时（当前配置号与 "Raft 达成共识后传回上层进行应用" 的配置号一致时），才能尝试进行 shard GC
	if info.ConfigNum != s.currentConfig.Num {
		return &OpReply{Err: kvrpc.ErrorCode_ERR_WRONG_CONFIG}
	}

	// 2. 由于完成迁移后，迁入组、迁出组都需要进行 GC 操作，但操作逻辑不同，根据 shard 状态进行区分：
	for _, shardId := range info.ShardIds {
		stateMachine := s.getKVMachine(int64(shardId))
		if stateMachine == nil {
			continue
		}

		// 2.1 对处于 GC 状态的 shard （完成了"从旧组迁入新组"的 新组的 shard），将状态改为 Normal
		if stateMachine.Status == GC {
			stateMachine.Status = Normal
		}

		// 2.2 对处于 MoveOut 状态的 shard（旧组 shard 中数据已被迁入别的组），清理 shard 对应的 KV数据库
		if stateMachine.Status == MoveOut {
			s.shards[int64(shardId)] = NewMemoryKVStateMachine() // 通过新建一个 shard 状态机来清空原有数据
		}
	}

	return &OpReply{Err: kvrpc.ErrorCode_OK}
}

// ============================================================================
// 快照相关函数（快照shards、duplicateTable、currentConfig、prevConfig四个字段）
// 快照shards原因：重启需重放 100 万条 Put，有快照则直接恢复
// 快照duplicateTable原因：保证重启后，客户端写操作不会重复执行
// 快照currentConfig原因：用于判断 key 归属，否则重启后相关请求无法正常处理
// 快照prevConfig原因：重启后可能导致分片迁移无法进行
// ============================================================================

// 制作快照，并交给 Raft 保存到持久化器中（上层已将状态机做到了 index 快照，Raft层 可将 <=index 的日志删除掉，并将传入的快照持久化）
func (s *ShardKV) makeSnapshot(index int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// gob 序列化时会进行深拷贝，这里不用处理深拷贝问题
	snapshot := tools.Serialize(ShardKVSnapshot{
		Shards:         s.shards,
		DuplicateTable: s.duplicateTable,
		CurrentConfig:  s.currentConfig,
		PrevConfig:     s.prevConfig,
	})

	s.rf.Snapshot(index, snapshot)
}

// 从 Raft 持久化到持久化器中的 snapshot 恢复状态机相关字段
func (s *ShardKV) restoreFromSnapshot(snapshot []byte) {
	shardkvSnapshot := &ShardKVSnapshot{}
	err := tools.Deserialize(snapshot, shardkvSnapshot)
	if err != nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.shards = shardkvSnapshot.Shards
	s.duplicateTable = shardkvSnapshot.DuplicateTable
	s.currentConfig = shardkvSnapshot.CurrentConfig
	s.prevConfig = shardkvSnapshot.PrevConfig
}

// ============================================================================
// 协程循环终止相关方法
// ============================================================================

func (s *ShardKV) Kill() {
	atomic.StoreInt32(&s.dead, 1)
	s.rf.Kill()
}

func (s *ShardKV) isKilled() bool {
	return atomic.LoadInt32(&s.dead) == 1
}
