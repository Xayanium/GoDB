package raft

import (
	raftrpc "GoDB/rpc/raft"
	"GoDB/tools"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Leader    = 0
	Candidate = 1
	Follower  = 2
)

const (
	InvalidIndex = 0
	InvalidTerm  = 0
)

type ApplyMsg struct {
	CommandValid bool        // 是否是上层状态机需要执行的日志条目
	Command      interface{} // 日志条目里存的用户命令（kv server 的 Put/Append/Get 请求内容等）
	CommandIndex int         // 该命令在节点本地日志中的索引号（log index）

	SnapshotValid bool   // 是否快照
	Snapshot      []byte // 快照内容（kvserver 序列化后的状态）
	SnapshotTerm  int    // 快照最后包含的日志条目的 term（即 lastIncludedTerm）
	SnapshotIndex int    // 快照最后包含的日志条目的 index（即 lastIncludedIndex）
}

type Raft struct {
	mu        sync.Mutex                  // 保护该 peer 的共享状态（term、log、commitIndex、nextIndex…）
	peers     []raftrpc.RaftServiceClient // 与所有其他节点通信的 RPC 端点数组。peers[i] 代表“给 i 发 RPC”。
	persister *tools.Persister            // 该节点的持久化器，保存/读取 Raft 的持久化状态（term、votedFor、log、snapshot 等）
	me        int                         // 节点自己在 peers[] 里的编号
	dead      int32                       // 被 Kill() 标记后退出后台循环用的标志位

	role        int // 节点当前角色：Follower/Candidate/Leader
	CurrentTerm int // 节点当前任期号
	VotedFor    int // 节点在currentTerm任期投票给谁（-1表示未投）

	log *Log // 本地日志对象

	// 仅 leader 使用
	nextIndex  []int // leader 对每个 follower 的 下一条要发送的日志索引
	matchIndex []int // leader 认为的每个 follower 已经复制到的最大日志索引

	// 该节点保存的"大多数节点认同的系统中已提交的最高日志索引"
	// follower由leader的LeaderCommit更新，leader由matchIndex多数派更新
	commitIndex int

	lastApplied int           // 该节点已经应用到状态机的最高日志索引，永远满足 lastApplied <= commitIndex
	applyCh     chan ApplyMsg // Raft 把 ApplyMsg 通过Channel发送给上层服务
	snapPending bool          // 有一个快照需要优先安装/发送到 applyCh
	applyCond   *sync.Cond    // 用于唤醒 apply goroutine。

	//electionStart   time.Time     // 上一次“重置选举计时器”的时间点
	//electionTimeout time.Duration // 本节点的随机选举超时时间，每次进入 follower/candidate 或每轮选举通常会重置。
	electionTimer *time.Timer // 选举超时计时器，electionTicker() 循环里检测该计时器是否超时

	raftrpc.UnimplementedRaftServiceServer
}

var _ raftrpc.RaftServiceServer = (*Raft)(nil) // 编译时检查 Raft 是否实现了 raftrpc.RaftServiceServer 接口（RPC 服务端代码实现）

// ============================================================================
// Raft 对外（上层服务）接口
// ============================================================================

// Make 创建并初始化一个 Raft 节点，恢复持久化状态，并启动后台协程
func Make(peers []raftrpc.RaftServiceClient, me int, persister *tools.Persister, applyCh chan ApplyMsg) *Raft {
	// 初始化 Raft 节点
	rf := &Raft{
		mu:          sync.Mutex{},
		peers:       peers,
		persister:   persister,
		me:          me,
		role:        Follower,
		CurrentTerm: 1,
		VotedFor:    -1,
		log:         NewLog(0, 0, nil),
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
		commitIndex: 0,
		lastApplied: 0,
		applyCh:     applyCh,
	}

	// 从持久化的数据文件读取 raftstate、snapshot 配置，覆盖部分初始值
	rf.readPersist()

	// 初始化选举超时计时器
	rf.mu.Lock()
	rf.electionTimer = time.NewTimer(3000 * time.Millisecond)
	rf.resetTimer()
	rf.mu.Unlock()

	// todo: 启动后台协程循环：选举、日志复制、应用日志、快照
	// go rf.electionTicker()
	// go rf.applyTicker()

	return rf
}

// Start 上层服务请求 Raft 追加新日志（Start() 只负责将命令写入 Leader 本地日志并持久化，不代表集群日志"已提交"（日志复制到多数节点））
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 只有 Leader 能处理 Start 请求
	if rf.role != Leader {
		return InvalidIndex, InvalidTerm, false
	}

	// 在 Leader 本地日志追加新条目
	rf.log.tailLog = append(rf.log.tailLog, &raftrpc.LogEntry{
		Term:         int32(rf.CurrentTerm),
		CommandValid: true,
		Command:      Serialize(command),
	})
	rf.persist() // 修改了 Raft 论文要求的需持久化的字段 log，需持久化
	return rf.log.size(), rf.CurrentTerm, true
}

// GetState 让上层服务查询该节点当前任期，以及节点是否是Leader
func (rf *Raft) GetState() (term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.CurrentTerm, rf.role == Leader
}

// GetStateSize 返回已持久化的 RaftState 的大小，上层服务判断该值是否超过阈值，从而触发 Snapshot 压缩日志
func (rf *Raft) GetStateSize() int {
	return int(rf.persister.RaftStateSize())
}

// Kill 让上层服务停止 Raft 实例
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

// ============================================================================
// Raft 角色转换辅助方法
// ============================================================================

// Candidate 收到多数选票后赢得选举后转为 Leader，在 RequestVote 的处理/投票统计逻辑里调用。
func (rf *Raft) becomeLeader() {
	// 只有 Candidate 才能转为 Leader
	if rf.role != Candidate {
		return
	}
	rf.role = Leader

	// 根据 leader 本地日志长度，初始化 nextIndex[] 和 matchIndex[] 数组
	for peer := 0; peer < len(rf.peers); peer++ {
		rf.nextIndex[peer] = rf.log.size()
		rf.matchIndex[peer] = 0
	}
	LOG(rf.me, rf.CurrentTerm, DLeader, fmt.Sprintf("Peer_%d become Leader in Term_%d", rf.me, rf.CurrentTerm))
}

// 节点收到的 RPC（RequestVote / AppendEntries / InstallSnapshot）中带有更大的 term、Candidate 在选举中发现别人的 term 更大，将当前节点切为 Follower
func (rf *Raft) becomeFollower(term int) {
	// 只有接收到不小于自己 term 的消息，当前节点才能变成 Follower
	if term < rf.CurrentTerm {
		return
	}
	rf.role = Follower
	LOG(rf.me, rf.CurrentTerm, DLog, fmt.Sprintf("Peer_%d become Follower in Term_%d -> Term_%d", rf.me, rf.CurrentTerm, term))

	// 如果接收到大于自己 term 的消息，当前节点在上一轮 term 的投票失效，需重置投票状态
	if term > rf.CurrentTerm {
		rf.VotedFor = -1
		rf.CurrentTerm = term // term 更大时还需更新当前 term
		rf.persist()          // 修改了 Raft 论文要求的需持久化的字段 CurrentTerm、VotedFor
	}
}

// electionTicker() 检测到选举超时（长时间没收到 leader 心跳）
func (rf *Raft) becomeCandidate() {
	// Leader 不能变 Candidate
	if rf.role == Leader {
		return
	}
	rf.role = Candidate

	// 进行投票（进入新的term）
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.persist() // 修改了 Raft 论文要求的需持久化的字段 CurrentTerm、VotedFor
	LOG(rf.me, rf.CurrentTerm, DVote, fmt.Sprintf("Peer_%d become Candidate in Term_%d -> Term_%d", rf.me, rf.CurrentTerm-1, rf.CurrentTerm))
}

// ============================================================================
// 全局辅助方法
// ============================================================================

// 角色是 Leader 或 Candidate 这样的有特殊性质且存在相互竞争的角色，在锁外做了 耗时/异步工作（RPC、sleep、wait）的 goroutine，
// 回到锁内准备修改 Raft 的共享状态之前，都应先确定当前仍处于启动它时的那个（role，term） 上下文里
func (rf *Raft) contextCheck(role int, term int) bool {
	return rf.role == role && rf.CurrentTerm == term
}

// isKilled() 在后台协程循环里检查该节点是否被 Kill()，如果被 Kill() 就退出循环
func (rf *Raft) isKilled() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}
