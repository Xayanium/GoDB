package raft

import raftrpc "GoDB/rpc/raft"

type Log struct {
	// 快照覆盖到的最后一个日志索引
	// 由于索引 <= snapLastIdx 的日志内容已经被压缩进快照，不再需要保留之前的条目
	snapLastIdx int

	snapLastTerm int // snapLastIdx 日志对应的 term，用于 Leader/Follower 日志一致性检查

	snapshot []byte // 快照的具体内容

	tailLog []*raftrpc.LogEntry // 只存没有被快照的剩余日志（从下标1开始存），需要与真实全局日志进行下标转换
}

func NewLog(snapLastIdx, snapLastTerm int, snapshot []byte) *Log {
	rl := &Log{
		snapLastIdx:  snapLastIdx,
		snapLastTerm: snapLastTerm,
		snapshot:     snapshot,
	}
	rl.tailLog = make([]*raftrpc.LogEntry, 0)
	rl.tailLog = append(rl.tailLog, &raftrpc.LogEntry{Term: int32(snapLastTerm)}) // 占位，保持 tailLog 从下标1开始存
	return rl
}

// ============================================================================
// 一些辅助方法
// ============================================================================

// 返回全局日志（真实日志）的长度
func (rl *Log) size() int {
	return rl.snapLastIdx + len(rl.tailLog) - 1 // tailLog 从下标1开始存
}

// 将全局日志索引映射到 tailLog 的数组下标索引（tailLog 从下标1开始存）
func (rl *Log) idx(logicIdx int) int {
	if logicIdx < rl.snapLastIdx || logicIdx > rl.size() {
		panic("invalid logic log index")
	}
	return logicIdx - rl.snapLastIdx
}

// 返回全局日志索引对应的全局日志信息
func (rl *Log) get(logicIdx int) *raftrpc.LogEntry {
	return rl.tailLog[rl.idx(logicIdx)]
}

// 在 logicIdx（全局日志）位置后追加 entries 条目。用于 leader 处理 Start 时本地追加日志、follower 处理 AppendEntries 时追加（或覆盖）日志
func (rl *Log) append(logicIdx int, entries []*raftrpc.LogEntry) {
	rl.tailLog = append(rl.tailLog[:rl.idx(logicIdx)+1], entries...)
}

// 在 tailLog 中寻找第一个 term 匹配的条目，返回其在全局日志中的 index。用于 leader 回退 nextIndex 的优化
func (rl *Log) firstFor(term int) int {
	for index, entry := range rl.tailLog {
		if int(entry.Term) == term {
			return index
		} else if int(entry.Term) > term {
			break
		}
	}
	return InvalidIndex
}

// 返回 tailLog 中从 startIdx（全局日志index） 开始到末尾的所有日志条目。用于 leader 构造 AppendEntries
func (rl *Log) tail(startIdx int) []*raftrpc.LogEntry {
	if startIdx <= rl.snapLastIdx || startIdx > rl.size() {
		return nil
	}
	return rl.tailLog[rl.idx(startIdx):]
}

// 返回 tailLog 中最后一条日志 在全局日志中的 index 与 term，供 RequestVote 中使用、 leader 初始化 nextIndex
func (rl *Log) last() (int, int) {
	index := rl.size()
	if index == 0 {
		return InvalidIndex, InvalidTerm
	}
	return index, int(rl.get(index).Term)
}
