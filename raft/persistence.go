package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

// ============================================================================
// Raft 论文中，必须持久化的状态只有：currentTerm、votedFor、log entries 以及快照相关的 snapLastIdx/snapLastTerm
// currentTerm：防止 term 回退，保证选举安全
// votedFor：防止一个 term 投两次票
// log[]：保证日志不丢失
// snapLastIdx：保证可正常访问日志条目、保证可正常判断日志在快照/tailLog中、保证不重复安装快照或重复应用日志
// snapLastTerm：
// snapshot：保证快照数据（日志）不丢失
// ============================================================================

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.log.SnapLastIdx)
	e.Encode(rf.log.SnapLastTerm)
	e.Encode(rf.log.tailLog)
	raftState := w.Bytes()

	if err := rf.persister.Save(raftState, rf.log.Snapshot); err != nil {
		LOG(rf.me, rf.CurrentTerm, DError, fmt.Sprintf("Persist failed: %v", err))
		return
	}
	LOG(rf.me, rf.CurrentTerm, DPersist, "Persist RaftState and Snapshot")
}

func (rf *Raft) readPersist() {
	raftState, err := rf.persister.ReadRaftState()
	if err != nil {
		LOG(rf.me, rf.CurrentTerm, DError, fmt.Sprintf("ReadPersist failed: %v", err))
		return
	}
	if len(raftState) == 0 {
		return
	}

	r := bytes.NewBuffer(raftState)
	d := gob.NewDecoder(r)
	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VotedFor)
	d.Decode(&rf.log.SnapLastIdx)
	d.Decode(&rf.log.SnapLastTerm)
	d.Decode(&rf.log.tailLog)

	rf.log.Snapshot, err = rf.persister.ReadSnapshot()
	if err != nil {
		LOG(rf.me, rf.CurrentTerm, DError, fmt.Sprintf("ReadSnapshot failed: %v", err))
		return
	}
	// 恢复持久化的快照后，检查 commitIndex、lastApplied 不小于 snapLastIdx（一直有 lastApplied <= commitIndex关系）
	if rf.commitIndex < rf.log.SnapLastIdx {
		rf.commitIndex = rf.log.SnapLastIdx // 系统多数节点已提交的日志肯定快于已快照的日志
		rf.lastApplied = rf.log.SnapLastIdx // 只有节点应用到状态机的日志才可进入快照
	}
	LOG(rf.me, rf.CurrentTerm, DPersist, "Read RaftState and Snapshot from disk")
}
