package raft

import (
	raftrpc "GoDB/rpc/raft"
	"fmt"
)

// 上层服务通过 rf.Start() 提交日志给 Raft 集群（Leader节点）进行日志一致性（日志根据Raft算法，成功复制到集群中大多数节点）
// 集群提交日志后（日志复制到大多数节点），Raft 集群通过 applyCh 通道通知上层服务可应用相关日志到状态机
func (rf *Raft) applicationTicker() {
	for !rf.isKilled() {
		rf.mu.Lock()
		rf.applyCond.Wait()

		// 锁内拷贝需要传入 applyCh 的日志信息，防止在锁外操作时出现并发问题
		entries := make([]*raftrpc.LogEntry, 0)
		lastApplied := rf.lastApplied + 1
		snapshot := append([]byte{}, rf.log.Snapshot...)
		snapshotTerm := rf.log.SnapLastTerm
		snapshotIdx := rf.log.SnapLastIdx

		// 将本地日志复制一份，防止在锁外操作时出现并发问题
		if !rf.isSnapshots {
			// (lastApplied, commitIndex] 范围中不能有日志被截断进 snapshot
			if rf.lastApplied <= rf.log.SnapLastIdx {
				rf.lastApplied = rf.log.SnapLastIdx
			}

			start := rf.lastApplied + 1
			end := rf.commitIndex
			// commitIndex 不能大于本地日志的最后一个日志索引号
			if end > rf.log.size() {
				end = rf.log.size()
			}
			for i := start; i <= end; i++ {
				entries = append(entries, rf.log.get(i))
			}
		}

		// 在锁外将 ApplyMsg 传入 applyCh，防止阻塞
		rf.mu.Unlock()
		if !rf.isSnapshots {
			// 如果上层服务 Start() 的信息是日志信息，将Leader本地 (lastApplied, commitIndex] 范围的日志传入 applyCh，供上层服务应用到状态机
			for i, entry := range entries {
				rf.applyCh <- ApplyMsg{
					CommandValid: entry.CommandValid,
					Command:      entry.Command,
					CommandIndex: lastApplied + i + 1,
				}
			}
		} else {
			// 如果上层服务 Start() 的信息是快照信息，将快照信息传入 applyCh，供上层服务安装快照
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      snapshot,
				SnapshotTerm:  snapshotTerm,
				SnapshotIndex: snapshotIdx,
			}
		}

		// 成功将消息传入 applyCh 后，锁内更新相关字段
		rf.mu.Lock()
		if !rf.isSnapshots {
			LOG(rf.me, rf.CurrentTerm, DApply, fmt.Sprintf("Peer_%d apply log [%d, %d]", rf.me, rf.lastApplied+1, rf.commitIndex))
			rf.lastApplied += len(entries) // 更新 lastApplied 可能小于 commitIndex，因其可能大于本地日志的最后一个日志索引号
		} else {
			LOG(rf.me, rf.CurrentTerm, DApply, fmt.Sprintf("Peer_%d apply snapshot [0, %d]", rf.me, rf.log.SnapLastIdx))
			// 安装快照意味着状态机已经包含到 snapLastIdx，所以 lastApplied 必须直接跳到 snapLastIdx，同时把 commitIndex 至少抬到 snapLastIdx
			rf.lastApplied = rf.log.SnapLastIdx
			rf.commitIndex = max(rf.commitIndex, rf.log.SnapLastIdx)
			rf.isSnapshots = false
		}
		rf.mu.Unlock()
	}
}
