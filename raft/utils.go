package raft

import (
	"GoDB/tools"
	"fmt"
)

type logTopic string

const (
	DError logTopic = "ERRO" // level = 3
	DWarn  logTopic = "WARN" // level = 2
	DInfo  logTopic = "INFO" // level = 1
	DDebug logTopic = "DBUG" // level = 0

	// level = 1
	DClient  logTopic = "CLNT"
	DCommit  logTopic = "CMIT"
	DDrop    logTopic = "DROP"
	DLeader  logTopic = "LEAD"
	DLog     logTopic = "LOG1" // sending log
	DLog2    logTopic = "LOG2" // receiving log
	DPersist logTopic = "PERS"
	DSnap    logTopic = "SNAP"
	DTerm    logTopic = "TERM"
	DTest    logTopic = "TEST"
	DTimer   logTopic = "TIMR"
	DTrace   logTopic = "TRCE"
	DVote    logTopic = "VOTE"
	DApply   logTopic = "APLY"
)

func LOG(peerId int, term int, topic logTopic, msg string) {
	msg = fmt.Sprintf("[%s]: %s", topic, msg)
	switch topic {
	case DError:
		tools.RaftLogger.Error(msg, "peer", peerId, "term", term)
	case DWarn:
		tools.RaftLogger.Warn(msg, "peer", peerId, "term", term)
	case DDebug:
		tools.RaftLogger.Debug(msg, "peer", peerId, "term", term)
	default:
		tools.RaftLogger.Info(msg, "peer", peerId, "term", term)
	}
}
