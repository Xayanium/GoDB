package shardctrler

import (
	"testing"
)

func TestShardCtrler_duplicateTask(t *testing.T) {
	sc := &ShardCtrler{
		notifyChans: make(map[int]chan *OpReply),
	}

	sc.notifyChans[1] = make(chan *OpReply, 1)
	sc.notifyChans[1] <- &OpReply{} // Full now!

	notifyCh := sc.notifyChans[1]
	select {
	case notifyCh <- &OpReply{}:
		t.Fatal("Should not block or select this case")
	default:
		// Passed!
	}
}
