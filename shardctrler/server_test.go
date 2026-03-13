package shardctrler

import (
	"testing"
)

func TestShardCtrler_duplicateTask(t *testing.T) {
	sc := &ShardCtrler{
		notifyChs: make(map[int]chan *OpReply),
	}

	sc.notifyChs[1] = make(chan *OpReply, 1)
	sc.notifyChs[1] <- &OpReply{} // Full now!

	notifyCh := sc.notifyChs[1]
	select {
	case notifyCh <- &OpReply{}:
		t.Fatal("Should not block or select this case")
	default:
		// Passed!
	}
}
