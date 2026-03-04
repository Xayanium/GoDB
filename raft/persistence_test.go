package raft

import (
	"GoDB/config"
	"GoDB/rpc/raft"
	"GoDB/tools"
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func init() {
	tools.InitLogger(config.Get().Logging)
}

func setupPersister(t *testing.T) *tools.Persister {
	t.Helper()
	tmp := t.TempDir()
	cfg := config.Get()
	cfg.Persistence.RaftStatePath = filepath.Join(tmp, "raft_state.bin")
	cfg.Persistence.SnapshotPath = filepath.Join(tmp, "snapshot.bin")
	return tools.MakePersister()
}

func makeRaftForPersistTest(ps *tools.Persister) *Raft {
	rf := &Raft{}
	rf.persister = ps
	rf.me = 1
	rf.CurrentTerm = 5
	rf.VotedFor = 2
	rf.log = NewLog(10, 4, []byte("snap-v1"))
	// Add a couple of tail log entries after the snapshot.
	rf.log.append(10, []*raftrpc.LogEntry{
		{Term: 5, Command: []byte("cmd-1")},
		{Term: 5, Command: []byte("cmd-2")},
	})
	return rf
}

func TestPersistAndReadPersist_RoundTrip(t *testing.T) {
	ps := setupPersister(t)

	rf := makeRaftForPersistTest(ps)
	rf.persist()

	// New raft instance reading from the same persister.
	rf2 := &Raft{persister: ps, log: NewLog(0, 0, nil)}
	rf2.commitIndex = 0
	rf2.lastApplied = 0
	rf2.readPersist()

	if rf2.CurrentTerm != rf.CurrentTerm {
		t.Fatalf("CurrentTerm mismatch: got %d want %d", rf2.CurrentTerm, rf.CurrentTerm)
	}
	if rf2.VotedFor != rf.VotedFor {
		t.Fatalf("VotedFor mismatch: got %d want %d", rf2.VotedFor, rf.VotedFor)
	}
	if rf2.log.SnapLastIdx != rf.log.SnapLastIdx {
		t.Fatalf("SnapLastIdx mismatch: got %d want %d", rf2.log.SnapLastIdx, rf.log.SnapLastIdx)
	}
	if rf2.log.SnapLastTerm != rf.log.SnapLastTerm {
		t.Fatalf("SnapLastTerm mismatch: got %d want %d", rf2.log.SnapLastTerm, rf.log.SnapLastTerm)
	}
	if !bytes.Equal(rf2.log.Snapshot, rf.log.Snapshot) {
		t.Fatalf("Snapshot mismatch: got %q want %q", string(rf2.log.Snapshot), string(rf.log.Snapshot))
	}
	if !reflect.DeepEqual(rf2.log.tailLog, rf.log.tailLog) {
		t.Fatalf("tailLog mismatch:\n got  %#v\n want %#v", rf2.log.tailLog, rf.log.tailLog)
	}
	// After restoring a snapshot, commitIndex/lastApplied should be at least SnapLastIdx.
	if rf2.commitIndex != rf.log.SnapLastIdx {
		t.Fatalf("commitIndex not updated: got %d want %d", rf2.commitIndex, rf.log.SnapLastIdx)
	}
	if rf2.lastApplied != rf.log.SnapLastIdx {
		t.Fatalf("lastApplied not updated: got %d want %d", rf2.lastApplied, rf.log.SnapLastIdx)
	}
}

func TestReadPersist_EmptyStateIsNoop(t *testing.T) {
	ps := setupPersister(t)
	// Ensure there is no persisted state on disk.
	cfg := config.Get()
	_ = os.Remove(cfg.Persistence.RaftStatePath)
	_ = os.Remove(cfg.Persistence.SnapshotPath)

	rf := &Raft{persister: ps, log: NewLog(0, 0, nil)}
	rf.CurrentTerm = 99
	rf.VotedFor = 88
	rf.commitIndex = 7
	rf.lastApplied = 6
	rf.readPersist()

	if rf.CurrentTerm != 99 || rf.VotedFor != 88 {
		t.Fatalf("readPersist should not change term/vote when no state exists")
	}
	if rf.commitIndex != 7 || rf.lastApplied != 6 {
		t.Fatalf("readPersist should not change indexes when no state exists")
	}
}

func TestPersist_OverwritesPreviousState(t *testing.T) {
	ps := setupPersister(t)

	rf := makeRaftForPersistTest(ps)
	rf.persist()

	// Modify state and persist again.
	rf.CurrentTerm = 6
	rf.VotedFor = 3
	rf.log.Snapshot = []byte("snap-v2")
	rf.log.append(rf.log.size(), []*raftrpc.LogEntry{{Term: 6, Command: []byte("cmd-3")}})
	rf.persist()

	rf2 := &Raft{persister: ps, log: NewLog(0, 0, nil)}
	rf2.readPersist()

	if rf2.CurrentTerm != 6 {
		t.Fatalf("CurrentTerm mismatch after overwrite: got %d want %d", rf2.CurrentTerm, 6)
	}
	if rf2.VotedFor != 3 {
		t.Fatalf("VotedFor mismatch after overwrite: got %d want %d", rf2.VotedFor, 3)
	}
	if !bytes.Equal(rf2.log.Snapshot, []byte("snap-v2")) {
		t.Fatalf("Snapshot mismatch after overwrite: got %q want %q", string(rf2.log.Snapshot), "snap-v2")
	}
	if rf2.log.size() != rf.log.size() {
		t.Fatalf("log size mismatch after overwrite: got %d want %d", rf2.log.size(), rf.log.size())
	}
}
