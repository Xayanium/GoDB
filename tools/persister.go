package tools

import (
	"GoDB/config"
	"fmt"
	"os"
)

type Persister struct {
	cfg *config.GlobalConfig
}

func MakePersister() *Persister {
	cfg := config.Get()
	return &Persister{
		cfg: cfg,
	}
}

func (ps *Persister) Save(raftstate []byte, snapshot []byte) error {
	if err := saveToDisk(ps.cfg.Persistence.RaftStatePath, raftstate); err != nil {
		return fmt.Errorf("save raft state failed: %w", err)
	}
	if err := saveToDisk(ps.cfg.Persistence.SnapshotPath, snapshot); err != nil {
		return fmt.Errorf("save snapshot failed: %w", err)
	}
	return nil
}

func (ps *Persister) ReadSnapshot() ([]byte, error) {
	return loadFromDisk(ps.cfg.Persistence.SnapshotPath)
}

func (ps *Persister) ReadRaftState() ([]byte, error) {
	return loadFromDisk(ps.cfg.Persistence.RaftStatePath)
}

func (ps *Persister) RaftStateSize() int64 {
	return getFileSize(ps.cfg.Persistence.RaftStatePath)
}

func (ps *Persister) SnapshotSize() int64 {
	return getFileSize(ps.cfg.Persistence.SnapshotPath)
}

func saveToDisk(filepath string, data []byte) error {
	if err := os.WriteFile(filepath, data, 0644); err != nil {
		return err
	}
	return nil
}

func loadFromDisk(filepath string) ([]byte, error) {
	if _, err := os.Stat(filepath); os.IsNotExist(err) {
		return nil, nil
	}
	return os.ReadFile(filepath)
}

func getFileSize(filepath string) int64 {
	info, err := os.Stat(filepath)
	if err != nil {
		return 0
	}
	return info.Size()
}
