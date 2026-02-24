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
	if err := saveToDisk(ps.cfg.Persistence.RaftStateDir, raftstate); err != nil {
		return fmt.Errorf("save raft state failed: %w", err)
	}
	if err := saveToDisk(ps.cfg.Persistence.SnapshotDir, snapshot); err != nil {
		return fmt.Errorf("save snapshot failed: %w", err)
	}
	return nil
}

func (ps *Persister) ReadSnapshot() ([]byte, error) {
	return loadFromDisk(ps.cfg.Persistence.SnapshotDir)
}

func (ps *Persister) ReadRaftState() ([]byte, error) {
	return loadFromDisk(ps.cfg.Persistence.RaftStateDir)
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
