package tools

import (
	"GoDB/config"
	"os"
	"path/filepath"
	"testing"
)

// TestMakePersister 测试 Persister 的初始化
func TestMakePersister(t *testing.T) {
	// 为了测试，我们需要临时修改全局配置
	// 注意：如果项目中有 config.Init() 或 SetConfig 方法，应优先使用
	// 这里假设 config.Get() 返回的是全局单例，我们直接修改其字段（仅测试用）
	cfg := config.Get()
	originalCfg := *cfg // 备份原始配置

	// 测试结束后恢复配置
	defer func() {
		*cfg = originalCfg
	}()

	p := MakePersister()
	if p == nil {
		t.Fatal("MakePersister() returned nil")
	}
	if p.cfg != cfg {
		t.Error("Persister config not initialized correctly")
	}
}

// TestPersister_SaveAndRead 测试保存和读取功能
func TestPersister_SaveAndRead(t *testing.T) {
	// 1. 准备环境：创建临时目录
	tempDir, err := os.MkdirTemp("", "persister_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir) // 测试结束后清理

	// 2. 准备配置
	cfg := config.Get()
	originalCfg := *cfg // 备份
	defer func() { *cfg = originalCfg }()

	// 设置测试用的路径
	raftStateFile := filepath.Join(tempDir, "raft", "state.bin")
	snapshotFile := filepath.Join(tempDir, "snapshots", "snap.bin")

	cfg.Persistence.RaftStateDir = raftStateFile
	cfg.Persistence.SnapshotDir = snapshotFile

	// 确保目录存在 (实际代码中可能需要 persister 或上层逻辑创建，测试中先行创建以防报错)
	if err := os.MkdirAll(filepath.Dir(raftStateFile), 0755); err != nil {
		t.Fatalf("Failed to create raft dir: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(snapshotFile), 0755); err != nil {
		t.Fatalf("Failed to create snapshot dir: %v", err)
	}

	p := MakePersister()

	// 3. 测试初始读取 (文件不存在的情况)
	t.Run("ReadNonExistent", func(t *testing.T) {
		data, err := p.ReadRaftState()
		if err != nil {
			t.Errorf("Expected no error for missing file, got: %v", err)
		}
		if data != nil {
			t.Errorf("Expected nil data for missing file, got len %d", len(data))
		}

		snap, err := p.ReadSnapshot()
		if err != nil {
			t.Errorf("Expected no error for missing snapshot, got: %v", err)
		}
		if snap != nil {
			t.Errorf("Expected nil snapshot for missing file, got len %d", len(snap))
		}
	})

	// 4. 测试保存功能
	t.Run("SaveData", func(t *testing.T) {
		raftData := []byte("raft-log-data-123")
		snapData := []byte("snapshot-data-456")

		err := p.Save(raftData, snapData)
		if err != nil {
			t.Fatalf("Save() failed: %v", err)
		}

		// 验证文件是否真的被写入磁盘
		if _, err := os.Stat(raftStateFile); os.IsNotExist(err) {
			t.Error("Raft state file was not created")
		}
		if _, err := os.Stat(snapshotFile); os.IsNotExist(err) {
			t.Error("Snapshot file was not created")
		}
	})

	// 5. 测试读取已保存的数据
	t.Run("ReadSavedData", func(t *testing.T) {
		raftData, err := p.ReadRaftState()
		if err != nil {
			t.Fatalf("ReadRaftState() failed: %v", err)
		}
		if string(raftData) != "raft-log-data-123" {
			t.Errorf("Raft data mismatch: got %s", string(raftData))
		}

		snapData, err := p.ReadSnapshot()
		if err != nil {
			t.Fatalf("ReadSnapshot() failed: %v", err)
		}
		if string(snapData) != "snapshot-data-456" {
			t.Errorf("Snapshot data mismatch: got %s", string(snapData))
		}
	})

	// 6. 测试覆盖写入
	t.Run("OverwriteData", func(t *testing.T) {
		newRaftData := []byte("new-raft-data")
		newSnapData := []byte("new-snap-data")

		err := p.Save(newRaftData, newSnapData)
		if err != nil {
			t.Fatalf("Save() overwrite failed: %v", err)
		}

		raftData, _ := p.ReadRaftState()
		if string(raftData) != "new-raft-data" {
			t.Errorf("Overwrite failed for raft data: got %s", string(raftData))
		}
	})
}

// TestSaveToDisk_Error 测试写入错误情况（例如路径不存在）
func TestSaveToDisk_Error(t *testing.T) {
	// 使用一个不存在的深层目录路径，且不创建父目录，预期写入失败
	invalidPath := "/non/existent/path/to/file"
	err := saveToDisk(invalidPath, []byte("test"))
	if err == nil {
		t.Error("Expected error when saving to invalid path, got nil")
	}
}
