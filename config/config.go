package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
	"sync"
)

var (
	globalConfig *GlobalConfig
	mu           sync.RWMutex
)

func DefaultConfig() *GlobalConfig {
	return &GlobalConfig{
		Server: ServerConfig{
			ID:          0,
			Host:        "127.0.0.1",
			Port:        8080,
			Environment: "dev",
		},
		Persistence: PersistenceConfig{
			Enabled:          true,
			DataDir:          "./data",
			RaftStateDir:     "./data/raft",
			SnapshotDir:      "./data/snapshots",
			SyncWrite:        true,
			UseChecksum:      true,
			CompressSnapshot: false,
			MaxBackups:       3,
			BackupInterval:   "1h",
		},
		Logging: LoggingConfig{
			Level:         "info",
			Format:        "text",
			Output:        "both",
			FilePath:      "./data/logs/raft.log",
			MaxSize:       100,
			MaxBackups:    10,
			MaxAge:        30,
			Compress:      true,
			EnableRaftLog: false,
		},
		Raft: RaftConfig{
			ElectionTimeoutMin: 250,
			ElectionTimeoutMax: 400,
			HeartbeatInterval:  30,
			ApplyInterval:      10,
			SnapshotThreshold:  1000,
			MaxLogSize:         10 * 1024 * 1024, // 10MB
		},
		Network: NetworkConfig{
			RPCTimeout:     5000,
			MaxRetries:     3,
			EnableTLS:      false,
			MaxConnections: 100,
		},
	}
}

// Load 加载配置文件
func Load(configPath string) (*GlobalConfig, error) {
	cfg := DefaultConfig()
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return cfg, nil
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	if err := createDirectories(cfg); err != nil {
		return nil, fmt.Errorf("failed to create directories: %w", err)
	}

	return cfg, nil
}

// Get 获取全局配置
func Get() *GlobalConfig {
	mu.RLock()
	defer mu.RUnlock()
	if globalConfig == nil {
		globalConfig = DefaultConfig()
	}
	return globalConfig
}

func Reload(configPath string) error {
	mu.Lock()
	defer mu.Unlock()

	cfg, err := Load(configPath)
	if err != nil {
		return err
	}
	globalConfig = cfg
	return nil
}

// createDirectories 创建必要的目录
func createDirectories(cfg *GlobalConfig) error {
	dirs := []string{
		cfg.Persistence.DataDir,
		cfg.Persistence.RaftStateDir,
		cfg.Persistence.SnapshotDir,
		filepath.Dir(cfg.Logging.FilePath),
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	return nil
}
