package config

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"gopkg.in/yaml.v3"
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
			RaftStatePath:    "./data/raft",
			SnapshotPath:     "./data/snapshots",
			SyncWrite:        true,
			UseChecksum:      true,
			CompressSnapshot: false,
			MaxBackups:       3,
			BackupInterval:   "1h",
		},
		Logging: LogConfig{
			Level:         "info",
			Format:        "text",
			Output:        "both",
			RaftLogPath:   "./data/logs/raft.log",
			KVLogPath:     "./data/logs/kv.log",
			CtrlerLogPath: "./data/logs/ctrler.log",
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
		ShardCtrler: ShardCtrlerConfig{
			NShards:          10,
			ClientReqTimeout: 500,
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
func Load(configPath string) *GlobalConfig {
	cfg := DefaultConfig()
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return cfg
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		panic(fmt.Errorf("failed to read config file: %w", err))
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		panic(fmt.Errorf("failed to parse config file: %w", err))
	}

	if err := createDirectories(cfg); err != nil {
		panic(err)
	}

	return cfg
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

func Reload(configPath string) {
	mu.Lock()
	defer mu.Unlock()

	cfg := Load(configPath)

	globalConfig = cfg
}

// createDirectories 创建必要的目录
func createDirectories(cfg *GlobalConfig) error {
	dirs := []string{
		cfg.Persistence.DataDir,
		filepath.Dir(cfg.Logging.RaftLogPath),
		filepath.Dir(cfg.Logging.KVLogPath),
		filepath.Dir(cfg.Logging.CtrlerLogPath),
	}

	for _, dir := range dirs {
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			if err = os.MkdirAll(dir, 0755); err != nil {
				return fmt.Errorf("failed to create directory %s: %w", dir, err)
			}
		}
	}

	return nil
}
