package config

// GlobalConfig 全局配置
type GlobalConfig struct {
	Server      ServerConfig      `yaml:"server"`
	Persistence PersistenceConfig `yaml:"persistence"`
	Logging     LogConfig         `yaml:"logging"`
	Raft        RaftConfig        `yaml:"raft"`
	Network     NetworkConfig     `yaml:"network"`
}

// ServerConfig 服务器配置
type ServerConfig struct {
	ID          int    `yaml:"id"`          // 服务器 ID
	Host        string `yaml:"host"`        // 监听地址
	Port        int    `yaml:"port"`        // 监听端口
	Environment string `yaml:"environment"` // 环境：dev/test/prod
}

// PersistenceConfig 持久化配置
type PersistenceConfig struct {
	Enabled          bool   `yaml:"enabled"`           // 是否启用持久化
	DataDir          string `yaml:"data_dir"`          // 数据根目录
	RaftStatePath    string `yaml:"raftstate_path"`    // Raft 状态文件路径
	SnapshotPath     string `yaml:"snapshot_path"`     // 快照文件路径
	SyncWrite        bool   `yaml:"sync_write"`        // 是否同步写入（fsync）
	UseChecksum      bool   `yaml:"use_checksum"`      // 是否使用校验和
	CompressSnapshot bool   `yaml:"compress_snapshot"` // 是否压缩快照
	MaxBackups       int    `yaml:"max_backups"`       // 最大备份数量
	BackupInterval   string `yaml:"backup_interval"`   // 备份间隔
}

// LogConfig 日志配置
type LogConfig struct {
	Level         string `yaml:"level"`           // 日志级别：debug/info/warn/error
	Format        string `yaml:"format"`          // 日志格式：json/text
	Output        string `yaml:"output"`          // 输出：stdout/file/both
	RaftLogPath   string `yaml:"raft_log_path"`   // 日志文件路径
	KVLogPath     string `yaml:"kv_log_path"`     // Shard KV 日志文件路径
	CtrlerLogPath string `yaml:"ctrler_log_path"` // Shard Ctrler 日志文件路径
	MaxSize       int    `yaml:"max_size"`        // 单个日志文件最大大小（MB）
	MaxBackups    int    `yaml:"max_backups"`     // 最大日志文件数量
	MaxAge        int    `yaml:"max_age"`         // 日志保留天数
	Compress      bool   `yaml:"compress"`        // 是否压缩旧日志
	EnableRaftLog bool   `yaml:"enable_raft_log"` // 是否启用 Raft 详细日志
}

// RaftConfig Raft 算法配置
type RaftConfig struct {
	ElectionTimeoutMin int `yaml:"election_timeout_min"` // 选举超时最小值（毫秒）
	ElectionTimeoutMax int `yaml:"election_timeout_max"` // 选举超时最大值（毫秒）
	HeartbeatInterval  int `yaml:"heartbeat_interval"`   // 心跳间隔（毫秒）
	ApplyInterval      int `yaml:"apply_interval"`       // 应用日志间隔（毫秒）
	SnapshotThreshold  int `yaml:"snapshot_threshold"`   // 快照阈值（日志条目数）
	MaxLogSize         int `yaml:"max_log_size"`         // 最大日志大小（字节）
}

// NetworkConfig 网络配置
type NetworkConfig struct {
	RPCTimeout     int    `yaml:"rpc_timeout"`     // RPC 超时时间（毫秒）
	MaxRetries     int    `yaml:"max_retries"`     // 最大重试次数
	EnableTLS      bool   `yaml:"enable_tls"`      // 是否启用 TLS
	CertFile       string `yaml:"cert_file"`       // 证书文件路径
	KeyFile        string `yaml:"key_file"`        // 密钥文件路径
	MaxConnections int    `yaml:"max_connections"` // 最大连接数
}
