package tools

import (
	"GoDB/config"
	"log/slog"
	"os"
	"strings"
	"time"
)

var (
	RaftLogger   *slog.Logger
	KVLogger     *slog.Logger
	CtrlerLogger *slog.Logger
	logStart     time.Time
)

func InitLogger(config config.LogConfig) {
	logStart = time.Now()
	RaftLogger = createLogger(config.RaftLogPath, config.Level)
	KVLogger = createLogger(config.KVLogPath, config.Level)
	CtrlerLogger = createLogger(config.CtrlerLogPath, config.Level)
}

func createLogger(path string, level string) *slog.Logger {
	// 创建用于日志写入writer
	writer, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		writer = os.Stdout
	}
	// 创建JSON格式的日志处理器
	handler := slog.NewJSONHandler(writer, &slog.HandlerOptions{Level: parseLevel(level)})
	return slog.New(handler)
}

func parseLevel(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
