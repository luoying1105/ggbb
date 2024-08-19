package pkg

import (
	"log/slog"
	"os"
)

var Logger *slog.Logger

// 初始化默认的 Logger，默认级别为 Info
func init() {
	SetLoggerLevel(slog.LevelInfo)
}

// SetLoggerLevel 设置全局 Logger 的日志级别
func SetLoggerLevel(level slog.Level) {
	Logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	}))
	Logger.Debug("Logger initialized", slog.String("level", level.String()))
}
