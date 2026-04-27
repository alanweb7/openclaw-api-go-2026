package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/alanweb7/openclaw-2026-api-go/internal/app"
	"github.com/alanweb7/openclaw-2026-api-go/internal/config"
	"github.com/alanweb7/openclaw-2026-api-go/internal/logger"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	logg := logger.New(cfg.LogLevel)
	application := app.New(cfg, logg)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logg.Info("starting openclaw bridge", "config", cfg.String())
	if err := application.Run(ctx); err != nil {
		logg.Error("bridge stopped with error", "error", err.Error())
		os.Exit(1)
	}
	logg.Info("bridge stopped")
}
