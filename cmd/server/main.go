package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alanweb7/openclaw-2026-api-go/internal/app"
	"github.com/alanweb7/openclaw-2026-api-go/internal/config"
	"github.com/alanweb7/openclaw-2026-api-go/internal/httpapi"
	"github.com/alanweb7/openclaw-2026-api-go/internal/logger"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	logg := logger.New(cfg.LogLevel)
	application := app.New(context.Background(), cfg, logg)
	apiServer := httpapi.New(cfg, application, logg)

	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logg.Info("starting openclaw bridge", "config", cfg.String())

	ctx, cancel := context.WithCancel(rootCtx)
	defer cancel()

	errCh := make(chan error, 2)

	go func() {
		if err := application.Run(ctx); err != nil {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	go func() {
		if err := apiServer.ListenAndServe(ctx); err != nil {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case <-rootCtx.Done():
		cancel()
		time.Sleep(200 * time.Millisecond)
	case err := <-errCh:
		if err != nil {
			logg.Error("bridge stopped with error", "error", err.Error())
			cancel()
			os.Exit(1)
		}
	}

	logg.Info("bridge stopped")
}
