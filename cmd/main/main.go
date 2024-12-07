package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"gitlab.crja72.ru/gospec/go5/coordinator/internal/config"
	"gitlab.crja72.ru/gospec/go5/coordinator/internal/domain/pingpong"
	"gitlab.crja72.ru/gospec/go5/rooms/pkg/logger"
	"go.uber.org/zap"
)

var (
	serviceName = "coordinator"
)

func main() {
	ctx := context.Background()
	mainLogger := logger.New(zap.DebugLevel, serviceName)
	ctx = context.WithValue(ctx, logger.LoggerKey, mainLogger)

	cfg, err := config.New()
	if err != nil {
		mainLogger.Error(ctx, err.Error())
		return
	}

	pingInteractor, err := pingpong.NewPingInteractor(ctx, cfg.GRPCServerHost, cfg.GRPCServerPort, mainLogger)
	if err != nil {
		mainLogger.Error(ctx, fmt.Sprintf("Failed to initialise PingInteractor: %v", err))
		return
	}

	graceCh := make(chan os.Signal, 1)
	signal.Notify(graceCh, syscall.SIGINT, syscall.SIGTERM)

	mainLogger.Info(ctx, "Successfully started")

	go func() {
		if err := pingInteractor.Start(ctx); err != nil {
			mainLogger.Error(ctx, err.Error())
		}
	}()

	<-graceCh

	if err := pingInteractor.Stop(); err != nil {
		mainLogger.Error(ctx, err.Error())
	}

	mainLogger.Info(ctx, "Successfully shut down")
}
