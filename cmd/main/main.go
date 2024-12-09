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
	ctx, cancel := context.WithCancel(context.WithValue(ctx, logger.LoggerKey, mainLogger))

	cfg, err := config.New()
	if err != nil {
		mainLogger.Error(ctx, err.Error())
		return
	}

	pingInteractor, err := pingpong.NewPingInteractor(mainLogger)
	if err != nil {
		mainLogger.Error(ctx, fmt.Sprintf("failed to initialise PingInteractor: %v", err))
		return
	}

	graceCh := make(chan os.Signal, 1)
	signal.Notify(graceCh, syscall.SIGINT, syscall.SIGTERM)

	mainLogger.Info(ctx, "successfully started")

	go func() {
		if err := pingInteractor.Start(ctx, cfg.GRPCServerHost, cfg.GRPCServerPort); err != nil {
			mainLogger.Error(ctx, err.Error())
		}
	}()

	<-graceCh

	cancel()

	mainLogger.Info(ctx, "successfully shut down")
}
