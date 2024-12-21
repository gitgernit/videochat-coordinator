package main

import (
	"context"
	"fmt"
	"gitlab.crja72.ru/gospec/go5/coordinator/internal/domain/dispatchers"
	"os"
	"os/signal"
	"syscall"

	"gitlab.crja72.ru/gospec/go5/coordinator/internal/config"
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

	dispatchersInteractor, err := dispatchers.NewCoordinatorInteractor(mainLogger, cfg.GRPCServerHost, cfg.GRPCServerPort)
	if err != nil {
		mainLogger.Error(ctx, fmt.Sprintf("failed to initialise dispatchers CoordinatorInteractor: %v", err))
		return
	}

	graceCh := make(chan os.Signal, 1)
	signal.Notify(graceCh, syscall.SIGINT, syscall.SIGTERM)

	mainLogger.Info(ctx, "successfully started")

	go func() {
		if err := dispatchersInteractor.ListenForRooms(ctx); err != nil {
			mainLogger.Error(ctx, err.Error())
		}
	}()

	<-graceCh

	cancel()

	mainLogger.Info(ctx, "successfully shut down")
}
