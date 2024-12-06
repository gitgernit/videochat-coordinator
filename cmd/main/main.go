package main

import (
	"context"
	"gitlab.crja72.ru/gospec/go5/coordinator/internal/config"
	"gitlab.crja72.ru/gospec/go5/rooms/pkg/logger"
	"os"
	"os/signal"
	"syscall"
)

var (
	serviceName = "coordinator"
)

func main() {
	ctx := context.Background()
	mainLogger := logger.New(serviceName)
	ctx = context.WithValue(ctx, logger.LoggerKey, mainLogger)

	_, err := config.New()
	if err != nil {
		mainLogger.Error(ctx, err.Error())
		return
	}

	graceCh := make(chan os.Signal, 1)
	signal.Notify(graceCh, syscall.SIGINT, syscall.SIGTERM)

	mainLogger.Info(ctx, "Successfully started")

	<-graceCh

	mainLogger.Info(ctx, "Successfully shut down")
}
