package pingpong

import (
	"context"
	"fmt"
	"gitlab.crja72.ru/gospec/go5/contracts/proto/rooms/go/proto"
	"gitlab.crja72.ru/gospec/go5/rooms/pkg/logger"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"time"
)

type PingInteractor struct {
	logger logger.Logger
}

func NewPingInteractor(logger logger.Logger) (*PingInteractor, error) {
	return &PingInteractor{
		logger: logger,
	}, nil
}

func (p *PingInteractor) Start(ctx context.Context, grpcHost string, grpcPort int) error {
	ticker := time.NewTicker(time.Second * 1)
	defer ticker.Stop()

	var counter uint32 = 0

	conn, err := grpc.NewClient(
		fmt.Sprintf("%v:%v", grpcHost, grpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {
			p.logger.Error(ctx, "couldn't close connection", zap.Error(err))
		}
	}(conn)
	if err != nil {
		return fmt.Errorf("failed to connect to gRPC server: %v", err)
	}

	client := proto.NewRoomsServiceClient(conn)
	stream, err := client.PingPong(ctx)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ticker.C:
			ping := Ping{Counter: counter}
			req := &proto.Ping{Counter: ping.Counter}
			if err := stream.Send(req); err != nil {
				return fmt.Errorf("failed to send a ping request: %v", err)
			}
			p.logger.Debug(ctx, "sent a ping request", zap.Uint32("counter", req.Counter))

			pong, err := stream.Recv()
			if err != nil {
				return err
			}
			p.logger.Debug(ctx, "received a pong response", zap.Uint32("counter", pong.Counter))

			counter = pong.Counter

		case <-ctx.Done():
			err := ctx.Err()
			if err != nil {
				return err
			}
		}
	}
}
