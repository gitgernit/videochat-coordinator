package pingpong

import (
	"context"
	"fmt"
	"time"

	pb "gitlab.crja72.ru/gospec/go5/contracts/proto/rooms/go/proto"
	"gitlab.crja72.ru/gospec/go5/rooms/pkg/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type PingInteractor struct {
	ticker *time.Ticker
	stream pb.RoomsService_PingPongClient
	client pb.RoomsServiceClient
	conn   *grpc.ClientConn
	logger logger.Logger
}

func NewPingInteractor(ctx context.Context, grpcHost string, grpcPort int, logger logger.Logger) (*PingInteractor, error) {
	conn, err := grpc.NewClient(fmt.Sprintf("%v:%v", grpcHost, grpcPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to gRPC server: %v", err)
	}

	client := pb.NewRoomsServiceClient(conn)
	stream, err := client.PingPong(ctx)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open the stream: %v", err)
	}

	return &PingInteractor{
		ticker: time.NewTicker(time.Second),
		client: client,
		conn:   conn,
		stream: stream,
		logger: logger,
	}, nil
}

func (p *PingInteractor) Start(ctx context.Context) error {
	defer p.conn.Close()
	counter := uint32(0)

	for {
		select {
		case <-p.ticker.C:
			counter++
			req := &pb.Ping{Counter: counter}
			if err := p.stream.Send(req); err != nil {
				return fmt.Errorf("failed to send a ping request: %v", err)
			}
			p.logger.Info(ctx, fmt.Sprintf("Sent a ping request: %v", req))
		}
	}
}

func (p *PingInteractor) Stop() error {
	p.ticker.Stop()
	if err := p.stream.CloseSend(); err != nil {
		return fmt.Errorf("Error when closing a stream: %v", err)
	}
	return nil
}
