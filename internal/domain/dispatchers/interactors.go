package dispatchers

import (
	"context"
	"fmt"
	"gitlab.crja72.ru/gospec/go5/contracts/proto/rooms/go/proto"
	"gitlab.crja72.ru/gospec/go5/rooms/pkg/logger"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"io"
)

const (
	dispatcherUsername = "dispatcher"
)

type CoordinatorInteractor struct {
	logger   logger.Logger
	GrpcHost string
	GrpcPort int
}

func NewCoordinatorInteractor(logger logger.Logger, grpcHost string, grpcPort int) (*CoordinatorInteractor, error) {
	return &CoordinatorInteractor{
		logger:   logger,
		GrpcHost: grpcHost,
		GrpcPort: grpcPort,
	}, nil
}

func (i CoordinatorInteractor) ListenForRooms(ctx context.Context) error {
	conn, err := grpc.NewClient(
		fmt.Sprintf("%v:%v", i.GrpcHost, i.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {
			i.logger.Error(ctx, "couldn't close connection", zap.Error(err))
		}
	}(conn)
	if err != nil {
		return fmt.Errorf("failed to connect to gRPC server: %v", err)
	}

	client := proto.NewRoomsServiceClient(conn)
	req := &proto.ListenForRoomsRequest{}
	stream, err := client.ListenForRooms(ctx, req)
	if err != nil {
		return err
	}

	for {
		room, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		i.logger.Debug(ctx, "received new room", zap.String("room_id", room.Id))

		err = i.SpawnDispatcher(ctx, room.Id)
		if err != nil {
			return status.Error(codes.Internal, "couldnt spawn a dispatcher")
		}
	}
}

func (i CoordinatorInteractor) SpawnDispatcher(ctx context.Context, roomID string) error {
	dispatcherInteractor, err := NewDispatcherInteractor(i.logger, roomID, i.GrpcHost, i.GrpcPort)
	if err != nil {
		return err
	}

	go func() {
		err := dispatcherInteractor.Listen(ctx)
		if err != nil {
			i.logger.Error(ctx, "dispatcher returned an error", zap.String("room_id", roomID), zap.Error(err))
		}
	}()

	return nil
}

type DispatcherInteractor struct {
	logger   logger.Logger
	RoomID   string
	GrpcHost string
	GrpcPort int
}

func NewDispatcherInteractor(logger logger.Logger, roomID string, grpcHost string, grpcPort int) (*DispatcherInteractor, error) {
	return &DispatcherInteractor{
		logger:   logger,
		RoomID:   roomID,
		GrpcHost: grpcHost,
		GrpcPort: grpcPort,
	}, nil
}

func (i DispatcherInteractor) Listen(ctx context.Context) error {
	conn, err := grpc.NewClient(
		fmt.Sprintf("%v:%v", i.GrpcHost, i.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {
			i.logger.Error(ctx, "couldn't close connection", zap.Error(err))
		}
	}(conn)
	if err != nil {
		return fmt.Errorf("failed to connect to gRPC server: %v", err)
	}

	client := proto.NewRoomsServiceClient(conn)

	md := metadata.Pairs(
		"username", dispatcherUsername,
		"room_id", i.RoomID,
	)
	ctx = metadata.NewOutgoingContext(ctx, md)

	stream, err := client.JoinRoom(ctx)
	if err != nil {
		return err
	}

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		method := msg.Method
		switch m := method.(type) {
		case *proto.RoomMethod_MessageReceived:
			update := m.MessageReceived
			i.logger.Debug(ctx, "message received", zap.String("text", update.Text), zap.String("username", update.Username), zap.String("room_id", i.RoomID))
		}
	}
}
