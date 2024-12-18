package dispatchers

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/pion/webrtc/v4"
	"gitlab.crja72.ru/gospec/go5/contracts/proto/rooms/go/proto"
	"gitlab.crja72.ru/gospec/go5/rooms/pkg/logger"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"io"
	"slices"
	"time"
)

const (
	dispatcherUsername = "dispatcher"
)

var (
	config = webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			{
				URLs: []string{"stun:stun.l.google.com:19302"},
			},
		},
	}
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
			return err
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
	logger     logger.Logger
	UsersPeers map[User]*webrtc.PeerConnection
	RoomID     string
	GrpcHost   string
	GrpcPort   int
}

func NewDispatcherInteractor(logger logger.Logger, roomID string, grpcHost string, grpcPort int) (*DispatcherInteractor, error) {
	return &DispatcherInteractor{
		logger:     logger,
		UsersPeers: make(map[User]*webrtc.PeerConnection),
		RoomID:     roomID,
		GrpcHost:   grpcHost,
		GrpcPort:   grpcPort,
	}, nil
}

func (i *DispatcherInteractor) Listen(ctx context.Context) error {
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
		case *proto.RoomMethod_RoomUsers_:
			update := m.RoomUsers_
			i.logger.Debug(ctx, "room users received", zap.Any("room_users", update.Users))

			roomUsers := make([]User, 0, len(update.Users)-1)
			sdps := make([]*proto.SDP, 0, len(roomUsers))

			for _, protoUser := range update.Users {
				if protoUser.Username == dispatcherUsername {
					continue
				}

				userID, err := uuid.Parse(protoUser.Id)
				if err != nil {
					return err
				}

				user := User{Id: userID, Name: protoUser.Username}
				roomUsers = append(roomUsers, user)
			}

			for user, pc := range i.UsersPeers {
				if user.Name == dispatcherUsername {
					continue
				}

				if !slices.Contains(roomUsers, user) {
					delete(i.UsersPeers, user)
					err := pc.Close()
					if err != nil {
						i.logger.Error(ctx, "couldnt close inactive peer connection")
					}
				}
			}

			for _, user := range roomUsers {
				if user.Name == dispatcherUsername {
					continue
				}

				pc, ok := i.UsersPeers[user]
				if !ok {
					pc, err = i.initializePeerConnection()
					if err != nil {
						return err
					}

					if err := i.createDatachannel(pc); err != nil {
						return err
					}

					i.UsersPeers[user] = pc
				}

				if err := i.createOffer(pc); err != nil {
					return err
				}

				sdp := pc.LocalDescription()
				sdps = append(sdps, &proto.SDP{
					Type:     sdp.Type.String(),
					Sdp:      sdp.SDP,
					Username: user.Name,
				})
			}

			method := &proto.RoomMethod{
				Method: &proto.RoomMethod_SendSdp{
					SendSdp: &proto.SendSDP{
						Sdp: sdps,
					},
				},
			}

			err := stream.Send(method)
			if err != nil {
				return err
			}
		case *proto.RoomMethod_SdpReceived:
			update := m.SdpReceived
			var user User

			for peerUser, _ := range i.UsersPeers {
				if peerUser.Name == update.From {
					user = peerUser
					break
				}
			}

			if user == (User{}) {
				return fmt.Errorf("no such user peer")
			}

			pc := i.UsersPeers[user]
			sdp := webrtc.SessionDescription{Type: webrtc.NewSDPType(update.Type), SDP: update.Sdp}

			err := i.acceptAnswer(pc, sdp)
			if err != nil {
				return err
			}
		}
	}
}

func (i *DispatcherInteractor) initializePeerConnection() (*webrtc.PeerConnection, error) {
	pc, err := webrtc.NewPeerConnection(config)
	if err != nil {
		return nil, err
	}

	return pc, nil
}

func (i *DispatcherInteractor) createDatachannel(pc *webrtc.PeerConnection) error {
	dataChannel, err := pc.CreateDataChannel(dispatcherUsername, nil)
	if err != nil {
		return err
	}

	dataChannel.OnMessage(func(msg webrtc.DataChannelMessage) {
		i.logger.Debug(context.Background(), "received message", zap.String("text", string(msg.Data)))
	})

	dataChannel.OnOpen(func() {
		ticker := time.NewTicker(time.Second * 5)

		for {
			select {
			case <-ticker.C:
				if dataChannel.ReadyState() == webrtc.DataChannelStateClosed {
					return
				}

				err := dataChannel.SendText("ping")
				if err != nil {
					i.logger.Error(context.Background(), "couldnt send a ping")
					return
				}
				i.logger.Debug(context.Background(), "sent a ping through datachannel")
			}
		}
	})

	return nil
}

func (i *DispatcherInteractor) createOffer(pc *webrtc.PeerConnection) error {
	if pc.SignalingState() == webrtc.SignalingStateStable {
		sdp, err := pc.CreateOffer(nil)
		if err != nil {
			return err
		}

		err = pc.SetLocalDescription(sdp)
		if err != nil {
			return err
		}

		gatheringComplete := webrtc.GatheringCompletePromise(pc)

		// Workaround, implement Ice Trickling later
		select {
		case <-gatheringComplete:
		case <-time.After(time.Second):
		}

	}

	return nil
}

func (i *DispatcherInteractor) acceptAnswer(pc *webrtc.PeerConnection, answer webrtc.SessionDescription) error {
	err := pc.SetRemoteDescription(answer)
	if err != nil {
		return err
	}

	return nil
}
