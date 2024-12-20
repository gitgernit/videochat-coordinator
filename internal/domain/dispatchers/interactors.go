package dispatchers

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"gitlab.crja72.ru/gospec/go5/contracts/proto/rooms/go/proto"
	"gitlab.crja72.ru/gospec/go5/rooms/pkg/logger"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"io"
	"slices"
	"sync"
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
	TrackAlreadyExistsErr = fmt.Errorf("track alreadly exists")
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
	logger       logger.Logger
	stream       proto.RoomsService_JoinRoomClient
	forwarder    *RTPForwarder
	mutex        *sync.Mutex
	UsersPeers   map[User]*webrtc.PeerConnection
	RemoteTracks map[User][]*webrtc.TrackRemote
	LocalTracks  map[User][]*webrtc.TrackLocalStaticRTP
	RoomID       string
	GrpcHost     string
	GrpcPort     int
}

func NewDispatcherInteractor(logger logger.Logger, roomID string, grpcHost string, grpcPort int) (*DispatcherInteractor, error) {
	return &DispatcherInteractor{
		logger:       logger,
		mutex:        &sync.Mutex{},
		forwarder:    NewRTPForwarder(logger),
		UsersPeers:   make(map[User]*webrtc.PeerConnection),
		RemoteTracks: make(map[User][]*webrtc.TrackRemote),
		LocalTracks:  make(map[User][]*webrtc.TrackLocalStaticRTP),
		RoomID:       roomID,
		GrpcHost:     grpcHost,
		GrpcPort:     grpcPort,
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

	i.stream = stream

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
					err := pc.Close()
					if err != nil {
						i.logger.Error(ctx, "couldnt close inactive peer connection", zap.Error(err))
					}

					for _, trackRemote := range i.RemoteTracks[user] {
						err := i.forwarder.Unbind(trackRemote)
						if err != nil {
							i.logger.Error(ctx, "couldnt unbind a remote track", zap.Error(err))
						}
					}

					for u, p := range i.UsersPeers {
						if u == user {
							continue
						}

						err = i.cleanupTracks(p, user)
						if err != nil {
							i.logger.Error(ctx, "couldnt cleanup a track", zap.Error(err))
						}
					}

					delete(i.RemoteTracks, user)
					delete(i.UsersPeers, user)
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
			}

			err := i.signalUsers()
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

	for _, typ := range []webrtc.RTPCodecType{webrtc.RTPCodecTypeVideo, webrtc.RTPCodecTypeAudio} {
		if _, err := pc.AddTransceiverFromKind(typ, webrtc.RTPTransceiverInit{
			Direction: webrtc.RTPTransceiverDirectionRecvonly,
		}); err != nil {
			i.logger.Error(context.Background(), "Failed to add transceiver")
			return nil, err
		}
	}

	pc.OnTrack(func(remote *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		i.logger.Debug(context.Background(), "received a track", zap.String("kind", remote.Kind().String()), zap.String("id", remote.ID()), zap.String("stream_id", remote.StreamID()))
		var user User

		for u, p := range i.UsersPeers {
			if pc == p {
				user = u
				break
			}
		}

		if user == (User{}) {
			i.logger.Error(context.Background(), "couldnt find a user corresponding to the peer connection")
			return
		}

		userTracks, ok := i.RemoteTracks[user]
		if !ok {
			i.RemoteTracks[user] = make([]*webrtc.TrackRemote, 0, 1)
		}
		userTracks = append(userTracks, remote)
		i.RemoteTracks[user] = userTracks

		err := i.signalUsers()
		if err != nil {
			i.logger.Error(context.Background(), "couldnt signal users", zap.Error(err))
			return
		}
	})

	go func() {
		for range time.NewTicker(time.Second * 3).C {
			err := i.dispatchKeyframe(pc)
			if errors.Is(err, io.ErrClosedPipe) {
				return
			}
			if err != nil {
				i.logger.Error(context.Background(), "couldnt dispatch keyframe", zap.Error(err))
				return
			}
		}
	}()

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
		//ticker := time.NewTicker(time.Second * 10)
		//
		//for {
		//	select {
		//	case <-ticker.C:
		//		if dataChannel.ReadyState() == webrtc.DataChannelStateClosed {
		//			return
		//		}
		//
		//		err := dataChannel.SendText("ping")
		//		if err != nil {
		//			i.logger.Error(context.Background(), "couldnt send a ping")
		//			return
		//		}
		//		i.logger.Debug(context.Background(), "sent a ping through datachannel")
		//	}
		//}
	})

	return nil
}

func (i *DispatcherInteractor) createOffer(pc *webrtc.PeerConnection) error {
	timeout := time.After(30 * time.Second)
	tick := time.Tick(100 * time.Millisecond)

	// Wait for the signaling state to become stable
	for {
		select {
		case <-timeout:
			i.logger.Error(context.Background(), "timeout waiting for signaling state to become stable")
			return nil
		case <-tick:
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
				case <-time.After(time.Millisecond * 250):
				}
				return nil
			}
		}
	}
}

func (i *DispatcherInteractor) acceptAnswer(pc *webrtc.PeerConnection, answer webrtc.SessionDescription) error {
	if pc.SignalingState() != webrtc.SignalingStateStable {
		err := pc.SetRemoteDescription(answer)
		if err != nil {
			return err
		}
	}

	return nil
}

func (i *DispatcherInteractor) signalUsers() error {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	sdps := make([]*proto.SDP, 0, len(i.UsersPeers))

	for user, pc := range i.UsersPeers {
		if user.Name == dispatcherUsername {
			continue
		}

		err := i.initializeTracks(pc)
		if err != nil {
			return err
		}
		i.logger.Debug(context.Background(), "initialized tracks")

		err = i.createOffer(pc)
		if err != nil {
			return err
		}
		i.logger.Debug(context.Background(), "created offer")

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

	err := i.stream.Send(method)
	if err != nil {
		return err
	}

	return nil
}

func (i *DispatcherInteractor) dispatchKeyframe(pc *webrtc.PeerConnection) error {
	for _, receiver := range pc.GetReceivers() {
		if receiver.Track() == nil {
			continue
		}

		err := pc.WriteRTCP([]rtcp.Packet{
			&rtcp.PictureLossIndication{
				MediaSSRC: uint32(receiver.Track().SSRC()),
			},
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (i *DispatcherInteractor) initializeTracks(pc *webrtc.PeerConnection) error {
	for user, tracks := range i.RemoteTracks {
		for _, track := range tracks {
			err := i.initializeTrack(pc, track, user)
			if errors.Is(err, TrackAlreadyExistsErr) {
				continue
			}
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (i *DispatcherInteractor) initializeTrack(pc *webrtc.PeerConnection, remote *webrtc.TrackRemote, user User) error {
	msid := user.Name + "-" + uuid.New().String()
	senders := pc.GetSenders()

	for _, sender := range senders {
		if sender.Track().ID() == remote.ID() {
			return TrackAlreadyExistsErr
		}
	}

	trackLocal, err := webrtc.NewTrackLocalStaticRTP(
		remote.Codec().RTPCodecCapability,
		remote.ID(),
		msid,
	)
	if err != nil {
		return err
	}

	_, ok := i.LocalTracks[user]
	if !ok {
		i.LocalTracks[user] = make([]*webrtc.TrackLocalStaticRTP, 0, 1)
	}

	localUserTracks := i.LocalTracks[user]
	localUserTracks = append(localUserTracks, trackLocal)
	i.LocalTracks[user] = localUserTracks

	_, err = pc.AddTrack(trackLocal)
	if err != nil {
		return err
	}

	i.logger.Debug(context.Background(), "added a track", zap.String("id", trackLocal.ID()))

	err = i.forwarder.Bind(trackLocal, remote)
	if err != nil {
		return err
	}

	return nil
}

func (i *DispatcherInteractor) cleanupTracks(pc *webrtc.PeerConnection, from User) error {
	senders := pc.GetSenders()

	for _, sender := range senders {
		track := sender.Track()

		if track == nil || slices.Contains(i.LocalTracks[from], track.(*webrtc.TrackLocalStaticRTP)) {
			err := pc.RemoveTrack(sender)
			if err != nil {
				return err
			}

			err = sender.Stop()
			if err != nil {
				return err
			}
		}
	}

	return nil
}

type RTPForwarder struct {
	remotes map[*webrtc.TrackRemote][]*webrtc.TrackLocalStaticRTP
	logger  logger.Logger
	mutex   *sync.Mutex
}

var (
	TrackAlreadyBoundErr = fmt.Errorf("track already bound to a remote")
)

func NewRTPForwarder(logger logger.Logger) *RTPForwarder {
	return &RTPForwarder{
		remotes: make(map[*webrtc.TrackRemote][]*webrtc.TrackLocalStaticRTP),
		logger:  logger,
		mutex:   &sync.Mutex{},
	}
}

func (f *RTPForwarder) Bind(local *webrtc.TrackLocalStaticRTP, remote *webrtc.TrackRemote) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	_, ok := f.remotes[remote]
	if !ok {
		f.remotes[remote] = make([]*webrtc.TrackLocalStaticRTP, 0, 1)
		defer func() {
			go f.forward(remote)
		}()
	}

	locals := f.remotes[remote]

	if slices.Contains(locals, local) {
		return TrackAlreadyBoundErr
	}

	locals = append(locals, local)

	f.remotes[remote] = locals

	return nil
}

func (f *RTPForwarder) Unbind(remote *webrtc.TrackRemote) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	delete(f.remotes, remote)

	return nil
}

func (f *RTPForwarder) forward(remote *webrtc.TrackRemote) {
	buf := make([]byte, 2048)
	rtpPkt := &rtp.Packet{}

	for {
		_, ok := f.remotes[remote]
		if !ok {
			return
		}

		read, _, err := remote.Read(buf)
		if err == io.EOF {
			return
		}
		if err != nil {
			f.logger.Error(context.Background(), "failed to read rtp packets from remote", zap.Error(err))
			return
		}

		if err = rtpPkt.Unmarshal(buf[:read]); err != nil {
			f.logger.Error(context.Background(), "failed to unmarshal incoming RTP packet", zap.Error(err))
			return
		}

		rtpPkt.Extension = false
		rtpPkt.Extensions = nil

		f.mutex.Lock()
		for _, local := range f.remotes[remote] {
			if err = local.WriteRTP(rtpPkt); err != nil {
				f.logger.Error(context.Background(), "couldnt write rtp packets", zap.Error(err))
				return
			}
		}
		f.mutex.Unlock()
	}
}
