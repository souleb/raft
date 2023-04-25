package server

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	pb "github.com/souleb/raft/api"
	"golang.org/x/exp/slog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
)

const (
	// defaultHeartbeatTimeout is the heartbeat timeout in milliseconds.
	defaultHeartbeatTimeout = 50
	// defaultTimeout is the timeout in milliseconds.
	defaultTimeout = 100
	//defaultRetry is the number of retry to connect to a peer.
	defaultRetry = 3
	// leader health service
	leaderHealthService = "quis.RaftLeader"
)

type Server interface {
	SendAppendEntries(ctx context.Context, node int, req AppendEntries) (*RPCResponse, error)
	SendRequestVote(ctx context.Context, node int, req VoteRequest) (*RPCResponse, error)
	Run(ctx context.Context, peers map[int]string, testMode bool) error
	SetVoteRPCChan(voteChan chan VoteRequest) Server
	SetAppendEntryRPCChan(appendEntriesChan chan AppendEntries) Server
	SetApplyEntryRPCChan(applyEntryChan chan ApplyRequest) Server
	Stop()
}

type getStateFunc func() (int64, bool)

type options struct {
	getStateFunc getStateFunc
	// heartbeatTimeout(ms) is used to send heartbeat to other peers
	heartbeatTimeout int
	// timeout(ms) is used to set the dial timeout for connecting to peers.
	timeout int
	// logger is the logger used by the server.
	logger *slog.Logger
}

type OptFunc func(o *options)

type RPCServer struct {
	pb.UnimplementedAppendEntriesServer
	pb.UnimplementedVoteServer
	pb.UnimplementedApplyEntryServer
	peersConn            map[int]*grpc.ClientConn
	deadPeersConn        map[int]bool
	id                   int
	port                 uint16
	grpcServer           *grpc.Server
	hs                   *health.Server
	observerChan         chan bool
	voteRPCChan          chan VoteRequest
	appendEntriesRPCChan chan AppendEntries
	applyEntryRPCChan    chan ApplyRequest
	options
	// Lock to protect shared access to this peer's state
	mu sync.Mutex
}

func WithHeartbeatTimeout(heartbeatTimeout int) OptFunc {
	return func(o *options) {
		o.heartbeatTimeout = heartbeatTimeout
	}
}

func WithTimeout(timeout int) OptFunc {
	return func(o *options) {
		o.timeout = timeout
	}
}

func WithGetCurrentTermFunc(g getStateFunc) OptFunc {
	return func(o *options) {
		o.getStateFunc = g
	}
}

func WithLogger(logger *slog.Logger) OptFunc {
	return func(o *options) {
		o.logger = logger
	}
}

func New(id int, port uint16, opts ...OptFunc) (*RPCServer, error) {
	s := &RPCServer{
		id:            id,
		port:          port,
		deadPeersConn: make(map[int]bool),
		peersConn:     make(map[int]*grpc.ClientConn),
	}

	for _, opt := range opts {
		opt(&s.options)
	}

	if s.getStateFunc == nil {
		return nil, fmt.Errorf("a function to get the current term is mandatory")
	}

	if s.logger == nil {
		return nil, fmt.Errorf("a logger is mandatory")
	}

	s.observerChan = make(chan bool, 1)

	return s, nil
}

func (s *RPCServer) SetApplyEntryRPCChan(applyEntryChan chan ApplyRequest) Server {
	s.applyEntryRPCChan = applyEntryChan
	return s
}

func (s *RPCServer) SetVoteRPCChan(voteChan chan VoteRequest) Server {
	s.voteRPCChan = voteChan
	return s
}

func (s *RPCServer) SetAppendEntryRPCChan(appendEntriesChan chan AppendEntries) Server {
	s.appendEntriesRPCChan = appendEntriesChan
	return s
}

func (s *RPCServer) Run(ctx context.Context, peers map[int]string, secure bool) error {
	// check if channels are set
	if s.voteRPCChan == nil || s.appendEntriesRPCChan == nil || s.applyEntryRPCChan == nil {
		return fmt.Errorf("channels to send RPCs are not set")
	}

	// start server
	if s.grpcServer != nil {
		return fmt.Errorf("server already started")
	}

	err := s.start()
	if err != nil {
		return fmt.Errorf("error while starting the grpc server: %w", err)
	}

	err = s.connectToPeers(ctx, peers, secure)
	if err != nil {
		return fmt.Errorf("error while connecting to peers: %w", err)
	}

	// start the conne checkers
	p := make(map[int]string)
	for k, v := range peers {
		p[k] = v
	}
	go s.checkConn(ctx, p, secure)
	return nil
}

func (s *RPCServer) isPeerDead(index int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deadPeersConn[index]
}

func (s *RPCServer) setPeerDead(index int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deadPeersConn[index] = true
}

func (s *RPCServer) setPeerAlive(index int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deadPeersConn[index] = false
}

func (s *RPCServer) GetPeerConn(index int) *grpc.ClientConn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peersConn[index]
}

func (s *RPCServer) setPeerConn(conn *grpc.ClientConn, index int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.peersConn[index] = conn
}

func (s *RPCServer) connectToPeers(ctx context.Context, peers map[int]string, secure bool) error {
	// TODO: handle TLS and Use insecure.NewCredentials() for testing
	// TODO: handle reconnection in case of peer failure
	opts := makeOpts(secure)

	if s.peersConn == nil {
		s.peersConn = make(map[int]*grpc.ClientConn)
	}

	for key, addr := range peers {
		conn, err := s.connectToPeer(ctx, addr, opts)
		if err != nil {
			s.deadPeersConn[key] = true
			return fmt.Errorf("error while connecting to peer: %w", err)
		}
		s.peersConn[key] = conn
	}
	s.logger.Info("connected to peers", slog.Int("id", int(s.id)))
	return nil
}

func (s *RPCServer) connectToPeer(ctx context.Context, addr string, opts []grpc.DialOption) (*grpc.ClientConn, error) {
	if s.timeout == 0 {
		s.timeout = defaultTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.timeout)*time.Millisecond)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err == nil {
		return conn, nil
	}
	return conn, err
}

func (s *RPCServer) start() error {
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", s.port))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	s.grpcServer = grpc.NewServer()
	pb.RegisterAppendEntriesServer(s.grpcServer, s)
	pb.RegisterVoteServer(s.grpcServer, s)

	if s.hs == nil {
		// setup health server
		err := s.setupHealthServer([]string{leaderHealthService})
		if err != nil {
			return fmt.Errorf("error while setting up the health server: %w", err)
		}
	}

	go s.grpcServer.Serve(lis)
	return nil
}

func (s *RPCServer) Stop() {
	if s.hs != nil {
		s.hs.Shutdown()
	}
	s.grpcServer.Stop()
}

func (s *RPCServer) Observe(state bool) {
	s.observerChan <- state
}

// checkConn checks if the connection to the peer is alive and if not, tries to reconnect
// by calling connectToPeer.
// We do this instead of resetting the connection because our server cannot be restarted
// so most likely we are trying to reconnect to a new server instance.
func (s *RPCServer) checkConn(ctx context.Context, peers map[int]string, secure bool) {
	opts := makeOpts(secure)
	ticker := time.NewTicker(defaultHeartbeatTimeout / 2 * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			for id := range peers {
				if s.isPeerDead(id) {
					s.logger.Debug("new attempt to reconnect to peer", slog.Int("id", id))
					conn, err := s.connectToPeer(ctx, peers[id], opts)
					if err != nil {
						s.logger.Error("error while trying to reconnect to peer", slog.Int("id", id), slog.String("error", err.Error()))
						continue
					}
					s.setPeerConn(conn, id)
					s.setPeerAlive(id)
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func makeOpts(secure bool) []grpc.DialOption {
	rOpts := []grpc_retry.CallOption{
		grpc_retry.WithBackoff(grpc_retry.BackoffLinear(time.Duration(defaultHeartbeatTimeout) * time.Millisecond)),
		grpc_retry.WithCodes(codes.Unavailable),
		grpc_retry.WithMax(defaultRetry),
	}

	opts := []grpc.DialOption{
		grpc.WithUnaryInterceptor(grpc_retry.UnaryClientInterceptor(rOpts...)),
	}

	if !secure {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	return opts
}
