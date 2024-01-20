package server

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"log/slog"

	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	pb "github.com/souleb/raft/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
)

const (
	// defaultTimeout is the timeout in milliseconds.
	defaultTimeout = 100
	//defaultRetry is the number of retry to connect to a peer.
	defaultRetry = 3
	// leader health service
	leaderHealthService = "quis.RaftLeader"
)

// Sender is the interface that wraps the basic methods for sending RPCs to
// other raft nodes.
type Sender interface {
	// SendAppendEntries sends an AppendEntries RPC to the given node.
	SendAppendEntries(ctx context.Context, node uint, req AppendEntries) (*RPCResponse, error)
	// SendRequestVote sends a VoteRequest RPC to the given node.
	SendRequestVote(ctx context.Context, node uint, req VoteRequest) (*RPCResponse, error)
	// SendInstallSnapshot sends an InstallSnapshot RPC to the given node.
	SendInstallSnapshot(ctx context.Context, node uint, req SnapshotRequest) (*RPCResponse, error)
}

// Server is the interface that wraps the basic methods for communicating with
// with a raft node.
type Server interface {
	Sender
	// Run starts the server.
	Run(ctx context.Context, peers map[uint]string, testMode bool) error
	// SetVoteRPCChan sets the channel to send vote requests to the fsm.
	SetVoteRPCChan(voteChan chan VoteRequest) Server
	// SetAppendEntryRPCChan sets the channel to send entries to replicate to the fsm.
	SetAppendEntryRPCChan(appendEntriesChan chan AppendEntries) Server
	// SetApplyEntryRPCChan sets the channel to send entries to apply to the fsm.
	SetApplyEntryRPCChan(applyEntryChan chan ApplyRequest) Server
	//SetInstallSnapshotRPCChan sets the channel to send snapshot to the fsm.
	SetInstallSnapshotRPCChan(installSnapshotChan chan SnapshotRequest) Server
	// Stop stops the server.
	Stop()
}

var _ Server = (*RPCServer)(nil)

// getStateFunc is a function that returns the current term and whether this
// peer is the leader. This function is used to report the health of the leader
// to the health server.
type getStateFunc func() (uint64, bool)

type options struct {
	// getStateFunc is a function that returns the current term and whether this
	getStateFunc getStateFunc
	// heartbeatTimeout(ms) is used to send heartbeat to other peers
	heartbeatTimeout int
	// timeout(ms) is used to set the dial timeout for connecting to peers.
	timeout int
	// logger is the logger used by the server.
	logger *slog.Logger
}

type OptFunc func(o *options)

// RPCServer is a RPC server that handles all the RPC communications.
// It implements the Server interface and uses the grpc protocol.
type RPCServer struct {
	pb.UnimplementedAppendEntriesServer
	pb.UnimplementedVoteServer
	pb.UnimplementedApplyEntryServer
	pb.UnimplementedInstallSnapshotServer
	peersConn              map[uint]*grpc.ClientConn
	deadPeersConn          map[uint]bool
	id                     int
	port                   uint16
	grpcServer             *grpc.Server
	hs                     *health.Server
	observerChan           chan bool
	voteRPCChan            chan VoteRequest
	appendEntriesRPCChan   chan AppendEntries
	applyEntryRPCChan      chan ApplyRequest
	installSnapshotRPCChan chan SnapshotRequest
	options
	// Lock to protect shared access to this peer's state
	mu sync.RWMutex
	// done is used to stop the health server.
	done chan struct{}
}

// WithTimeout sets the timeout.
func WithTimeout(timeout int) OptFunc {
	return func(o *options) {
		o.timeout = timeout
	}
}

// WithGetCStateFunc sets the function to get the current term.
func WithGetStateFunc(g getStateFunc) OptFunc {
	return func(o *options) {
		o.getStateFunc = g
	}
}

// WithLogger sets the logger.
func WithLogger(logger *slog.Logger) OptFunc {
	return func(o *options) {
		o.logger = logger
	}
}

// New returns a new RPCServer.
func New(id int, port uint16, opts ...OptFunc) (*RPCServer, error) {
	s := &RPCServer{
		id:            id,
		port:          port,
		deadPeersConn: make(map[uint]bool),
		peersConn:     make(map[uint]*grpc.ClientConn),
		done:          make(chan struct{}),
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

// SetApplyEntryRPCChan sets the channel to send entries to apply to the fsm.
func (s *RPCServer) SetApplyEntryRPCChan(applyEntryChan chan ApplyRequest) Server {
	s.applyEntryRPCChan = applyEntryChan
	return s
}

// SetVoteRPCChan sets the channel to send vote requests to the fsm.
func (s *RPCServer) SetVoteRPCChan(voteChan chan VoteRequest) Server {
	s.voteRPCChan = voteChan
	return s
}

// SetAppendEntryRPCChan sets the channel to send entries to replicate to the fsm.
func (s *RPCServer) SetAppendEntryRPCChan(appendEntriesChan chan AppendEntries) Server {
	s.appendEntriesRPCChan = appendEntriesChan
	return s
}

// SetInstallSnapshotRPCChan sets the channel to send snapshot to the fsm.
func (s *RPCServer) SetInstallSnapshotRPCChan(installSnapshotChan chan SnapshotRequest) Server {
	s.installSnapshotRPCChan = installSnapshotChan
	return s
}

// Run starts the server.
func (s *RPCServer) Run(ctx context.Context, peers map[uint]string, secure bool) error {
	// check if channels are set
	if s.voteRPCChan == nil || s.appendEntriesRPCChan == nil || s.applyEntryRPCChan == nil {
		return fmt.Errorf("channels to send RPCs are not set")
	}

	// start server
	err := s.start()
	if err != nil {
		return fmt.Errorf("error while starting the grpc server: %w", err)
	}

	err = s.connectToPeers(ctx, peers, secure)
	if err != nil {
		return fmt.Errorf("error while connecting to peers: %w", err)
	}

	// start the conne checkers
	p := make(map[uint]string)
	for k, v := range peers {
		p[k] = v
	}
	go s.checkConn(ctx, p, secure)
	return nil
}

func (s *RPCServer) isPeerDead(index uint) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.deadPeersConn[index]
}

func (s *RPCServer) setPeerDead(index uint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deadPeersConn[index] = true
}

func (s *RPCServer) setPeerAlive(index uint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deadPeersConn[index] = false
}

// GetPeerConn returns a grpc connection to be used to send RPCs to the peer.
func (s *RPCServer) GetPeerConn(index uint) *grpc.ClientConn {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.peersConn[index]
}

func (s *RPCServer) setPeerConn(conn *grpc.ClientConn, index uint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.peersConn[index] = conn
}

func (s *RPCServer) connectToPeers(ctx context.Context, peers map[uint]string, secure bool) error {
	opts := makeOpts(secure)

	if s.peersConn == nil {
		s.peersConn = make(map[uint]*grpc.ClientConn)
	}

	for key, addr := range peers {
		conn, err := s.connectToPeer(ctx, addr, opts)
		if err != nil {
			s.setPeerDead(key)
			return fmt.Errorf("error while connecting to peer: %w", err)
		}
		s.setPeerConn(conn, key)
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
	if err != nil {
		return conn, fmt.Errorf("failed to dial: %w", err)
	}
	return conn, nil
}

func (s *RPCServer) start() error {
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", s.port))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	s.grpcServer = grpc.NewServer()
	pb.RegisterAppendEntriesServer(s.grpcServer, s)
	pb.RegisterVoteServer(s.grpcServer, s)
	pb.RegisterApplyEntryServer(s.grpcServer, s)

	// setup health server
	err = s.setupHealthServer([]string{leaderHealthService}, s.done)
	if err != nil {
		return fmt.Errorf("error while setting up the health server: %w", err)
	}

	go s.grpcServer.Serve(lis)
	return nil
}

// Stop stops the server.
func (s *RPCServer) Stop() {
	if s.hs != nil {
		s.done <- struct{}{}
		s.hs.Shutdown()
	}
	s.grpcServer.Stop()
}

// Notify is used to observe the state of the node. It implements the Observer
// interface.
func (s *RPCServer) Notify(state bool) {
	s.observerChan <- state
}

// checkConn checks if the connection to the peer is alive and if not, tries to reconnect
// by calling connectToPeer.
// We do this instead of resetting the connection because our server cannot be restarted
// so most likely we are trying to reconnect to a new server instance.
func (s *RPCServer) checkConn(ctx context.Context, peers map[uint]string, secure bool) {
	opts := makeOpts(secure)
	opts = append(opts, grpc.WithBlock())
	ticker := time.NewTicker(defaultTimeout * time.Millisecond >> 4)
	for {
		select {
		case <-ticker.C:
			for id, peer := range peers {
				if s.isPeerDead(id) {
					s.logger.Debug("new attempt to reconnect to peer", slog.Uint64("id", uint64(id)))
					conn, err := s.connectToPeer(ctx, peer, opts)
					if err != nil {
						s.logger.Error("error while trying to reconnect to peer", slog.Uint64("id", uint64(id)), slog.String("error", err.Error()))
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
		grpc_retry.WithBackoff(grpc_retry.BackoffExponentialWithJitter(defaultTimeout*time.Millisecond>>4, 0.10)),
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
