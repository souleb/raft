package server

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	pb "github.com/souleb/raft/api"
	"github.com/souleb/raft/errors"
	"golang.org/x/exp/slog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	// defaultHeartbeatTimeout is the heartbeat timeout in milliseconds.
	defaultHeartbeatTimeout = 50
	// defaultTimeout is the timeout in milliseconds.
	defaultTimeout = 100
	//defaultRetry is the number of retry to connect to a peer.
	defaultRetry = 3
)

type getCurrentTermFunc func() int64
type SendAppendEntriesFunc func(term int64, leaderId int32, prevLogIndex int64, prevLogTerm int64, entries []byte, leaderCommit int64, responseChan chan RPCResponse)
type SendVoteRequestFunc func(term int64, candidateID int32, lastLogIndex int64, lastLogTerm int64, responseChan chan RPCResponse)

type AppendEntries struct {
	Term         int64
	LeaderId     int32
	PrevLogIndex int64
	PrevLogTerm  int64
	Entries      []byte
	LeaderCommit int64
	ResponseChan chan RPCResponse
}

type VoteRequest struct {
	Term         int64
	CandidateId  int32
	LastLogIndex int64
	LastLogTerm  int64
	ResponseChan chan RPCResponse
}

type options struct {
	getCurrentTermFunc    getCurrentTermFunc
	sendVoteRequestFunc   SendVoteRequestFunc
	sendAppendEntriesFunc SendAppendEntriesFunc
	// heartbeatTimeout(ms) is used to send heartbeat to other peers
	heartbeatTimeout int
	// timeout(ms) is used to set the dial timeout for connecting to peers.
	timeout int
	// logger is the logger used by the server.
	logger *slog.Logger
}

type OptFunc func(o *options)

type Server struct {
	pb.UnimplementedAppendEntriesServer
	pb.UnimplementedVoteServer
	peersConn  map[int]*grpc.ClientConn
	id         int
	port       uint16
	grpcServer *grpc.Server
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

func WithVoteRequestFunc(s func(term int64, candidateID int32, lastLogIndex int64, lastLogTerm int64, responseChan chan RPCResponse)) OptFunc {
	return func(o *options) {
		o.sendVoteRequestFunc = s
	}
}

func WithAppendEntriesFunc(s func(term int64, leaderID int32, prevLogIndex int64, prevLogTerm int64, entries []byte, leaderCommit int64, responseChan chan RPCResponse)) OptFunc {
	return func(o *options) {
		o.sendAppendEntriesFunc = s
	}
}

func WithGetCurrentTermFunc(g getCurrentTermFunc) OptFunc {
	return func(o *options) {
		o.getCurrentTermFunc = g
	}
}

func WithLogger(logger *slog.Logger) OptFunc {
	return func(o *options) {
		o.logger = logger
	}
}

func New(id int, port uint16, opts ...OptFunc) (*Server, error) {
	s := &Server{
		id: id,
	}

	for _, opt := range opts {
		opt(&s.options)
	}

	if s.getCurrentTermFunc == nil || s.sendAppendEntriesFunc == nil || s.sendVoteRequestFunc == nil {
		return nil, fmt.Errorf("appendEntriesRPC and voteRPC channels are mandatory")
	}

	if s.logger == nil {
		return nil, fmt.Errorf("a logger is mandatory")
	}

	return s, nil
}

func (s *Server) Run(ctx context.Context, peers map[int]string, testMode bool) error {
	// start server
	err := s.start()
	if err != nil {
		return fmt.Errorf("error while starting the grpc server: %w", err)
	}
	err = s.connectToPeers(ctx, peers, testMode)
	if err != nil {
		return fmt.Errorf("error while connecting to peers: %w", err)
	}
	return nil
}

func (s *Server) GetPeersConn() map[int]*grpc.ClientConn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peersConn
}

// requestVoteMsg is a message sent to the raft node to request a vote.
type RPCResponse struct {
	Term     int64
	Response bool
}

// RequestVote is called by candidates to gather votes.
func (s *Server) RequestVote(ctx context.Context, in *pb.VoteRequest) (*pb.VoteResponse, error) {
	term := s.getCurrentTermFunc()
	if in.GetTerm() < term {
		return &pb.VoteResponse{
			Term:        term,
			VoteGranted: false,
		}, nil
	}

	reply := make(chan RPCResponse)
	s.sendVoteRequestFunc(in.GetTerm(), in.GetCandidateId(), in.GetLastLogIndex(), in.GetLastLogTerm(), reply)

	response := <-reply

	return &pb.VoteResponse{Term: response.Term, VoteGranted: response.Response}, nil
}

func (s *Server) AppendEntries(ctx context.Context, in *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	term := s.getCurrentTermFunc()
	if in.GetTerm() < term {
		return &pb.AppendEntriesResponse{
			Term:    term,
			Success: false,
		}, nil
	}

	reply := make(chan RPCResponse)
	s.sendAppendEntriesFunc(in.GetTerm(), in.GetLeaderId(), in.GetPrevLogIndex(), in.GetPrevLogTerm(), in.GetEntries(), in.GetLeaderCommit(), reply)

	response := <-reply

	return &pb.AppendEntriesResponse{Term: response.Term, Success: response.Response}, nil
}

// SendRequestVote sends a request vote to a node.
func (s *Server) SendRequestVote(ctx context.Context, node int, req VoteRequest) (*RPCResponse, error) {
	client := pb.NewVoteClient(s.GetPeersConn()[node])
	resp, err := client.RequestVote(ctx, &pb.VoteRequest{
		Term:         req.Term,
		CandidateId:  req.CandidateId,
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	})
	if err != nil {
		st, ok := status.FromError(err)
		if !ok {
			return nil, fmt.Errorf("failed to send request vote to node %d: %w", node, err)
		}
		return nil, &errors.Error{StatusCode: errors.Code(st.Code()), Err: err}
	}
	return &RPCResponse{resp.GetTerm(), resp.GetVoteGranted()}, nil
}

// sendRequestVote sends a request vote to a node.
func (s *Server) SendAppendEntries(ctx context.Context, node int, req AppendEntries) (*RPCResponse, error) {
	client := pb.NewAppendEntriesClient(s.GetPeersConn()[node])
	resp, err := client.AppendEntries(ctx, &pb.AppendEntriesRequest{
		Term:         req.Term,
		LeaderId:     req.LeaderId,
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      req.Entries,
		LeaderCommit: req.LeaderCommit,
	})
	if err != nil {
		st, ok := status.FromError(err)
		if !ok {
			return nil, fmt.Errorf("failed to send append entries to node %d: %w", node, err)
		}
		return nil, &errors.Error{StatusCode: errors.Code(st.Code()), Err: err}
	}

	return &RPCResponse{resp.GetTerm(), resp.GetSuccess()}, nil
}

func (s *Server) connectToPeers(ctx context.Context, peers map[int]string, testMode bool) error {
	//TODO: handle TLS and Use insecure.NewCredentials() for testing
	// TODO: handle reconnect
	ropts := []grpc_retry.CallOption{
		grpc_retry.WithBackoff(grpc_retry.BackoffLinear(time.Duration(defaultHeartbeatTimeout) * time.Millisecond)),
		grpc_retry.WithCodes(codes.Unavailable),
		grpc_retry.WithMax(defaultRetry),
	}

	opts := []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithUnaryInterceptor(grpc_retry.UnaryClientInterceptor(ropts...)),
	}

	if testMode {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	if s.peersConn == nil {
		s.peersConn = make(map[int]*grpc.ClientConn)
	}

	for key, addr := range peers {
		conn, err := s.connectToPeer(ctx, addr, opts)
		if err != nil {
			return err
		}
		s.peersConn[key] = conn
	}
	s.logger.Info("connected to peers", slog.Int("id", int(s.id)))
	return nil
}

func (s *Server) connectToPeer(ctx context.Context, addr string, opts []grpc.DialOption) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.timeout)*time.Millisecond)
	defer cancel()
	retryCount := 0
	var (
		conn *grpc.ClientConn
		err  error
	)
	for retryCount < defaultRetry {
		retryCount++
		conn, err = grpc.DialContext(ctx, addr, opts...)
		if err == nil {
			break
		}
	}
	return conn, err
}

func (s *Server) start() error {
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", s.id))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	s.grpcServer = grpc.NewServer()
	pb.RegisterAppendEntriesServer(s.grpcServer, s)
	pb.RegisterVoteServer(s.grpcServer, s)
	go s.grpcServer.Serve(lis)
	return nil
}

func (s *Server) Stop() {
	s.grpcServer.Stop()
}
