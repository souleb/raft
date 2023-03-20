package server

import (
	"context"
	"fmt"
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

type Server struct {
	pb.UnimplementedAppendEntriesServer
	pb.UnimplementedVoteServer
	peersConn  map[int]*grpc.ClientConn
	appendChan chan appenEntriesMsg
	voteChan   chan requestVoteMsg
	// heartbeatTimeout(ms) is used to send heartbeat to other peers
	heartbeatTimeout int
	// timeout(ms) is used to set the dial timeout for connecting to peers.
	timeout int
	id      int
	// logger is the logger used by the server.
	logger *slog.Logger
	// Lock to protect shared access to this peer's state
	mu sync.Mutex
}

func (s *Server) GetPeersConn() map[int]*grpc.ClientConn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peersConn
}

// requestVoteMsg is a message sent to the raft node to request a vote.
type requestVoteMsg struct {
	msg *pb.VoteRequest
	// reply is a channel to send the response to.
	reply chan *pb.VoteResponse
}

// RequestVote is called by candidates to gather votes.
func (s *Server) RequestVote(ctx context.Context, in *pb.VoteRequest) (*pb.VoteResponse, error) {
	// term := s.getCurrentTerm()
	// if in.GetTerm() < term {
	// 	return &pb.VoteResponse{
	// 		Term:        term,
	// 		VoteGranted: false,
	// 	}, nil
	// }

	msg := requestVoteMsg{
		msg:   in,
		reply: make(chan *pb.VoteResponse),
	}

	s.voteChan <- msg

	return <-msg.reply, nil
}

type appenEntriesMsg struct {
	msg   *pb.AppendEntriesRequest
	reply chan *pb.AppendEntriesResponse
}

func (s *Server) AppendEntries(ctx context.Context, in *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	// term := s.getCurrentTerm()
	// if in.GetTerm() < term {
	// 	return &pb.AppendEntriesResponse{
	// 		Term:    term,
	// 		Success: false,
	// 	}, nil
	// }

	msg := appenEntriesMsg{
		msg:   in,
		reply: make(chan *pb.AppendEntriesResponse),
	}

	s.appendChan <- msg

	return <-msg.reply, nil
}

type rpcResponse struct {
	term     int64
	response bool
}

// sendRequestVote sends a request vote to a node.
func (s *Server) sendRequestVote(ctx context.Context, node int, req *pb.VoteRequest) (*rpcResponse, error) {
	client := pb.NewVoteClient(s.GetPeersConn()[node])
	resp, err := client.RequestVote(ctx, req)
	if err != nil {
		st, ok := status.FromError(err)
		if !ok {
			return nil, fmt.Errorf("failed to send request vote to node %d: %w", node, err)
		}
		return nil, &errors.Error{errors.Code(st.Code()), err}
	}
	return &rpcResponse{resp.GetTerm(), resp.GetVoteGranted()}, nil
}

// sendRequestVote sends a request vote to a node.
func (s *Server) sendAppendEntries(ctx context.Context, node int, req *pb.AppendEntriesRequest) (*rpcResponse, error) {
	client := pb.NewAppendEntriesClient(s.GetPeersConn()[node])
	resp, err := client.AppendEntries(ctx, req)
	if err != nil {
		st, ok := status.FromError(err)
		if !ok {
			return nil, fmt.Errorf("failed to send append entries to node %d: %w", node, err)
		}
		return nil, &errors.Error{StatusCode: errors.Code(st.Code()), Err: err}
	}

	return &rpcResponse{resp.GetTerm(), resp.GetSuccess()}, nil
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
