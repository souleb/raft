package server

import (
	"context"
	"fmt"

	pb "github.com/souleb/raft/api"
	"github.com/souleb/raft/errors"
	"github.com/souleb/raft/log"

	"google.golang.org/grpc/status"
)

type SendAppendEntriesFunc func(term int64, leaderId int32, prevLogIndex int64, prevLogTerm int64, entries []byte, leaderCommit int64, responseChan chan RPCResponse)
type SendVoteRequestFunc func(term int64, candidateID int32, lastLogIndex int64, lastLogTerm int64, responseChan chan RPCResponse)

type AppendEntries struct {
	Term         int64
	LeaderId     int32
	PrevLogIndex int64
	PrevLogTerm  int64
	Entries      log.LogEntries
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

type ApplyRequest struct {
	Sn           int64
	Command      []byte
	ResponseChan chan RPCResponse
}

// requestVoteMsg is a message sent to the raft node to request a vote.
type RPCResponse struct {
	Term     int64
	Response bool
}

// RequestVote is called by candidates to gather votes.
func (s *RPCServer) RequestVote(ctx context.Context, in *pb.VoteRequest) (*pb.VoteResponse, error) {
	term, _ := s.getStateFunc()
	if in.GetTerm() < term {
		return &pb.VoteResponse{
			Term:        term,
			VoteGranted: false,
		}, nil
	}

	reply := make(chan RPCResponse)
	s.voteRPCChan <- VoteRequest{
		Term:         in.GetTerm(),
		CandidateId:  in.GetCandidateId(),
		LastLogIndex: in.GetLastLogIndex(),
		LastLogTerm:  in.GetLastLogTerm(),
		ResponseChan: reply,
	}

	response := <-reply

	return &pb.VoteResponse{Term: response.Term, VoteGranted: response.Response}, nil
}

func (s *RPCServer) AppendEntries(ctx context.Context, in *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	term, _ := s.getStateFunc()
	if in.GetTerm() < term {
		return &pb.AppendEntriesResponse{
			Term:    term,
			Success: false,
		}, nil
	}

	reply := make(chan RPCResponse)
	r := AppendEntries{
		Term:         in.GetTerm(),
		LeaderId:     in.GetLeaderId(),
		PrevLogIndex: in.GetPrevLogIndex(),
		PrevLogTerm:  in.GetPrevLogTerm(),
		Entries:      make(log.LogEntries, 0, len(in.GetEntries())),
		LeaderCommit: in.GetLeaderCommit(),
		ResponseChan: reply,
	}

	for _, entry := range in.GetEntries() {
		r.Entries = append(r.Entries, log.LogEntry{
			Term:    entry.GetTerm(),
			Command: entry.GetCommand(),
			Sn:      entry.GetSerialNumber(),
		})
	}

	s.appendEntriesRPCChan <- r

	response := <-reply

	return &pb.AppendEntriesResponse{Term: response.Term, Success: response.Response}, nil
}

// SendRequestVote sends a request vote to a node.
func (s *RPCServer) SendRequestVote(ctx context.Context, node int, req VoteRequest) (*RPCResponse, error) {
	if s.isPeerDead(node) {
		return nil, fmt.Errorf("cannot send request vote to node %d: node is dead", node)
	}
	client := pb.NewVoteClient(s.GetPeerConn(node))
	resp, err := client.RequestVote(ctx, &pb.VoteRequest{
		Term:         req.Term,
		CandidateId:  req.CandidateId,
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	})
	if err != nil {
		// mark the conn as dead
		s.setPeerDead(node)
		st, ok := status.FromError(err)
		if !ok {
			return nil, fmt.Errorf("failed to send request vote to node %d: %w", node, err)
		}
		return nil, &errors.Error{StatusCode: errors.Code(st.Code()), Err: err}
	}
	return &RPCResponse{resp.GetTerm(), resp.GetVoteGranted()}, nil
}

// sendRequestVote sends a request vote to a node.
func (s *RPCServer) SendAppendEntries(ctx context.Context, node int, req AppendEntries) (*RPCResponse, error) {
	if s.isPeerDead(node) {
		return nil, fmt.Errorf("cannot send append entries to node %d: node is dead", node)
	}
	client := pb.NewAppendEntriesClient(s.GetPeerConn(node))
	r := &pb.AppendEntriesRequest{
		Term:         req.Term,
		LeaderId:     req.LeaderId,
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      make([]*pb.Entry, 0, len(req.Entries)),
		LeaderCommit: req.LeaderCommit,
	}
	for _, entry := range req.Entries {
		r.Entries = append(r.Entries, &pb.Entry{
			Term:         entry.Term,
			Command:      entry.Command,
			SerialNumber: entry.Sn,
		})
	}
	resp, err := client.AppendEntries(ctx, r)
	if err != nil {
		// mark the conn as dead
		s.setPeerDead(node)
		st, ok := status.FromError(err)
		if !ok {
			return nil, fmt.Errorf("failed to send append entries to node %d: %w", node, err)
		}
		return nil, &errors.Error{StatusCode: errors.Code(st.Code()), Err: err}
	}

	return &RPCResponse{resp.GetTerm(), resp.GetSuccess()}, nil
}

// RequestVote is called by candidates to gather votes.
func (s *RPCServer) ApplyEntry(ctx context.Context, in *pb.ApplyRequest) (*pb.ApplyResponse, error) {
	reply := make(chan RPCResponse)
	s.applyEntryRPCChan <- ApplyRequest{
		Sn:           in.GetSerialNumber(),
		Command:      in.GetEntry(),
		ResponseChan: reply,
	}

	response := <-reply

	return &pb.ApplyResponse{Term: response.Term, Success: response.Response}, nil
}
