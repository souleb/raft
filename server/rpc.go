package server

import (
	"context"
	"fmt"

	pb "github.com/souleb/raft/api"
	"github.com/souleb/raft/errors"
	"github.com/souleb/raft/log"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SnapshotRequest is a message sent to the raft node to install a snapshot.
type SnapshotRequest struct {
	// Term is the observed term of the leader.
	Term uint64
	// LeaderId is the ID of the leader.
	LeaderId int32
	// LastIncludedIndex is the index of the log entry immediately preceding the new ones.
	LastIncludedIndex uint64
	// LastIncludedTerm is the term of the log entry immediately preceding the new ones.
	LastIncludedTerm uint64
	// Data is the snapshot data.
	Data []byte
	// ResponseChan is the channel to send the response to.
	ResponseChan chan RPCResponse
}

// AppendEntries is a message sent to the a raft node to append entries to the log.
type AppendEntries struct {
	// Term is the observed term of the leader.
	Term uint64
	// LeaderId is the ID of the leader.
	LeaderId int32
	// PrevLogIndex is the index of the log entry immediately preceding the new ones.
	PrevLogIndex uint64
	// PrevLogTerm is the term of the log entry immediately preceding the new ones.
	PrevLogTerm uint64
	// Entries are the log entries to append.
	Entries []*log.LogEntry
	// LeaderCommit is the leader's commit index.
	LeaderCommit uint64
	// ResponseChan is the channel to send the response to.
	ResponseChan chan RPCResponse
}

// VoteRequest is a message sent to the raft node to request a vote.
type VoteRequest struct {
	// Term is the candidate's term.
	Term uint64
	// CandidateId is the candidate requesting the vote.
	CandidateId int32
	// LastLogIndex is the index of the candidate's last log entry.
	LastLogIndex uint64
	// LastLogTerm is the term of the candidate's last log entry.
	LastLogTerm uint64
	// ResponseChan is the channel to send the response to.
	ResponseChan chan RPCResponse
}

// ApplyRequest is a message sent to the raft node to apply a command.
type ApplyRequest struct {
	// Sn is the serial number of the command.
	Sn int64
	// Command is the command to apply.
	Command []byte
	// ResponseChan is the channel to send the response to.
	ResponseChan chan RPCResponse
}

// RPCResponse is a response to an RPC request.
type RPCResponse struct {
	// Term is the current term of the node.
	Term uint64
	// Response is the response to the request. It is true if the request was
	// accepted.
	Response bool

	// ConflictTerm is the term of the conflicting entry.
	ConflictTerm uint64
	// ConflictIdx is the index of the first entry with the conflicting term.
	ConflictIdx uint64
}

// RequestVote is called by candidates to gather votes. It receives a vote request
// and sends a response. If returns early if the request term is less than the
// current term.
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

// AppendEntries is called by leaders to replicate log entries. It receives an
// append entries request and sends a response. If returns early if the request
// term is less than the current term.
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
		Entries:      make([]*log.LogEntry, 0, len(in.GetEntries())),
		LeaderCommit: in.GetLeaderCommit(),
		ResponseChan: reply,
	}

	for _, entry := range in.GetEntries() {
		r.Entries = append(r.Entries, &log.LogEntry{
			Term:    entry.GetTerm(),
			Command: entry.GetCommand(),
			Sn:      entry.GetSerialNumber(),
		})
	}

	s.appendEntriesRPCChan <- r

	response := <-reply

	return &pb.AppendEntriesResponse{Term: response.Term, Success: response.Response}, nil
}

func (s *RPCServer) InstallSnapshot(ctx context.Context, in *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	term, _ := s.getStateFunc()
	if in.GetTerm() < term {
		return &pb.InstallSnapshotResponse{
			Term: term,
		}, nil
	}

	reply := make(chan RPCResponse)
	s.installSnapshotRPCChan <- SnapshotRequest{
		Term:              in.GetTerm(),
		LeaderId:          in.GetLeaderId(),
		LastIncludedIndex: in.GetLastIncludedIndex(),
		LastIncludedTerm:  in.GetLastIncludedTerm(),
		Data:              in.GetData(),
		ResponseChan:      reply,
	}

	response := <-reply

	return &pb.InstallSnapshotResponse{Term: response.Term}, nil

}

// SendRequestVote sends a request vote to a node. The node is identified by its
// ID. It returns an error if the node is not connected and marks the node as
// dead.
func (s *RPCServer) SendRequestVote(ctx context.Context, node uint, req VoteRequest) (*RPCResponse, error) {
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
	return &RPCResponse{Term: resp.GetTerm(), Response: resp.GetVoteGranted()}, nil
}

// SendAppendEntries sends an append entries to a node. The node is identified by
// its ID. It returns an error if the node is not connected and marks the node as
// dead.
func (s *RPCServer) SendAppendEntries(ctx context.Context, node uint, req AppendEntries) (*RPCResponse, error) {
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
		st, ok := status.FromError(err)
		if !ok {
			return nil, fmt.Errorf("failed to send append entries to node %d: %w", node, err)
		}

		if st.Code() == codes.Unavailable {
			// mark the conn as dead
			s.setPeerDead(node)
		}
		return nil, &errors.Error{StatusCode: errors.Code(st.Code()), Err: err}
	}

	return &RPCResponse{Term: resp.GetTerm(), Response: resp.GetSuccess()}, nil
}

func (s *RPCServer) SendInstallSnapshot(ctx context.Context, node uint, req SnapshotRequest) (*RPCResponse, error) {
	if s.isPeerDead(node) {
		return nil, fmt.Errorf("cannot send install snapshot to node %d: node is dead", node)
	}
	client := pb.NewInstallSnapshotClient(s.GetPeerConn(node))
	resp, err := client.InstallSnapshot(ctx, &pb.InstallSnapshotRequest{
		Term:              req.Term,
		LeaderId:          req.LeaderId,
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Data:              req.Data,
	})
	if err != nil {
		// mark the conn as dead
		s.setPeerDead(node)
		st, ok := status.FromError(err)
		if !ok {
			return nil, fmt.Errorf("failed to send install snapshot to node %d: %w", node, err)
		}
		return nil, &errors.Error{StatusCode: errors.Code(st.Code()), Err: err}
	}
	return &RPCResponse{Term: resp.GetTerm()}, nil
}

// ApplyEntry is called by clients to apply a command. It receives an apply entry
// request and sends a response. It blocks until the command is committed.
// If the node is not the leader, it returns an error.
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
