package raft

import (
	"context"

	pb "github.com/souleb/raft/api"
)

// requestVoteMsg is a message sent to the raft node to request a vote.
type requestVoteMsg struct {
	msg *pb.VoteRequest
	// reply is a channel to send the response to.
	reply chan *pb.VoteResponse
}

// RequestVote is called by candidates to gather votes.
func (r *RaftNode) RequestVote(ctx context.Context, in *pb.VoteRequest) (*pb.VoteResponse, error) {
	term := r.state.getCurrentTerm()
	if in.GetTerm() < term {
		return &pb.VoteResponse{
			Term:        term,
			VoteGranted: false,
		}, nil
	}

	msg := requestVoteMsg{
		msg:   in,
		reply: make(chan *pb.VoteResponse),
	}

	r.voteChan <- msg

	return <-msg.reply, nil
}

type appenEntriesMsg struct {
	msg   *pb.AppendEntriesRequest
	reply chan *pb.AppendEntriesResponse
}

func (r *RaftNode) AppendEntries(ctx context.Context, in *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	term := r.state.getCurrentTerm()
	if in.GetTerm() < term {
		return &pb.AppendEntriesResponse{
			Term:    term,
			Success: false,
		}, nil
	}

	msg := appenEntriesMsg{
		msg:   in,
		reply: make(chan *pb.AppendEntriesResponse),
	}

	r.appendChan <- msg

	return <-msg.reply, nil
}
