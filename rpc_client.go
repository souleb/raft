package raft

import (
	"context"
	"fmt"

	pb "github.com/souleb/raft/api"
	"github.com/souleb/raft/errors"
	"google.golang.org/grpc/status"
)

type rpcResponse struct {
	term     int64
	response bool
}

// sendRequestVote sends a request vote to a node.
func (r *RaftNode) sendRequestVote(ctx context.Context, node int, req *pb.VoteRequest) (*rpcResponse, error) {
	client := pb.NewVoteClient(r.GetPeersConn()[node])
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
func (r *RaftNode) sendAppendEntries(ctx context.Context, node int, req *pb.AppendEntriesRequest) (*rpcResponse, error) {
	client := pb.NewAppendEntriesClient(r.GetPeersConn()[node])
	resp, err := client.AppendEntries(ctx, req)
	if err != nil {
		st, ok := status.FromError(err)
		if !ok {
			return nil, fmt.Errorf("failed to send append entries to node %d: %w", node, err)
		}
		return nil, &errors.Error{errors.Code(st.Code()), err}
	}

	return &rpcResponse{resp.GetTerm(), resp.GetSuccess()}, nil
}
