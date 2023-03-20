package raft

import (
	"context"
	"fmt"
	"net"

	"github.com/phayes/freeport"
	pb "github.com/souleb/raft/api"
	"golang.org/x/exp/slog"
	"google.golang.org/grpc"
)

func makeServer(node *RaftNode, stopChan chan struct{}) error {
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", node.GetID()))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterAppendEntriesServer(grpcServer, node)
	pb.RegisterVoteServer(grpcServer, node)
	go grpcServer.Serve(lis)
	<-stopChan
	grpcServer.Stop()
	return nil
}

func nodeSetup(ctx context.Context, peers map[int]string, id int32, logger *slog.Logger) (*RaftNode, error) {
	node, err := New(ctx, peers, id, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create a new node: %w", err)
	}
	return node, nil
}

func makeNodes(ctx context.Context, n int, logger *slog.Logger) ([]*RaftNode, error) {
	logger.Info("making nodes", slog.Int("nodes-number", n))
	var nodes []*RaftNode
	// get free ports
	ports, err := getFreePorts(n)
	if err != nil {
		return nil, err
	}

	// make n nodes
	for i := 0; i < n; i++ {
		peers := make(map[int]string)
		for j := 0; j < n; j++ {
			if i != j {
				peers[j] = fmt.Sprintf("localhost:%d", ports[j])
			}
		}
		n, err := nodeSetup(ctx, peers, int32(ports[i]), logger)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, n)
	}

	return nodes, nil
}

func getFreePorts(n int) ([]int, error) {
	var ports []int
	for i := 0; i < n; i++ {
		port, err := freeport.GetFreePort()
		if err != nil {
			return nil, err
		}
		ports = append(ports, port)
	}
	return ports, nil
}
