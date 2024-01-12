package raft

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"log/slog"

	"github.com/phayes/freeport"
	"github.com/souleb/raft/storage"
	"google.golang.org/grpc/resolver"
)

const (
	nodeScheme      = "raft"
	nodeServiceName = "nodes.raft.grpc.io"
)

var (
	logger    *slog.Logger
	nodeAddrs = []string{}
)

func TestMain(m *testing.M) {
	opts := &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}
	textHandler := slog.NewTextHandler(os.Stderr, opts)
	logger = slog.New(textHandler)
	os.Exit(m.Run())
}

func nodeSetup(peers map[uint]string, id int32, store storage.Store, logger *slog.Logger) (*RaftNode, error) {
	node, err := New(peers, id, uint16(id), store, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create a new node: %w", err)
	}
	return node, nil
}

func makeNodes(n int, ports []int, logger *slog.Logger) ([]*RaftNode, error) {
	logger.Info("making nodes", slog.Int("nodes-number", n))
	var nodes []*RaftNode

	// make n nodes
	for i := 0; i < n; i++ {
		peers := make(map[uint]string)
		for j := 0; j < n; j++ {
			if j != i {
				peers[uint(ports[j])] = fmt.Sprintf("localhost:%d", ports[j])
			}
		}
		store, err := storage.NewStore(":memory:", storage.Low, logger)
		if err != nil {
			return nil, err
		}
		n, err := nodeSetup(peers, int32(ports[i]), store, logger)
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

func checkTerms(nodes []*RaftNode) (int, error) {
	term := -1
	for _, node := range nodes {
		t, _ := node.GetState()
		if term == -1 {
			term = int(t)
		} else if term != int(t) {
			return -1, fmt.Errorf("nodes disagree on terms")
		}
	}
	return term, nil
}

func checkNoLeader(nodes []*RaftNode) error {
	for _, node := range nodes {
		_, leader := node.GetState()
		if leader {
			return fmt.Errorf("expected no leader")
		}
	}
	return nil
}

func checkLeaderIsElected(nodes []*RaftNode) (*RaftNode, error) {
	for i := 0; i < 10; i++ {
		ms := randomWaitTime(defaultMaxElectionTimeout, defaultMaxElectionTimeout+50)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		leadersMap := make(map[int][]*RaftNode)
		for _, node := range nodes {
			if term, leader := node.GetState(); leader {
				leadersMap[int(term)] = append(leadersMap[int(term)], node)
			}
		}

		lastTermWithLeader := -1
		for term, leaders := range leadersMap {
			if len(leaders) > 1 {
				return nil, fmt.Errorf("got more than one leader")
			}
			if int(term) > lastTermWithLeader {
				lastTermWithLeader = int(term)
			}
		}

		if len(leadersMap) != 0 {
			return leadersMap[lastTermWithLeader][0], nil
		}
	}
	return nil, fmt.Errorf("1 leader was expected")
}

func stopNode(node *RaftNode) error {
	return node.Stop()
}

func stopNextNode(nodes []*RaftNode, node int) (*RaftNode, error) {
	nodeIndex := -1
	for i, n := range nodes {
		if int(n.GetID()) == node {
			nodeIndex = i
			break
		}
	}
	if nodeIndex == -1 {
		return nil, fmt.Errorf("cannot find node")
	}
	nextIndex := (nodeIndex + 1) % len(nodes)
	return nodes[nextIndex], nodes[nextIndex].Stop()
}

func startNode(ctx context.Context, nodes []*RaftNode, node int) error {
	for i, n := range nodes {
		if int(n.GetID()) == node {
			store, err := storage.NewStore(":memory:", storage.Low, logger)
			if err != nil {
				return err
			}
			newNode, err := nodeSetup(n.CopyPeers(), n.GetID(), store, logger)
			if err != nil {
				return err
			}
			// copy the state
			newNode.SetCurrentTerm(n.GetCurrentTerm())
			newNode.SetVotedFor(n.GetVotedFor())
			newNode.SetLastApplied(n.GetLastApplied())
			newNode.SetLog(n.GetLog())
			nodes[i] = newNode
			return newNode.Run(ctx, false)
		}
	}
	return fmt.Errorf("cannot find node")
}

func startNextNode(ctx context.Context, nodes []*RaftNode, node int) error {
	nodeIndex := -1
	for i, n := range nodes {
		if int(n.GetID()) == node {
			nodeIndex = i
			break
		}
	}
	if nodeIndex == -1 {
		return fmt.Errorf("cannot find node")
	}
	nextIndex := (nodeIndex + 1) % len(nodes)
	store, err := storage.NewStore(":memory:", storage.Low, logger)
	if err != nil {
		return err
	}
	newNode, err := nodeSetup(nodes[nextIndex].CopyPeers(), nodes[nextIndex].GetID(), store, logger)
	if err != nil {
		return err
	}
	// copy the state
	newNode.SetCurrentTerm(nodes[nextIndex].GetCurrentTerm())
	newNode.SetVotedFor(nodes[nextIndex].GetVotedFor())
	newNode.SetLog(nodes[nextIndex].GetLog())
	nodes[nextIndex] = newNode
	return nodes[nextIndex].Run(ctx, false)
}

type nodeResolverBuilder struct{}

func (*nodeResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	r := &nodeResolver{
		target: target,
		cc:     cc,
		addrsStore: map[string][]string{
			nodeServiceName: nodeAddrs,
		},
	}
	r.start()
	return r, nil
}
func (*nodeResolverBuilder) Scheme() string { return nodeScheme }

type nodeResolver struct {
	target     resolver.Target
	cc         resolver.ClientConn
	addrsStore map[string][]string
}

func (r *nodeResolver) start() {
	addrStrs := r.addrsStore[r.target.Endpoint()]
	addrs := make([]resolver.Address, len(addrStrs))
	for i, s := range addrStrs {
		addrs[i] = resolver.Address{Addr: s}
	}
	r.cc.UpdateState(resolver.State{Addresses: addrs})
}
func (*nodeResolver) ResolveNow(o resolver.ResolveNowOptions) {}
func (*nodeResolver) Close()                                  {}

func init() {
	resolver.Register(&nodeResolverBuilder{})
}
