package raft

//
// Raft tests.
//
// we will use the original test_test.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"log/slog"

	pb "github.com/souleb/raft/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	_ "google.golang.org/grpc/health"
)

const RaftElectionTimeout = 350 * time.Millisecond

func TestRaftNode_Election(t *testing.T) {
	nodeCount := 3
	ctx := context.Background()
	ports, err := getFreePorts(nodeCount)
	require.NoError(t, err)
	nodes, err := makeNodes(nodeCount, ports, logger)
	require.NoError(t, err)

	// start the nodes
	for _, node := range nodes {
		go func(node *RaftNode) {
			err := node.Run(ctx, false)
			require.NoError(t, err)
		}(node)
	}

	// check that there is a leader
	_, err = checkLeaderIsElected(nodes)
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)
	term1, err := checkTerms(nodes)
	require.NoError(t, err)

	time.Sleep(2 * RaftElectionTimeout)
	term2, err := checkTerms(nodes)
	require.NoError(t, err)

	// check that terms doesn't change
	require.Equal(t, term1, term2)

	_, err = checkLeaderIsElected(nodes)
	require.NoError(t, err)

	// stop the nodes
	for _, node := range nodes {
		err := node.Stop()
		require.NoError(t, err)
	}
}

func TestRaftNode_ReElection(t *testing.T) {
	nodeCount := 3
	ctx := context.Background()
	ports, err := getFreePorts(nodeCount)
	require.NoError(t, err)
	nodes, err := makeNodes(nodeCount, ports, logger)
	require.NoError(t, err)

	// start the nodes
	for _, node := range nodes {
		go func(node *RaftNode) {
			err := node.Run(ctx, false)
			require.NoError(t, err)
		}(node)
	}

	// check that there is a leader
	leader1, err := checkLeaderIsElected(nodes)
	require.NoError(t, err)

	// if the leader disconnects, a new one should be elected.
	err = stopNode(nodes, leader1)
	require.NoError(t, err)
	leader2, err := checkLeaderIsElected(nodes)
	require.NoError(t, err)

	// if the old leader rejoins, that shouldn't
	// disturb the new leader. and the old leader
	// should switch to follower.
	err = startNode(ctx, nodes, leader1)
	require.NoError(t, err)
	newLeader, err := checkLeaderIsElected(nodes)
	require.NoError(t, err)
	assert.Equal(t, leader2, newLeader, "leader should not change")

	// if there's no quorum, no new leader should
	// be elected.
	err = stopNode(nodes, leader2)
	require.NoError(t, err)
	err = stopNextNode(nodes, leader2)
	require.NoError(t, err)
	time.Sleep(2 * RaftElectionTimeout)

	// check that the one connected server
	// does not think it is the leader.
	err = checkNoLeader(nodes)
	require.NoError(t, err)

	// if a quorum arises, it should elect a leader.
	err = startNextNode(ctx, nodes, leader2)
	require.NoError(t, err)
	_, err = checkLeaderIsElected(nodes)
	require.NoError(t, err)

	// re-join of last node shouldn't prevent leader from existing.
	err = startNode(ctx, nodes, leader2)
	require.NoError(t, err)
	_, err = checkLeaderIsElected(nodes)
	require.NoError(t, err)

	// stop the nodes
	for _, node := range nodes {
		err := node.Stop()
		require.NoError(t, err)
	}
}

func TestRaftNode_MultipleElection(t *testing.T) {
	nodeCount := 7
	ctx := context.Background()
	ports, err := getFreePorts(nodeCount)
	require.NoError(t, err)
	nodes, err := makeNodes(nodeCount, ports, logger)
	require.NoError(t, err)

	// start the nodes
	for _, node := range nodes {
		go func(node *RaftNode) {
			err := node.Run(ctx, false)
			require.NoError(t, err)
		}(node)
	}

	// check that there is a leader
	_, err = checkLeaderIsElected(nodes)
	require.NoError(t, err)

	for i := 1; i < 10; i++ {
		logger.Info("election round", slog.Int("round", i))
		// disconnect three nodes
		perms := rand.Perm(nodeCount)[0:3]
		i1 := int(nodes[perms[0]].GetID())
		i2 := int(nodes[perms[1]].GetID())
		i3 := int(nodes[perms[2]].GetID())
		logger.Info("disconnecting nodes", slog.Int("node1", i1), slog.Int("node2", i2), slog.Int("node3", i3))
		err = stopNode(nodes, i1)
		require.NoError(t, err)
		err = stopNode(nodes, i2)
		require.NoError(t, err)
		err = stopNode(nodes, i3)
		require.NoError(t, err)

		// either the current leader should still be alive,
		// or the remaining four should elect a new one.
		_, err = checkLeaderIsElected(nodes)
		require.NoError(t, err)

		err = startNode(ctx, nodes, i1)
		require.NoError(t, err)
		err = startNode(ctx, nodes, i2)
		require.NoError(t, err)
		err = startNode(ctx, nodes, i3)
		require.NoError(t, err)

		_, err = checkLeaderIsElected(nodes)
		require.NoError(t, err)
	}

	for _, node := range nodes {
		go func(node *RaftNode) {
			err := node.Stop()
			require.NoError(t, err)
		}(node)
	}
}

func TestRaftNode_LogReplication(t *testing.T) {
	nodeCount := 3
	ctx := context.Background()
	ports, err := getFreePorts(nodeCount)
	require.NoError(t, err)
	nodes, err := makeNodes(nodeCount, ports, logger)
	require.NoError(t, err)

	// start the nodes
	for _, node := range nodes {
		go func(node *RaftNode) {
			err := node.Run(ctx, false)
			require.NoError(t, err)
		}(node)
	}

	// set nodeAddrs
	for _, port := range ports {
		nodeAddrs = append(nodeAddrs, fmt.Sprintf("localhost:%d", port))
	}

	// check that there is a leader
	_, err = checkLeaderIsElected(nodes)
	require.NoError(t, err)

	// get a connection to the leader
	c := `{"healthCheckConfig": {"serviceName": "quis.RaftLeader"}, "loadBalancingConfig": [ { "round_robin": {} } ]}`
	target := fmt.Sprintf("%s:///%s", nodeScheme, nodeServiceName)
	conn, err := grpc.Dial(target, grpc.WithDefaultServiceConfig(c),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)))

	require.NoError(t, err)
	defer conn.Close()

	// create a client and apply an entry
	client := pb.NewApplyEntryClient(conn)
	_, err = client.ApplyEntry(ctx, &pb.ApplyRequest{Entry: []byte("hello"), SerialNumber: 1})
	require.NoError(t, err)

	// stop the nodes
	for _, node := range nodes {
		err := node.Stop()
		require.NoError(t, err)
	}
}
