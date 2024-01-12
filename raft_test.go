package raft

//
// Raft tests.
//
// we will use the original test_test.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import (
	"bytes"
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
	err = stopNode(leader1)
	require.NoError(t, err)
	leader2, err := checkLeaderIsElected(nodes)
	require.NoError(t, err)

	// if the old leader rejoins, that shouldn't
	// disturb the new leader. and the old leader
	// should switch to follower.
	err = startNode(ctx, nodes, int(leader1.GetID()))
	require.NoError(t, err)
	newLeader, err := checkLeaderIsElected(nodes)
	require.NoError(t, err)
	assert.Equal(t, leader2, newLeader, "leader should not change")

	// if there's no quorum, no new leader should
	// be elected.
	err = stopNode(leader2)
	require.NoError(t, err)
	_, err = stopNextNode(nodes, int(leader2.GetID()))
	require.NoError(t, err)
	time.Sleep(2 * RaftElectionTimeout)

	// check that the one connected server
	// does not think it is the leader.
	err = checkNoLeader(nodes)
	require.NoError(t, err)

	// if a quorum arises, it should elect a leader.
	err = startNextNode(ctx, nodes, int(leader2.GetID()))
	require.NoError(t, err)
	_, err = checkLeaderIsElected(nodes)
	require.NoError(t, err)

	// re-join of last node shouldn't prevent leader from existing.
	err = startNode(ctx, nodes, int(leader2.GetID()))
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
		err = stopNode(nodes[perms[0]])
		require.NoError(t, err)
		err = stopNode(nodes[perms[1]])
		require.NoError(t, err)
		err = stopNode(nodes[perms[2]])
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

func TestRaftNode_BasicLogReplication(t *testing.T) {
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
	sn := uint64(1)
	client := pb.NewApplyEntryClient(conn)
	_, err = client.ApplyEntry(ctx, &pb.ApplyRequest{Entry: []byte("hello"), SerialNumber: int64(sn)})
	require.NoError(t, err)

	// check that the entry was committed
	ok, err := checkValueIsCommitted(sn, nodes, len(nodes), []byte("hello"), 0)
	require.NoError(t, err)
	require.True(t, ok)

	// stop the nodes
	for _, node := range nodes {
		err := node.Stop()
		require.NoError(t, err)
	}
}

func TestRaftNode_LogReplicationWithFollowerFailure(t *testing.T) {
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
	leader, err := checkLeaderIsElected(nodes)
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
	sn := uint64(1)
	client := pb.NewApplyEntryClient(conn)
	_, err = client.ApplyEntry(ctx, &pb.ApplyRequest{Entry: []byte("hello"), SerialNumber: int64(sn)})
	require.NoError(t, err)

	// check that the entry was committed
	ok, err := checkValueIsCommitted(sn, nodes, len(nodes), []byte("hello"), 0)
	require.NoError(t, err)
	require.True(t, ok)

	// disconnect one follower from the network.
	follower, err := stopNextNode(nodes, int(leader.GetID()))
	require.NoError(t, err)

	n2 := removeNode(nodes, follower)

	// the leader and remaining follower should be
	// able to agree despite the disconnected follower.
	sn = uint64(2)
	_, err = client.ApplyEntry(ctx, &pb.ApplyRequest{Entry: []byte("hello2"), SerialNumber: int64(sn)})
	require.NoError(t, err)

	// check that the entry was committed
	ok, err = checkValueIsCommitted(sn, nodes, len(nodes)-1, []byte("hello2"), 0)
	require.NoError(t, err)
	require.True(t, ok)

	time.Sleep(RaftElectionTimeout)

	sn = uint64(3)
	_, err = client.ApplyEntry(ctx, &pb.ApplyRequest{Entry: []byte("hello3"), SerialNumber: int64(sn)})
	require.NoError(t, err)

	// check that the entry was committed
	ok, err = checkValueIsCommitted(sn, nodes, len(nodes)-1, []byte("hello3"), 0)
	require.NoError(t, err)
	require.True(t, ok)

	// stop remaining follower
	_, err = stopNextNode(n2, int(leader.GetID()))
	require.NoError(t, err)

	// submit a command to the leader.
	sn = uint64(4)
	response, err := client.ApplyEntry(ctx, &pb.ApplyRequest{Entry: []byte("hello4"), SerialNumber: int64(sn)})
	require.NoError(t, err)
	require.False(t, response.Success)

	// check that the entry was not committed
	ok, err = checkValueIsCommitted(sn, nodes, 1, []byte("hello4"), 0)
	require.NoError(t, err)
	require.False(t, ok)

	err = leader.Stop()
	require.NoError(t, err)
}

func TestRaftNode_LogReplicationWithFollowerReconnect(t *testing.T) {
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
	leader, err := checkLeaderIsElected(nodes)
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
	sn := uint64(1)
	client := pb.NewApplyEntryClient(conn)
	_, err = client.ApplyEntry(ctx, &pb.ApplyRequest{Entry: []byte("hello"), SerialNumber: int64(sn)})
	require.NoError(t, err)

	// check that the entry was committed
	ok, err := checkValueIsCommitted(sn, nodes, len(nodes), []byte("hello"), 0)
	require.NoError(t, err)
	require.True(t, ok)

	// disconnect one follower from the network.
	follower, err := stopNextNode(nodes, int(leader.GetID()))
	require.NoError(t, err)

	// the leader and remaining follower should be
	// able to agree despite the disconnected follower.
	sn = uint64(2)
	_, err = client.ApplyEntry(ctx, &pb.ApplyRequest{Entry: []byte("hello2"), SerialNumber: int64(sn)})
	require.NoError(t, err)

	// check that the entry was committed
	ok, err = checkValueIsCommitted(sn, nodes, len(nodes)-1, []byte("hello2"), 0)
	require.NoError(t, err)
	require.True(t, ok)

	time.Sleep(RaftElectionTimeout)

	sn = uint64(3)
	_, err = client.ApplyEntry(ctx, &pb.ApplyRequest{Entry: []byte("hello3"), SerialNumber: int64(sn)})
	require.NoError(t, err)

	// check that the entry was committed
	ok, err = checkValueIsCommitted(sn, nodes, len(nodes)-1, []byte("hello3"), 0)
	require.NoError(t, err)
	require.True(t, ok)

	// reconnect follower
	err = startNode(ctx, nodes, int(follower.GetID()))
	require.NoError(t, err)

	// submit a command to the leader.
	sn = uint64(4)
	_, err = client.ApplyEntry(ctx, &pb.ApplyRequest{Entry: []byte("hello4"), SerialNumber: int64(sn)})
	require.NoError(t, err)

	time.Sleep(RaftElectionTimeout)

	// submit a command to the leader.
	sn = uint64(5)
	_, err = client.ApplyEntry(ctx, &pb.ApplyRequest{Entry: []byte("hello5"), SerialNumber: int64(sn)})
	require.NoError(t, err)

	// check that the entry was committed
	ok, err = checkValueIsCommitted(sn, nodes, len(nodes), []byte("hello5"), 0)
	require.NoError(t, err)
	require.True(t, ok)

	// stop nodes
	for _, node := range nodes {
		err := node.Stop()
		require.NoError(t, err)
	}
}

func TestRaftNode_LogReplicationWithLeaderFailure(t *testing.T) {
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
	leader, err := checkLeaderIsElected(nodes)
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
	sn := uint64(1)
	client := pb.NewApplyEntryClient(conn)
	_, err = client.ApplyEntry(ctx, &pb.ApplyRequest{Entry: []byte("hello"), SerialNumber: int64(sn)})
	require.NoError(t, err)

	// check that the entry was committed
	ok, err := checkValueIsCommitted(sn, nodes, len(nodes), []byte("hello"), 0)
	require.NoError(t, err)
	require.True(t, ok)

	// disconnect the leader from the network.
	err = stopNode(leader)
	require.NoError(t, err)

	n2 := removeNode(nodes, leader)

	// a new leader should be elected.
	_, err = checkLeaderIsElected(nodes)
	require.NoError(t, err)

	// the leader and remaining follower should be
	// able to agree despite the disconnected server.
	sn = uint64(2)
	_, err = client.ApplyEntry(ctx, &pb.ApplyRequest{Entry: []byte("hello2"), SerialNumber: int64(sn)})
	require.NoError(t, err)

	// check that the entry was committed
	ok, err = checkValueIsCommitted(sn, n2, len(nodes)-1, []byte("hello2"), 0)
	require.NoError(t, err)
	require.True(t, ok)

	time.Sleep(RaftElectionTimeout)

	sn = uint64(3)
	_, err = client.ApplyEntry(ctx, &pb.ApplyRequest{Entry: []byte("hello3"), SerialNumber: int64(sn)})
	require.NoError(t, err)

	// check that the entry was committed
	ok, err = checkValueIsCommitted(sn, nodes, len(nodes)-1, []byte("hello3"), 0)
	require.NoError(t, err)
	require.True(t, ok)

	// stop nodes
	for _, node := range n2 {
		err := node.Stop()
		require.NoError(t, err)
	}
}

func TestRaftNode_LogReplicationNotEnoughFollowers(t *testing.T) {
	nodeCount := 5
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
	leader, err := checkLeaderIsElected(nodes)
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
	sn := uint64(1)
	client := pb.NewApplyEntryClient(conn)
	_, err = client.ApplyEntry(ctx, &pb.ApplyRequest{Entry: []byte("hello"), SerialNumber: int64(sn)})
	require.NoError(t, err)

	// check that the entry was committed
	ok, err := checkValueIsCommitted(sn, nodes, len(nodes), []byte("hello"), 0)
	require.NoError(t, err)
	require.True(t, ok)

	// disconnect 3 followers from the network.
	follower1, err := stopNextNode(nodes, int(leader.GetID()))
	require.NoError(t, err)
	follower2, err := stopNextNode(nodes, int(follower1.GetID()))
	require.NoError(t, err)
	follower3, err := stopNextNode(nodes, int(follower2.GetID()))
	require.NoError(t, err)

	// the leader should reject the command.
	sn = uint64(2)
	_, err = client.ApplyEntry(ctx, &pb.ApplyRequest{Entry: []byte("hello2"), SerialNumber: int64(sn)})
	require.NoError(t, err)

	time.Sleep(RaftElectionTimeout)

	// check that the entry was committed
	ok, err = checkValueIsCommitted(sn, nodes, len(nodes)-3, []byte("hello2"), 0)
	require.NoError(t, err)
	require.False(t, ok)

	// reconnect followers
	err = startNode(ctx, nodes, int(follower1.GetID()))
	require.NoError(t, err)

	err = startNode(ctx, nodes, int(follower2.GetID()))
	require.NoError(t, err)

	err = startNode(ctx, nodes, int(follower3.GetID()))
	require.NoError(t, err)

	time.Sleep(RaftElectionTimeout)

	sn = uint64(3)
	_, err = client.ApplyEntry(ctx, &pb.ApplyRequest{Entry: []byte("hello3"), SerialNumber: int64(sn)})
	require.NoError(t, err)

	// check that the entry was committed
	ok, err = checkValueIsCommitted(sn, nodes, len(nodes)-1, []byte("hello3"), 0)
	require.NoError(t, err)
	require.True(t, ok)

	// stop nodes
	for _, node := range nodes {
		err := node.Stop()
		require.NoError(t, err)
	}
}

func TestRaftNode_Persist(t *testing.T) {
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
	sn := uint64(1)
	client := pb.NewApplyEntryClient(conn)
	_, err = client.ApplyEntry(ctx, &pb.ApplyRequest{Entry: []byte("hello"), SerialNumber: int64(sn)})
	require.NoError(t, err)

	// check that the entry was committed
	ok, err := checkValueIsCommitted(sn, nodes, len(nodes), []byte("hello"), 0)
	require.NoError(t, err)
	require.True(t, ok)

	// stop nodes
	for _, node := range nodes {
		err := node.Stop()
		require.NoError(t, err)
	}

	// restart nodes
	for _, node := range nodes {
		go func(node *RaftNode) {
			err := node.Run(ctx, false)
			require.NoError(t, err)
		}(node)
	}
	//time.Sleep(2 * RaftElectionTimeout)

	// check that there is a leader
	_, err = checkLeaderIsElected(nodes)
	require.NoError(t, err)

	// get a connection to the leader
	conn, err = grpc.Dial(target, grpc.WithDefaultServiceConfig(c),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)))

	require.NoError(t, err)
	defer conn.Close()

	// create a client and apply an entry
	sn = uint64(2)
	client = pb.NewApplyEntryClient(conn)
	_, err = client.ApplyEntry(ctx, &pb.ApplyRequest{Entry: []byte("hello2"), SerialNumber: int64(sn)})
	require.NoError(t, err)

	// check that the entry was committed
	ok, err = checkValueIsCommitted(sn, nodes, len(nodes), []byte("hello2"), 0)
	require.NoError(t, err)
	require.True(t, ok)

	// stop nodes
	for _, node := range nodes {
		err := node.Stop()
		require.NoError(t, err)
	}
}

func checkValueIsCommitted(index uint64, nodes []*RaftNode, expectedNodes int, value []byte, retry int) (bool, error) {
	// somebody claimed to be the leader and to have
	// submitted our command; wait a while for agreement.
	retry += 1
	for retry > 0 {
		t1 := time.Now()
		for time.Since(t1).Seconds() < 2 {
			nd, cmd1, err := nCommitted(index, nodes)
			if err != nil {
				return false, err
			}
			if nd > 0 && nd >= expectedNodes {
				// committed
				if bytes.Equal(cmd1, value) {
					// and it was the command we submitted.
					return true, nil
				}
			}
			time.Sleep(20 * time.Millisecond)
		}
		retry--
	}

	return false, nil
}

func nCommitted(index uint64, nodes []*RaftNode) (int, []byte, error) {
	count := 0
	var cmd []byte = nil

	for _, node := range nodes {
		if node == nil {
			continue
		}
		if index == node.state.getCommitIndex() {
			if cmd1 := node.GetLogByIndex(index); cmd1.Command != nil {
				if count > 0 && !bytes.Equal(cmd, cmd1.Command) {
					return -1, nil, fmt.Errorf("committed values do not match: index %v, %s, %s",
						index, string(cmd), string(cmd1.Command))
				}
				count += 1
				cmd = cmd1.Command
			}
		}
	}
	return count, cmd, nil
}

func removeNode(nodes []*RaftNode, node *RaftNode) []*RaftNode {
	var newNodes []*RaftNode
	for _, n := range nodes {
		if n != node {
			newNodes = append(newNodes, n)
		}
	}
	return newNodes
}
