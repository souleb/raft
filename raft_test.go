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
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slog"
)

const RaftElectionTimeout = 350 * time.Millisecond

func TestRaftNode(t *testing.T) {
	opts := slog.HandlerOptions{
		Level: slog.LevelDebug,
	}
	textHandler := opts.NewTextHandler(os.Stdout)
	logger := slog.New(textHandler)

	ctx, cancel := context.WithCancel(context.Background())
	nodes, err := makeNodes(ctx, 3, logger)
	require.NoError(t, err)

	// stopChan := make(chan struct{})
	// for _, node := range nodes {
	// 	go func(node *RaftNode) {
	// 		err := makeServer(node, stopChan)
	// 		require.NoError(t, err)
	// 	}(node)
	// }
	// defer close(stopChan)

	// start the nodes
	for _, node := range nodes {
		go func(node *RaftNode) {
			err := node.Run(ctx, true)
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

	require.Equal(t, term1, term2)

	_, err = checkLeaderIsElected(nodes)
	require.NoError(t, err)

	// stop the nodes
	for _, node := range nodes {
		err := node.Stop(cancel)
		require.NoError(t, err)
	}
}

func checkTerms(nodes []*RaftNode) (int, error) {
	// adapted from mit/6.5840
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

func checkLeaderIsElected(nodes []*RaftNode) (int, error) {
	// adapted from mit/6.5840
	for i := 0; i < 10; i++ {
		ms := randomWaitTime(defaultMinElectionTimeout, defaultMaxElectionTimeout)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		leadersMap := make(map[int][]int)
		for _, node := range nodes {
			if term, leader := node.GetState(); leader {
				leadersMap[int(term)] = append(leadersMap[int(term)], int(node.GetID()))
			}
		}

		lastTermWithLeader := -1
		for term, leaders := range leadersMap {
			if len(leaders) > 1 {
				return -1, fmt.Errorf("got more than one leader")
			}
			if int(term) > lastTermWithLeader {
				lastTermWithLeader = int(term)
			}
		}

		if len(leadersMap) != 0 {
			return leadersMap[lastTermWithLeader][0], nil
		}
	}
	return -1, fmt.Errorf("1 leader was expected")
}
