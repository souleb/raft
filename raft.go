package raft

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/souleb/raft/server"
	"github.com/souleb/raft/storage"
	"golang.org/x/exp/slog"
)

const (
	// defaultMinElectionTimeout is the minimum election timeout in milliseconds.
	defaultMinElectionTimeout = 150
	// defaultMaxElectionTimeout is the maximum election timeout in milliseconds.
	defaultMaxElectionTimeout = 300
	// defaultHeartbeatTimeout is the heartbeat timeout in milliseconds.
	defaultHeartbeatTimeout = 50
	// defaultTimeout is the timeout in milliseconds.
	defaultTimeout = 100
)

// OptionsFn is a function that sets an option.
type OptionsFn func(opt Options)

// // ApplyEntry is a command to be applied to the state machine.
// type ApplyEntry struct {
// 	// CommandValid is true if the command is valid.
// 	CommandValid bool
// 	// CommandIndex is the index of the command in the log.
// 	Command any
// 	// CommandIndex is the index of the command in the log.
// 	CommandIndex int
// }

// Options holds the configurable options for a RaftNode.
type Options struct {
	// electionTimeout(ms) is used to decide whether to start an election
	// timeout = fn(min, max) where fn is a function that return randon values
	// bounded by min and max
	electionTimeoutmin int
	electionTimeoutmax int
	// heartbeatTimeout(ms) is used to send heartbeat to other peers
	heartbeatTimeout int
	// timeout(ms) is used to set the dial timeout for connecting to peers.
	timeout int
}

// RaftNode is a member of the Raft cluster
type RaftNode struct {
	// Peers is a map of peer id to peer address.
	peers map[int]string
	// Peers talks over grpc.
	//peersConn map[int]*grpc.ClientConn
	// handle all the rpc comms
	RPCServer server.Server

	// persister handles this peer's persisted state
	persister *storage.Persister
	state     *state

	start     sync.Mutex
	startOnce sync.Once
	started   bool

	stop     sync.RWMutex
	stopOnce sync.Once
	stopped  bool

	// id is this peer's id
	id         int32
	appendChan chan server.AppendEntries
	voteChan   chan server.VoteRequest
	// errChan is a channel that receives errors from the RaftNode.
	errChan chan error
	// Lock to protect shared access to this peer's fields
	mu sync.RWMutex
	// logger is the logger used by the server.
	logger *slog.Logger
	Options
}

// WithElectionTimeout sets the election timeout for the RaftNode.
func WithElectionTimeout(min, max int) OptionsFn {
	return func(opts Options) {
		opts.electionTimeoutmin = min
		opts.electionTimeoutmax = max
	}
}

// WithHeartbeatTimeout sets the heartbeat timeout for the RaftNode.
func WithHeartbeatTimeout(timeout int) OptionsFn {
	return func(opts Options) {
		opts.heartbeatTimeout = timeout
	}
}

func WithTimeout(timeout int) OptionsFn {
	return func(opts Options) {
		opts.timeout = timeout
	}
}

// New creates a new RaftNode.
func New(ctx context.Context, peers map[int]string, id int32, port uint16, logger *slog.Logger, opts ...OptionsFn) (*RaftNode, error) {
	r := &RaftNode{
		peers:  peers,
		id:     id,
		logger: logger,
	}

	for _, opt := range opts {
		opt(r.Options)
	}

	if r.logger == nil {
		return nil, fmt.Errorf("an initialized logger must be provided")
	}

	if r.electionTimeoutmax == 0 {
		r.electionTimeoutmax = defaultMaxElectionTimeout
	}

	if r.electionTimeoutmin == 0 {
		r.electionTimeoutmin = defaultMinElectionTimeout
	}

	if r.heartbeatTimeout == 0 {
		r.heartbeatTimeout = defaultHeartbeatTimeout
	}

	if r.timeout == 0 {
		r.timeout = defaultTimeout
	}

	r.state = &state{
		votedFor: -1,
		log:      make([]logEntry, 0),
	}

	appendEntriesRPCChan := make(chan server.AppendEntries)
	voteRPCChan := make(chan server.VoteRequest)
	r.appendChan = appendEntriesRPCChan
	r.voteChan = voteRPCChan

	r.errChan = make(chan error)

	s, err := server.New(int(r.id), port,
		server.WithHeartbeatTimeout(r.heartbeatTimeout),
		server.WithLogger(r.logger),
		server.WithVoteRPCChan(r.voteChan),
		server.WithAppendEntryRPCChan(r.appendChan),
		server.WithGetCurrentTermFunc(r.getCurrentTermCallback()),
		server.WithTimeout(r.timeout),
	)

	if err != nil {
		return nil, err
	}

	r.RPCServer = s
	r.state.observers = append(r.state.observers, s)

	return r, nil
}

func (r *RaftNode) Run(ctx context.Context, testMode bool) error {
	var retErr error

	r.startOnce.Do(func() {
		r.logger.Info("starting raft node", slog.Int("id", int(r.GetID())))
		err := r.RPCServer.Run(ctx, r.peers, testMode)
		if err != nil {
			retErr = fmt.Errorf("error while connecting to peers: %w", err)
			return
		}

		go r.runStateMachine(ctx)
		r.start.Lock()
		r.started = true
		r.start.Unlock()
	})

	return retErr
}

// Stop tells the RaftNode to shut itself down.
func (r *RaftNode) Stop(cancel context.CancelFunc) error {
	var retErr error

	r.stopOnce.Do(func() {
		r.logger.Info("raft node is stopping", slog.Int("id", int(r.GetID())))
		r.stop.Lock()
		r.stopped = true
		r.stop.Unlock()
		cancel()
		retErr = <-r.errChan
		r.RPCServer.Stop()
		r.logger.Info("raft node is stopped", slog.Int("id", int(r.GetID())))
	})

	return retErr
}

// IsStopped returns true if the RaftNode has been killed.
func (r *RaftNode) IsStopped() bool {
	r.stop.Lock()
	defer r.stop.Unlock()
	return r.stopped
}

func (r *RaftNode) runStateMachine(ctx context.Context) {
	r.logger.Debug("starting raft node state machine for node", slog.Int("id", int(r.id)))
	state := r.follower(ctx)

	for {
		if state == nil {
			// no state transition means we are stopping
			return
		}
		state = state(ctx)
	}
}

func (r *RaftNode) GetID() int32 {
	return atomic.LoadInt32(&r.id)
}

func (r *RaftNode) IsLeader() bool {
	r.state.mu.Lock()
	defer r.state.mu.Unlock()
	return r.state.isLeader
}

// GetState returns the currentTerm and whether this server
// believes it is the leader.
func (r *RaftNode) GetState() (int64, bool) {
	r.state.mu.Lock()
	defer r.state.mu.Unlock()
	return r.state.currentTerm, r.state.isLeader
}

func (r *RaftNode) GetPeers() map[int]string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.peers
}

func (r *RaftNode) getElectionTimeoutMax() int {
	return r.electionTimeoutmax
}

func (r *RaftNode) getElectionTimeoutMin() int {
	return r.electionTimeoutmin
}

func (r *RaftNode) getHeartbeatTimeout() int {
	return r.heartbeatTimeout
}

// logEntry is a log entry.
type logEntry struct {
	// term is the term in which the entry was received by the leader.
	term int64
	// command is the command to be applied to the state machine.
	command any
}

func (l *logEntry) String() string {
	return fmt.Sprintf("term: %d, command: %s", l.term, l.command)
}

// LogEntries is a slice of logEntry.
type LogEntries []logEntry

func (l LogEntries) LastIndex() int64 {
	return int64(len(l) - 1)
}

func (l LogEntries) Last() *logEntry {
	if len(l) == 0 {
		return nil
	}
	return &l[l.LastIndex()]
}

func (l LogEntries) LastTerm() int64 {
	if len(l) == 0 {
		return -1
	}
	return l.Last().term
}

func (l *logEntry) Equal(other *logEntry) bool {
	return l.term == other.term && l.command == other.command
}

func (r *RaftNode) AppendEntry(cmd any) (int, int, bool) {
	index := -1
	term := -1
	// if not leader return false
	if !r.state.isLeader {
		return -1, -1, false
	}

	// append entry to local log and respond after entry applied to state machine
	return index, term, r.state.isLeader

}

func (r *RaftNode) getCurrentTermCallback() func() (int64, bool) {
	return func() (int64, bool) {
		return r.GetState()
	}
}
