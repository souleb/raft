package raft

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"log/slog"

	"github.com/souleb/raft/errors"
	"github.com/souleb/raft/log"
	"github.com/souleb/raft/server"
	"github.com/souleb/raft/storage"
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
	// defaultBufferSize is the default size of the commit channel.
	defaultBufferSize = 1 << 10 // 1024
)

const (
	termStorageKey     = "term"
	votedForStorageKey = "votedFor"
)

// OptionsFn is a function that sets an option.
type OptionsFn func(opt Options)

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
	// bufferSize is the length of the commitChan.
	bufferSize int
}

// RaftNode is a member of the Raft cluster
type RaftNode struct {
	// Peers is a map of peer id to peer address.
	peers map[uint]string

	// RPCServer is the server used to communicate with other peers.
	RPCServer server.Server

	//storage handles this peer's persisted state
	storage storage.Store
	state   *state

	start     sync.Mutex
	startOnce sync.Once
	started   bool

	stop     sync.RWMutex
	stopOnce sync.Once
	stopped  bool
	cancel   context.CancelFunc

	// id is this peer's id
	id             int32
	leaderID       int32
	appendChan     chan server.AppendEntries
	voteChan       chan server.VoteRequest
	applyEntryChan chan server.ApplyRequest
	// commitIndexChan is a channel that signals when the commit index has been updated.
	commitIndexChan chan struct{}
	// commitChan is a channel that receives committed entries from the RaftNode to be applied to the state machine.
	// It is buffered to allow the RaftNode to continue committing entries while the state machine is busy.
	CommitChan chan log.LogEntry
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

// WithTimeout sets the timeout for the RaftNode.
func WithTimeout(timeout int) OptionsFn {
	return func(opts Options) {
		opts.timeout = timeout
	}
}

// WithBufferSize sets the size of the commit channel.
func WithBufferSize(len int) OptionsFn {
	return func(opts Options) {
		opts.bufferSize = len
	}
}

// New creates a new RaftNode.
func New(peers map[uint]string, id int32, port uint16, storage storage.Store, logger *slog.Logger, opts ...OptionsFn) (*RaftNode, error) {
	r := &RaftNode{
		peers:           peers,
		id:              id,
		leaderID:        -1,
		commitIndexChan: make(chan struct{}),
		logger:          logger,
		storage:         storage,
	}

	for _, opt := range opts {
		opt(r.Options)
	}

	if r.storage == nil {
		return nil, fmt.Errorf("an initialized storage must be provided")
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

	r.state = &state{}
	r.state.initState()

	err := r.restoreFromStorage()
	if err != nil {
		return nil, err
	}

	if r.bufferSize == 0 {
		r.bufferSize = defaultBufferSize
	}

	r.CommitChan = make(chan log.LogEntry, defaultBufferSize)

	appendEntriesRPCChan := make(chan server.AppendEntries)
	voteRPCChan := make(chan server.VoteRequest)
	applyEntryRPCChan := make(chan server.ApplyRequest)
	r.appendChan = appendEntriesRPCChan
	r.voteChan = voteRPCChan
	r.applyEntryChan = applyEntryRPCChan

	r.errChan = make(chan error)

	s, err := server.New(int(r.id), port,
		server.WithLogger(r.logger),
		server.WithGetStateFunc(r.getCurrentTermCallback()),
		server.WithTimeout(r.timeout),
	)

	if err != nil {
		return nil, err
	}

	r.state.observers = append(r.state.observers, s)
	r.RPCServer = s.SetVoteRPCChan(r.voteChan).
		SetAppendEntryRPCChan(r.appendChan).
		SetApplyEntryRPCChan(r.applyEntryChan)

	return r, nil
}

func (r *RaftNode) Run(ctx context.Context, secure bool) error {
	var retErr error
	ctx, cancel := context.WithCancel(ctx)
	r.cancel = cancel

	r.startOnce.Do(func() {
		r.logger.Info("starting raft node", slog.Int("id", int(r.GetID())))
		retErr = r.RPCServer.Run(ctx, r.peers, secure)
		if retErr != nil {
			return
		}

		go r.runStateMachine(ctx)
		go r.commitEntries(ctx)
		r.start.Lock()
		r.started = true
		r.start.Unlock()
	})

	return retErr
}

// Stop tells the RaftNode to shut itself down.
func (r *RaftNode) Stop() error {
	var retErr error

	r.stopOnce.Do(func() {
		r.logger.Info("raft node is stopping", slog.Int("id", int(r.GetID())))
		r.stop.Lock()
		r.stopped = true
		r.stop.Unlock()
		r.cancel()
		retErr = <-r.errChan
		r.RPCServer.Stop()
		r.state.setLeader(false)
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

func (r *RaftNode) setLeaderID(id int32) {
	atomic.StoreInt32(&r.leaderID, id)
}

func (r *RaftNode) GetLeaderID() int32 {
	return atomic.LoadInt32(&r.leaderID)
}

func (r *RaftNode) IsLeader() bool {
	r.state.mu.RLock()
	defer r.state.mu.RUnlock()
	return r.state.isLeader
}

// GetState returns the currentTerm and whether this server
// believes it is the leader.
func (r *RaftNode) GetState() (uint64, bool) {
	r.state.mu.RLock()
	defer r.state.mu.RUnlock()
	return r.state.currentTerm, r.state.isLeader
}

func (r *RaftNode) GetCurrentTerm() uint64 {
	r.state.mu.RLock()
	defer r.state.mu.RUnlock()
	return r.state.currentTerm
}

func (r *RaftNode) SetCurrentTerm(term uint64) {
	r.state.mu.RLock()
	defer r.state.mu.RUnlock()
	r.state.currentTerm = term
}

func (r *RaftNode) SetLastApplied(index uint64) {
	r.state.setLastApplied(index)
}

func (r *RaftNode) GetLastApplied() uint64 {
	return r.state.getLastApplied()
}

func (r *RaftNode) GetVotedFor() int32 {
	r.state.mu.RLock()
	defer r.state.mu.RUnlock()
	return r.state.votedFor
}

func (r *RaftNode) SetVotedFor(id int32) {
	r.state.mu.Lock()
	defer r.state.mu.Unlock()
	r.state.votedFor = id
}

func (r *RaftNode) GetLogByIndex(index uint64) log.LogEntry {
	r.state.mu.RLock()
	defer r.state.mu.RUnlock()
	return r.state.log.GetLog(index)
}

func (r *RaftNode) GetLog() []log.LogEntry {
	r.state.mu.RLock()
	defer r.state.mu.RUnlock()
	l := make([]log.LogEntry, len(r.state.log))
	copy(l, r.state.log)
	return l
}

func (r *RaftNode) SetLog(log []log.LogEntry) {
	r.state.mu.Lock()
	defer r.state.mu.Unlock()
	r.state.log = log
}

func (r *RaftNode) GetPeers() map[uint]string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.peers
}

func (r *RaftNode) CopyPeers() map[uint]string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	peers := make(map[uint]string)
	for k, v := range r.peers {
		peers[k] = v
	}
	return peers
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

func (r *RaftNode) getCurrentTermCallback() func() (uint64, bool) {
	return func() (uint64, bool) {
		return r.GetState()
	}
}

func (r *RaftNode) persistCurrentTerm() error {
	if err := r.storage.SetUint64([]byte(termStorageKey), r.GetCurrentTerm()); err != nil {
		return err
	}
	return nil
}

func (r *RaftNode) persistVotedFor() error {
	if err := r.storage.SetUint64([]byte(votedForStorageKey), uint64(r.GetVotedFor())); err != nil {
		return err
	}

	return nil
}

func (r *RaftNode) persistLogs(logs []*log.LogEntry) error {
	if err := r.storage.StoreLogs(logs); err != nil {
		return err
	}
	return nil
}

func (r *RaftNode) restoreFromStorage() (err error) {
	if term, err := r.storage.GetUint64([]byte(termStorageKey)); err == nil {
		r.state.setCurrentTerm(term)
	} else {
		if !errors.Is(err, errors.NotFound) {
			return err
		}
	}

	if votedFor, err := r.storage.GetUint64([]byte(votedForStorageKey)); err == nil {
		r.state.setVotedFor(int32(votedFor))
	} else {
		if !errors.Is(err, errors.NotFound) {
			return err
		}
	}

	start, err := r.storage.FirstIndex()
	if err != nil {
		return err
	}
	// first log entry is at index 1
	if start == 0 {
		return nil
	}

	end, err := r.storage.LastIndex()
	if err != nil {
		return err
	}

	logEntries := make([]log.LogEntry, end-start+1)
	for i := start; i <= end; i++ {
		log, err := r.storage.GetLog(i)
		if err != nil {
			return err
		}
		// set the index of the log entry to start at 0
		logEntries[i-start] = *log
	}

	r.state.SetLogs(logEntries)
	return nil
}
