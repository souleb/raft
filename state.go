package raft

import (
	"sync"

	"github.com/souleb/raft/log"
	"golang.org/x/net/context"
)

// stateFn represents the state of the RaftNode
type stateFn func(ctx context.Context) stateFn

type state struct {
	isLeader bool
	//latest term server has seen (initialized to 0, increases monotonically).
	currentTerm uint64
	// candidateId that received vote in current term (or null if none)
	votedFor int32
	// index of highest log entry known to be committed (initialized to 0, increases monotonically).
	commitIndex uint64
	// index of highest log entry applied to state machine (initialized to 0, increases monotonically).
	lastApplied uint64
	// log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	log log.LogEntries
	// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	nextIndex map[uint]uint64
	// for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	matchIndex map[uint]uint64
	// Observer is a list of observers that are notified when the RaftNode
	// observes a change in leadership.
	observers []Observer
	mu        sync.RWMutex
}

func (s *state) setTermAndVote(term uint64, id int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.currentTerm = term
	s.votedFor = id
}

func (s *state) resetElectionFields(term uint64, leader bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.currentTerm = term
	s.votedFor = -1
	if leader {
		s.isLeader = false
		for _, o := range s.observers {
			o.Notify(false)
		}
	}
}

// SetLogs sets the log entries for the RaftNode.
func (s *state) SetLogs(start uint64, logs []log.LogEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log = log.New(start, logs)
}

func (s *state) getVotedFor() int32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.votedFor
}

func (s *state) setVotedFor(id int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.votedFor = id
}

func (s *state) getCurrentTerm() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentTerm
}

func (s *state) setCurrentTerm(term uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.currentTerm = term
}

func (s *state) setLeader(leader bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.isLeader = leader
	for _, o := range s.observers {
		o.Notify(leader)
	}
}

func (s *state) getPeerNextIndex(peer uint) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nextIndex[peer]
}

func (s *state) decrementPeerNextIndex(peer uint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.nextIndex[peer] > 1 {
		s.nextIndex[peer]--
	}
}

func (s *state) updatePeerNextIndex(peer uint, index uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextIndex[peer] = index
}

func (s *state) updatePeerMatchIndex(peer uint, index uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.matchIndex[peer] = index
}

func (s *state) appendEntry(entry log.LogEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log.AppendEntry(entry)
}

func (s *state) getLogs(index uint64) []*log.LogEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.log.GetEntriesSlice(index)
}

func (s *state) getEntriesFromIndex(index uint64) []log.LogEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.log.GetEntriesFromIndex(index)
}

func (s *state) getEntries(minIndex, maxIndex uint64) []log.LogEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.log.GetEntries(minIndex, maxIndex)
}

func (s *state) getAllEntries() []log.LogEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	l := make([]log.LogEntry, s.log.Length())
	copy(l, s.log.GetAllEntries())
	return l
}

func (s *state) getCommitIndex() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.commitIndex
}

func (s *state) setCommitIndex(index uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commitIndex = index
}

func (s *state) getLogTerm(index uint64) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.log.GetLogTerm(index)
}

func (s *state) matchEntry(prevLogIndex, prevLogTerm uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if prevLogIndex == 0 {
		return true
	}
	return s.log.MatchEntry(prevLogIndex, prevLogTerm)
}

func (s *state) getLastApplied() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastApplied
}

func (s *state) setLastApplied(index uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastApplied = index
}

func (s *state) getLastIndex() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.log.LastIndex()
}

func (s *state) getLastSN() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.log.LastSN()
}

func (s *state) initState() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastApplied = 0
	s.commitIndex = 0
	s.currentTerm = 0
	s.votedFor = -1
	s.log = log.New(0, []log.LogEntry{
		{
			Index:   0,
			Term:    0,
			Command: nil,
		},
	})
}

func (s *state) initLeaderVolatileState(peers map[uint]string) {
	s.nextIndex = make(map[uint]uint64)
	s.matchIndex = make(map[uint]uint64)
	for i := range peers {
		s.nextIndex[i] = s.log.LastIndex() + 1
		s.matchIndex[i] = 0
	}
}

func (s *state) updateCommitIndex(commitIndex uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for n := commitIndex + 1; n <= s.log.LastIndex(); n++ {
		if s.log.GetLogTerm(n) == s.currentTerm {
			// initialize to 1 because we count the leader itself
			c := 1
			for _, match := range s.matchIndex {
				if match >= n {
					c++
				}
				if c > len(s.matchIndex)/2 {
					s.commitIndex = n
					return
				}
			}
		}
	}
}

func (s *state) storeEntriesFromIndex(index uint64, entries []log.LogEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log.StoreEntriesFromIndex(index, entries)
}
