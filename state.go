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
	currentTerm int64
	// candidateId that received vote in current term (or null if none)
	votedFor int32
	// index of highest log entry known to be committed (initialized to 0, increases monotonically).
	commitIndex int64
	// index of highest log entry applied to state machine (initialized to 0, increases monotonically).
	lastApplied int64
	// log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	log log.LogEntries
	// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	nextIndex map[int]int64
	// for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	matchIndex map[int]int64
	// Observer is a list of observers that are notified when the RaftNode
	// observes a change in leadership.
	observers []Observer
	mu        sync.Mutex
}

func (s *state) setTermAndVote(term int64, id int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.currentTerm = term
	s.votedFor = id
}

func (s *state) resetElectionFields(term int64, leader bool) {
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

func (s *state) getVotedFor() int32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.votedFor
}

func (s *state) setVotedFor(id int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.votedFor = id
}

func (s *state) getCurrentTerm() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.currentTerm
}

func (s *state) setCurrentTerm(term int64) {
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

func (s *state) getPeerNextIndex(peer int) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nextIndex[peer]
}

func (s *state) decrementPeerNextIndex(peer int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextIndex[peer]--
}

func (s *state) updatePeerNextIndex(peer int, index int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextIndex[peer] = index
}

func (s *state) getPeerMatchIndex(peer int) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.matchIndex[peer]
}

func (s *state) updatePeerMatchIndex(peer int, index int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.matchIndex[peer] = index
}

func (s *state) appendEntry(entry log.LogEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log.AppendEntry(entry)
}

func (s *state) getEntriesFromNextIndex(peer int) log.LogEntries {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.log.GetEntriesFromIndex(s.nextIndex[peer])
}

func (s *state) getEntries(minIndex, maxIndex int64) log.LogEntries {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.log.GetEntries(minIndex, maxIndex)
}

func (s *state) getCommitIndex() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.commitIndex
}

func (s *state) setCommitIndex(index int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commitIndex = index
}

func (s *state) getLastLogIndexAndTerm() (int64, int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.log.GetLastLogIndexAndTerm()
}

func (s *state) matchEntry(prevLogIndex, prevLogTerm int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if prevLogIndex == -1 {
		return true
	}
	return s.log.MatchEntry(prevLogIndex, prevLogTerm)
}

func (s *state) getLastApplied() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastApplied
}

func (s *state) setLastApplied(index int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastApplied = index
}

func (s *state) getLastIndex() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.log.LastIndex()
}

func (s *state) getLogTerm(index int64) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.log.GetLogTerm(index)
}

func (s *state) getLastSN() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.log.LastSN()
}

func (s *state) initLog() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastApplied = 0
	for i := range s.nextIndex {
		s.nextIndex[i] = s.lastApplied + 1
		s.matchIndex[i] = 0
	}
	// If the log is empty, add a dummy entry at index 0.
	if s.log.LastIndex() == -1 {
		s.log.AppendEntry(log.LogEntry{})
	}
	return nil
}

func (s *state) updateCommitIndex(commitIndex int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := commitIndex + 1; i <= s.log.LastIndex(); i++ {
		if s.log.GetLogTerm(i) == s.currentTerm {
			c := 1
			for _, match := range s.matchIndex {
				if match >= i {
					c++
				}
			}
			if c > len(s.matchIndex)/2 {
				s.commitIndex = i
			}
		}
	}
}

func (s *state) storeEntriesFromIndex(index int64, entries log.LogEntries) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log.StoreEntriesFromIndex(index, entries)
}
