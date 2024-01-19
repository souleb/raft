package raft

import (
	"github.com/souleb/raft/errors"
	"github.com/souleb/raft/log"
)

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

	logEntries := make([]*log.LogEntry, end-start+1)
	for i := start; i <= end; i++ {
		log, err := r.storage.GetLog(i)
		if err != nil {
			return err
		}
		// set the index of the log entry to start at 0
		logEntries[i-start] = log
	}

	r.state.SetLogs(logEntries[0].Index, logEntries)
	return nil
}

// Snapshot is called to provide a snapshot of the RaftNode's state machine.
// The index is the index of the last entry in the snapshot.
// The log entries up to and including that index should be deleted. And the
// snapshot should be saved to the storage.
func (r *RaftNode) Snapshot(index int, snapshot []byte) error {
	// delete all log entries up to and including index
	r.state.mu.Lock()
	r.state.log.DeleteEntriesBefore(uint64(index))
	start := r.state.log.Start()
	r.state.mu.Unlock()

	// delete all entries up to and including index from the storage
	err := r.storage.DeleteRange(start, uint64(index))
	if err != nil {
		return err
	}

	// save the snapshot to the storage
	if err := r.storage.Set([]byte("snapshot"), snapshot); err != nil {
		return err
	}

	return nil
}
