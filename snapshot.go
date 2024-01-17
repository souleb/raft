package raft

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
