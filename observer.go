package raft

// Observer is an interface that is notified when the RaftNode observes a change
// in leadership. It is used to notify the leader health service.
type Observer interface {
	// Notify is called when the fsm observes a change in leadership.
	Notify(state bool)
}
