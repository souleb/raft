package raft

type Observer interface {
	// Observe is called when the given observation is made.
	Observe(state bool)
}
