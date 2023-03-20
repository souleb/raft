package raft

import (
	"hash/maphash"
	"math/rand"
	"time"
)

func randomWaitTime(min, max int64) int64 {
	var h maphash.Hash
	h.SetSeed(maphash.MakeSeed())
	rand := rand.New(rand.NewSource(int64(h.Sum64())))
	return min + (rand.Int63() % (max - min))

}

func newTimer(ms int64) *time.Timer {
	return time.NewTimer(time.Duration(ms) * time.Millisecond)
}

func resetTimer(t *time.Timer, ms int64) {
	t.Reset(time.Duration(ms) * time.Millisecond)
}
