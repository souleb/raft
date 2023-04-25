package raft

import (
	"context"
	"sync"
	"time"

	"github.com/souleb/raft/errors"
	"github.com/souleb/raft/log"
	"github.com/souleb/raft/server"
	"golang.org/x/exp/slog"
)

// TO DO: handle client calls when the node is not the leader
func (r *RaftNode) follower(ctx context.Context) stateFn {
	min, max := int64(r.getElectionTimeoutMin()), int64(r.getElectionTimeoutMax())
	r.logger.Debug("follower state", slog.Int("id", int(r.GetID())))
	timer := newTimer(randomWaitTime(min, max))

	for !r.IsStopped() {
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			r.errChan <- nil
			return nil
		case req := <-r.appendChan:
			resetTimer(timer, randomWaitTime(min, max))

			if len(req.Entries) == 0 {
				r.logger.Debug("received heartbeat", slog.Int("id", int(r.GetID())), slog.Int("currentTerm",
					int(r.state.getCurrentTerm())), slog.Int("term", int(req.Term)), slog.String("state", "follower"))
			}

			// if the term is greater than the current term, reset the election fields
			if req.Term > r.state.getCurrentTerm() {
				r.logger.Debug("received request with newer term", slog.Int("id", int(r.GetID())),
					slog.Int("currentTerm", int(r.state.getCurrentTerm())), slog.Int("term", int(req.Term)),
					slog.String("state", "follower"))
				r.state.setCurrentTerm(req.Term)
			}

			ok := r.state.matchEntry(req.PrevLogIndex, req.PrevLogTerm)
			if !ok {
				r.logger.Debug("log mismatch", slog.Int("id", int(r.GetID())), slog.Int("currentTerm",
					int(r.state.getCurrentTerm())), slog.Int("term", int(req.Term)), slog.Int("prevLogIndex",
					int(req.PrevLogIndex)), slog.Int("prevLogTerm", int(req.PrevLogTerm)), slog.String("state", "follower"))
				req.ResponseChan <- server.RPCResponse{
					Term:     r.state.getCurrentTerm(),
					Response: false,
				}
			}

			r.state.storeEntriesFromIndex(r.state.getLastIndex()+1, req.Entries)
			if req.LeaderCommit > r.state.getCommitIndex() {
				r.state.setCommitIndex(minValue(req.LeaderCommit, r.state.getLastIndex()))
			}
			req.ResponseChan <- server.RPCResponse{
				Term:     r.state.getCurrentTerm(),
				Response: true,
			}
		case req := <-r.voteChan:
			voted := false
			r.logger.Debug("received vote request", slog.Int("id", int(r.GetID())), slog.Int("currentTerm",
				int(r.state.getCurrentTerm())), slog.Int("term", int(req.Term)), slog.Int("candidateID",
				int(req.CandidateId)), slog.String("state", "follower"))
			if req.Term > r.state.getCurrentTerm() {
				r.logger.Debug("received request with newer term", slog.Int("currentTerm", int(r.state.getCurrentTerm())),
					slog.Int("term", int(req.Term)), slog.Int("candidateID", int(req.CandidateId)),
					slog.String("state", "follower"))
				r.state.resetElectionFields(req.Term, false)
			}
			votedFor := r.state.getVotedFor()
			if votedFor == -1 || votedFor == req.CandidateId {
				lastIndex := r.state.log.LastIndex()
				lastTerm := r.state.log.LastTerm()

				// if incoming request's log is at least as up-to-date as owned log, grant vote
				if req.LastLogTerm > lastTerm || (req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIndex) {
					r.state.setVotedFor(req.CandidateId)
					voted = true
					resetTimer(timer, randomWaitTime(min, max))
					r.logger.Debug("voted for candidate", slog.Int("currentTerm", int(r.state.getCurrentTerm())),
						slog.Int("term", int(req.Term)), slog.Int("candidateID", int(req.CandidateId)),
						slog.String("state", "follower"))
				}
			}
			vote := server.RPCResponse{
				Term:     r.state.getCurrentTerm(),
				Response: voted,
			}
			req.ResponseChan <- vote
		case <-timer.C:
			r.logger.Debug("election timeout, transitionning to candidate", slog.Int("id", int(r.GetID())),
				slog.Int("currentTerm", int(r.state.getCurrentTerm())))
			return r.candidate
		}
	}
	return nil
}

func (r *RaftNode) candidate(ctx context.Context) stateFn {
	r.logger.Debug("entering candidate state", slog.Int("id", int(r.GetID())),
		slog.Int("currentTerm", int(r.state.getCurrentTerm())))
	respChan := make(chan *server.RPCResponse, len(r.GetPeers())*2)
	ctx, cancel := context.WithCancel(ctx)
	wg := sync.WaitGroup{}

	var (
		timer *time.Timer
		votes int
	)
	timer, votes = r.startElection(ctx, &wg, respChan)
	for {
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			cancel()
			wg.Wait()
			r.errChan <- nil
			return nil
		case vote := <-respChan:
			r.logger.Debug("received vote", slog.Int("id", int(r.GetID())), slog.Int("currentTerm",
				int(r.state.getCurrentTerm())), slog.Int("term", int(vote.Term)), slog.Bool("vote", vote.Response))
			if vote.Term > r.state.getCurrentTerm() {
				if !timer.Stop() {
					<-timer.C
				}
				r.prepareStateRevert(vote.Term, false, cancel, &wg)
				r.logger.Debug("received response with newer term", slog.Int("id", int(r.GetID())),
					slog.Int("currentTerm", int(r.state.getCurrentTerm())), slog.Int("term", int(vote.Term)),
					slog.String("state", "candidate"))
				return r.follower
			}
			if vote.Response {
				votes++
			}
			if votes > len(r.GetPeers())/2 {
				r.state.setLeader(true)
				cancel()
				wg.Wait()
				return r.leader
			}
		case <-timer.C:
			r.logger.Debug("election timeout", slog.Int("id", int(r.GetID())), slog.Int("currentTerm",
				int(r.state.getCurrentTerm())), slog.String("state", "candidate"))
			timer, votes = r.startElection(ctx, &wg, respChan)
		case req := <-r.appendChan:
			if req.Term >= r.state.getCurrentTerm() {
				if !timer.Stop() {
					<-timer.C
				}
				// return response to leader
				req.ResponseChan <- server.RPCResponse{
					Term:     r.state.getCurrentTerm(),
					Response: true,
				}
				r.prepareStateRevert(req.Term, false, cancel, &wg)
				return r.follower
			}
			// return response to leader
			req.ResponseChan <- server.RPCResponse{
				Term:     r.state.getCurrentTerm(),
				Response: true,
			}
		case req := <-r.voteChan:
			r.logger.Debug("received vote request", slog.Int("currentTerm", int(r.state.getCurrentTerm())),
				slog.Int("term", int(req.Term)), slog.String("state", "candidate"))
			if req.Term > r.state.getCurrentTerm() {
				// if newer term, revert to follower and grant vote
				r.prepareStateRevert(req.Term, false, cancel, &wg)
				// grant vote if candidate's log is at least as up-to-date as own log
				lastIndex := r.state.log.LastIndex()
				lastTerm := r.state.log.LastTerm()
				vote := false
				if req.LastLogTerm > lastTerm || (req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIndex) {
					r.state.setVotedFor(req.CandidateId)
					vote = true
					r.logger.Debug("voted for candidate with newer term", slog.Int("currentTerm", int(r.state.getCurrentTerm())),
						slog.Int("term", int(req.Term)), slog.Int("candidateID", int(req.CandidateId)),
						slog.String("state", "candidate"))
				}

				req.ResponseChan <- server.RPCResponse{Term: req.Term, Response: vote}
				return r.follower
			}
			req.ResponseChan <- server.RPCResponse{Term: req.Term, Response: false}
		}
	}
}

func (r *RaftNode) leader(ctx context.Context) stateFn {
	r.logger.Debug("entering leader state", slog.Int("id", int(r.GetID())))
	respChan := make(chan *server.RPCResponse)
	ctx, cancel := context.WithCancel(ctx)
	wg := sync.WaitGroup{}
	// initialize nextIndex and matchIndex for each server
	r.state.initNextIndexAndMatchIndex()
	// run a goroutine to send heartbeats
	newHeartbeat(r, respChan, r.logger).start(ctx)
	for {
		select {
		case <-ctx.Done():
			cancel()
			wg.Wait()
			r.errChan <- nil
			return nil
		case resp := <-respChan:
			if resp.Term > r.state.getCurrentTerm() {
				r.prepareStateRevert(resp.Term, true, cancel, &wg)
				return r.follower
			}
		case req := <-r.applyEntryChan:
			r.logger.Debug("received apply entry request", slog.Int("id", int(r.GetID())),
				slog.Int("currentTerm", int(r.state.getCurrentTerm())),
				slog.String("state", "leader"))
			// check serial number
			if req.Sn <= r.state.getLastSN() {
				r.logger.Debug("entry already committed", slog.Int("id", int(r.GetID())),
					slog.Int("currentTerm", int(r.state.getCurrentTerm())),
					slog.String("state", "leader"))
				req.ResponseChan <- server.RPCResponse{Term: r.state.getCurrentTerm(), Response: false}
				continue
			}
			// a new apply entry request is received from the client, append it to the log
			prevLogIndex, prevLogTerm := r.state.getLastLogIndexAndTerm()
			r.state.appendEntry(log.LogEntry{Term: r.state.getCurrentTerm(), Command: req.Command, Sn: req.Sn})
			// send the new entry to all peers
			updatedTerm, success := r.appendEntry(ctx, &wg, prevLogIndex, prevLogTerm)
			if updatedTerm > r.state.getCurrentTerm() {
				r.prepareStateRevert(updatedTerm, true, cancel, &wg)
				return r.follower
			}
			if success {
				// commit the entry
				r.state.updateCommitIndex(r.state.getLastIndex())
				// if the entry is committed, send the response to the client
				r.logger.Debug("entry committed", slog.Int("id", int(r.GetID())),
					slog.Int("currentTerm", int(r.state.getCurrentTerm())),
					slog.String("state", "leader"))
				req.ResponseChan <- server.RPCResponse{Term: r.state.getCurrentTerm(), Response: true}
			}
		case resp := <-r.appendChan:
			if resp.Term > r.state.getCurrentTerm() {
				r.prepareStateRevert(resp.Term, true, cancel, &wg)
				return r.follower
			}
		case req := <-r.voteChan:
			if req.Term > r.state.getCurrentTerm() {
				r.logger.Debug("received vote request with newer term, transitionnning to follower",
					slog.Int("id", int(r.GetID())), slog.Int("currentTerm", int(r.state.getCurrentTerm())),
					slog.Int("term", int(req.Term)), slog.String("state", "leader"))
				req.ResponseChan <- server.RPCResponse{Term: req.Term, Response: false}
				r.prepareStateRevert(req.Term, true, cancel, &wg)
				return r.follower
			}
			req.ResponseChan <- server.RPCResponse{Term: req.Term, Response: false}
		}
	}
}

func (r *RaftNode) startElection(ctx context.Context, wg *sync.WaitGroup, respChan chan<- *server.RPCResponse) (*time.Timer, int) {
	r.logger.Debug("starting next election", slog.Int("id", int(r.GetID())),
		slog.Int("currentTerm", int(r.state.getCurrentTerm())))
	// increment current term and vote for self
	r.state.setTermAndVote(r.state.getCurrentTerm()+1, r.GetID())
	// set election timer
	timer := newTimer(randomWaitTime(int64(r.getElectionTimeoutMin()), int64(r.getElectionTimeoutMax())))
	// send RequestVote RPCs to all other servers
	r.getVotes(ctx, wg, respChan)
	return timer, 1
}

func (r *RaftNode) getVotes(ctx context.Context, wg *sync.WaitGroup, respChan chan<- *server.RPCResponse) {
	r.logger.Debug("sending request vote", slog.Int("id", int(r.GetID())),
		slog.Int("currentTerm", int(r.state.getCurrentTerm())))
	var (
		lastIndex   int64 = 0
		lastLogTerm int64 = 0
	)
	if len(r.state.log) > 0 {
		lastIndex = r.state.log.LastIndex()
		lastLogTerm = r.state.log.LastTerm()
	}
	req := server.VoteRequest{
		Term:         r.state.getCurrentTerm(),
		CandidateId:  r.GetID(),
		LastLogIndex: lastIndex,
		LastLogTerm:  lastLogTerm,
	}

	for index := range r.GetPeers() {
		wg.Add(1)
		go func(index int) {
			response, err := r.RPCServer.SendRequestVote(ctx, index, req)
			if err != nil {
				if e, ok := err.(*errors.Error); !ok || e.StatusCode != errors.Canceled {
					r.logger.Error("while sending requestVote rpc", slog.String("error", err.Error()))
				}
				wg.Done()
				return
			}
			respChan <- response
			wg.Done()
		}(index)
	}
}

func (r *RaftNode) prepareStateRevert(term int64, leader bool, cancel context.CancelFunc, wg *sync.WaitGroup) {
	// If a candidate or leader discovers
	// that its term is out of date, it immediately reverts to follower state.
	r.state.resetElectionFields(term, leader)
	cancel()
	wg.Wait()
}

func (r *RaftNode) appendEntry(ctx context.Context, wg *sync.WaitGroup, prevLogIndex, prevLogTerm int64) (int64, bool) {
	respChan := make(chan *server.RPCResponse)

	req := server.AppendEntries{
		Term:         r.state.getCurrentTerm(),
		LeaderId:     r.GetID(),
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: r.state.getCommitIndex(),
	}
	n := len(r.GetPeers())
	for index := range r.GetPeers() {
		if r.state.getLastIndex() >= r.state.getNextIndex(index) {
			wg.Add(1)
			go func(index int) {
				for {
					req.Entries = r.state.getEntriesFromIndex(r.state.getNextIndex(index))
					resp, err := r.RPCServer.SendAppendEntries(ctx, index, req)
					if err != nil {
						if e, ok := err.(*errors.Error); !ok || e.StatusCode != errors.Canceled {
							r.logger.Error("while sending appendEntries rpc", slog.String("error", err.Error()))
							time.Sleep(time.Duration(r.heartbeatTimeout) * time.Millisecond)
							continue
						}
						respChan <- &server.RPCResponse{Term: r.state.getCurrentTerm(), Response: false}
						wg.Done()
						return
					}
					if !resp.Response && resp.Term <= r.state.getCurrentTerm() {
						// if fails cause of inconsistency, decrement nextIndex and retry
						r.state.decrementNextIndex(index)
						continue
					}

					if resp.Response {
						r.state.updateMatchIndex(index, r.state.getLastIndex())
						r.state.updateNextIndex(index, r.state.getLastIndex()+1)
					}

					respChan <- resp
					wg.Done()
					return
				}
			}(index)
		}
	}

	updateTerm := r.state.getCurrentTerm()
	success := 0
	for i := 0; i < n; i++ {
		resp := <-respChan
		switch {
		case resp.Term > updateTerm:
			return resp.Term, false
		case resp.Response:
			success++
			if success > n/2 {
				return updateTerm, true
			}
		}
	}
	return updateTerm, false
}

func minValue(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
