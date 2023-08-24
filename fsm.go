package raft

import (
	"context"
	"sync"
	"time"

	"log/slog"

	"github.com/souleb/raft/errors"
	"github.com/souleb/raft/log"
	"github.com/souleb/raft/server"
)

// TO DO: handle client calls when the node is not the leader and an appendEntries is received
// We should return an error to the client and the leader ID
func (r *RaftNode) follower(ctx context.Context) stateFn {
	min, max := int64(r.getElectionTimeoutMin()), int64(r.getElectionTimeoutMax())
	r.logger.Debug("follower state", slog.Int("id", int(r.GetID())))
	timer := newTimer(randomWaitTime(min, max))

	for {
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			r.errChan <- nil
			return nil
		case req := <-r.appendChan:
			resetTimer(timer, randomWaitTime(min, max))
			// if the term is greater than the current term, reset the election fields
			if req.Term > r.state.getCurrentTerm() {
				r.logger.Debug("received request with newer term", slog.Int("id", int(r.GetID())),
					slog.Int("currentTerm", int(r.state.getCurrentTerm())), slog.Int("term", int(req.Term)),
					slog.String("state", "follower"))
				r.state.setCurrentTerm(req.Term)
			}
			r.setLeaderID(req.LeaderId)
			r.handleAppendEntries(req)
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
}

func (r *RaftNode) handleAppendEntries(req server.AppendEntries) {
	if len(req.Entries) == 0 {
		r.logger.Debug("received heartbeat", slog.Int("id", int(r.GetID())), slog.Int("currentTerm",
			int(r.state.getCurrentTerm())), slog.Int("term", int(req.Term)), slog.String("state", "follower"))
		req.ResponseChan <- server.RPCResponse{
			Term:     r.state.getCurrentTerm(),
			Response: true,
		}
		return
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
		return
	}

	r.state.storeEntriesFromIndex(r.state.getLastIndex()+1, req.Entries)
	if req.LeaderCommit > r.state.getCommitIndex() {
		r.state.setCommitIndex(minValue(req.LeaderCommit, r.state.getLastIndex()))
		r.logger.Debug("committing entries", slog.Int("id", int(r.GetID())), slog.Int("currentTerm",
			int(r.state.getCurrentTerm())), slog.Int("term", int(req.Term)), slog.Int("commitIndex",
			int(r.state.getCommitIndex())), slog.String("state", "follower"))
		r.commitIndexChan <- struct{}{}
	}
	req.ResponseChan <- server.RPCResponse{
		Term:     r.state.getCurrentTerm(),
		Response: true,
	}
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
				r.setLeaderID(req.LeaderId)
				r.handleAppendEntries(req)
				r.prepareStateRevert(req.Term, false, cancel, &wg)
				return r.follower
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
			if req.Sn <= r.state.getLastSN() {
				r.logger.Debug("entry already committed", slog.Int("id", int(r.GetID())),
					slog.Int("currentTerm", int(r.state.getCurrentTerm())),
					slog.String("state", "leader"))
				req.ResponseChan <- server.RPCResponse{Term: r.state.getCurrentTerm(), Response: false}
				break
			}
			prevLogIndex, prevLogTerm := r.state.getLastLogIndexAndTerm()
			r.state.appendEntry(log.LogEntry{Term: r.state.getCurrentTerm(), Command: req.Command, Sn: req.Sn})
			// send the new entry to all peers
			responseTerm, commited := r.appendEntry(ctx, &wg, prevLogIndex, prevLogTerm)
			if responseTerm > r.state.getCurrentTerm() {
				req.ResponseChan <- server.RPCResponse{Term: responseTerm, Response: false}
				r.prepareStateRevert(responseTerm, true, cancel, &wg)
				return r.follower
			}
			if commited {
				req.ResponseChan <- server.RPCResponse{Term: r.state.getCurrentTerm(), Response: true}
			} else {
				req.ResponseChan <- server.RPCResponse{Term: r.state.getCurrentTerm(), Response: false}
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
	r.state.setTermAndVote(r.state.getCurrentTerm()+1, r.GetID())
	timer := newTimer(randomWaitTime(int64(r.getElectionTimeoutMin()), int64(r.getElectionTimeoutMax())))
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
	r.state.resetElectionFields(term, leader)
	cancel()
	wg.Wait()
}

func (r *RaftNode) appendEntry(ctx context.Context, wg *sync.WaitGroup, prevLogIndex, prevLogTerm int64) (int64, bool) {
	currentTerm := r.state.getCurrentTerm()
	currentCommitIndex := r.state.getCommitIndex()
	respChan := make(chan *server.RPCResponse, len(r.GetPeers()))
	req := server.AppendEntries{
		Term:         currentTerm,
		LeaderId:     r.GetID(),
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: r.state.getCommitIndex(),
	}

	for index := range r.GetPeers() {
		if r.state.getLastIndex() >= r.state.getPeerNextIndex(index) {
			wg.Add(1)
			go func(index int, req server.AppendEntries) {
				req.Entries = r.state.getEntriesFromNextIndex(index)
				for {
					resp, err := r.RPCServer.SendAppendEntries(ctx, index, req)
					if err != nil {
						if e, ok := err.(*errors.Error); !ok || e.StatusCode != errors.Canceled {
							r.logger.Error("while sending appendEntries rpc", slog.String("error", err.Error()))
							time.Sleep(time.Duration(r.heartbeatTimeout) * time.Millisecond)
							// TODO: Should we timeout at some point?
							continue
						}
						respChan <- &server.RPCResponse{Term: r.state.getCurrentTerm(), Response: false}
						wg.Done()
						return
					}

					if resp.Term > r.state.getCurrentTerm() {
						r.logger.Debug("received appendEntries response with newer term, transitionnning to follower",
							slog.Int("id", int(r.GetID())), slog.Int("currentTerm", int(r.state.getCurrentTerm())),
							slog.Int("term", int(resp.Term)), slog.Bool("response", resp.Response))
						respChan <- resp
						wg.Done()
						return
					}

					if !resp.Response && resp.Term <= r.state.getCurrentTerm() {
						// if fails because of inconsistency, decrement nextIndex and retry
						r.state.decrementPeerNextIndex(index)
						continue
					}

					commitIndex := r.state.getCommitIndex()
					if resp.Response && resp.Term == r.state.getCurrentTerm() {
						r.state.updatePeerMatchIndex(index, r.state.getLastIndex())
						r.state.updatePeerNextIndex(index, r.state.getLastIndex()+1)
						r.state.updateCommitIndex(commitIndex)
					}

					if r.state.getCommitIndex() != commitIndex {
						r.logger.Debug("commit index updated", slog.Int("id", int(r.GetID())),
							slog.Int("currentTerm", int(r.state.getCurrentTerm())),
							slog.Int("commitIndex", int(r.state.getCommitIndex())))
						r.commitIndexChan <- struct{}{}
					}

					respChan <- resp
					wg.Done()
					return
				}
			}(index, req)
		}
	}

	responseCount := 0
	for resp := range respChan {
		if resp.Term > r.state.getCurrentTerm() || !resp.Response {
			return resp.Term, false
		}
		responseCount++
		if responseCount >= len(r.GetPeers())/2 {
			// if currentCommitIndex has been updated, then we can return true
			if r.state.getCommitIndex() > currentCommitIndex {
				return currentTerm, true
			}
		}
	}
	return currentTerm, false
}

func (r *RaftNode) commitEntries(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.commitIndexChan:
			term := r.state.getCurrentTerm()
			lastApplied := r.state.getLastApplied()
			var entries log.LogEntries
			if r.state.getCommitIndex() > r.state.getLastApplied() {
				entries = r.state.getEntries(r.state.lastApplied+1, r.state.getCommitIndex()+1)
				r.state.setLastApplied(r.state.getCommitIndex())
			}
			r.logger.Debug("committing entries", slog.Int("id", int(r.GetID())), slog.Int("currentTerm", int(term)),
				slog.Int("lastApplied", int(lastApplied)), slog.Int("commitIndex", int(r.state.getCommitIndex())))
			for _, entry := range entries {
				r.commitChan <- entry
			}
		}
	}
}

func minValue(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
