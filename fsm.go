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
// We should return an error to the client and the leader ID. We should not process the request
// because we want to preserve the linearizability guarantee of the system.
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
				r.persistCurrentTerm()
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
				r.persistVotedFor()
				r.persistCurrentTerm()
			}
			votedFor := r.state.getVotedFor()
			if votedFor == -1 || votedFor == req.CandidateId {
				lastIndex := r.state.log.LastIndex()
				lastTerm := r.state.log.LastTerm()

				// if incoming request's log is at least as up-to-date as owned log, grant vote
				if req.LastLogTerm > lastTerm || (req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIndex) {
					r.state.setVotedFor(req.CandidateId)
					r.persistVotedFor()
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
		case req := <-r.installSnapshotChan:
			resetTimer(timer, randomWaitTime(min, max))
			snapshot := req.Data
			if err := r.persistSnapshot(snapshot); err != nil {
				r.logger.Error("while persisting snapshot", slog.String("error", err.Error()))
				req.ResponseChan <- server.RPCResponse{Term: r.state.getCurrentTerm(), Response: true}
				break
			}

			start := r.state.getFirstIndex()
			last := r.state.getLastIndex()
			if start < req.LastIncludedIndex {
				if last > req.LastIncludedIndex {
					last = req.LastIncludedIndex + 1
				}
				r.state.deleteEntriesBefore(last)
				err := r.storage.DeleteRange(start, last)
				if err != nil {
					r.logger.Error("while deleting entries from storage", slog.String("error", err.Error()))
				}
			} else {
				r.state.SetLogs(req.LastIncludedIndex+1, []*log.LogEntry{})
				r.storage.DeleteRange(start, last)
			}
			r.CommitChan <- &ApplyMsg{CommitType: Snapshot, Snapshot: snapshot, SnapshotIndex: req.LastIncludedIndex, SnapshotTerm: req.LastIncludedTerm}
			req.ResponseChan <- server.RPCResponse{Term: r.state.getCurrentTerm(), Response: true}
		case <-timer.C:
			r.logger.Debug("election timeout, transitionning to candidate", slog.Int("id", int(r.GetID())),
				slog.Int("currentTerm", int(r.state.getCurrentTerm())))
			return r.candidate
		}
	}
}

func (r *RaftNode) handleAppendEntries(req server.AppendEntries) {
	ok, index, term := r.state.matchEntry(req.PrevLogIndex, req.PrevLogTerm)

	// check if commit index needs to be updated
	if req.LeaderCommit > r.state.getCommitIndex() && ok {
		r.notifyCommitUpdate(req)
	}

	// if heartbeat, just repond success and return
	if len(req.Entries) == 0 {
		r.logger.Debug("received heartbeat", slog.Int("id", int(r.GetID())), slog.Int("currentTerm",
			int(r.state.getCurrentTerm())), slog.Int("term", int(req.Term)), slog.String("state", "follower"))

		req.ResponseChan <- server.RPCResponse{
			Term:     r.state.getCurrentTerm(),
			Response: true,
		}
		return
	}

	r.logger.Debug("received appendEntries", slog.Int("id", int(r.GetID())), slog.Int("currentTerm",
		int(r.state.getCurrentTerm())), slog.Int("term", int(req.Term)), slog.Int("prevLogIndex",
		int(req.PrevLogIndex)), slog.Int("prevLogTerm", int(req.PrevLogTerm)), slog.String("state", "follower"))

	if !ok {
		r.logger.Debug("log mismatch", slog.Int("id", int(r.GetID())), slog.Int("currentTerm",
			int(r.state.getCurrentTerm())), slog.Int("term", int(req.Term)), slog.Int("prevLogIndex",
			int(req.PrevLogIndex)), slog.Int("prevLogTerm", int(req.PrevLogTerm)), slog.String("state", "follower"))
		req.ResponseChan <- server.RPCResponse{
			Term:         r.state.getCurrentTerm(),
			Response:     false,
			ConflictTerm: term,
			ConflictIdx:  index,
		}
		return
	}

	idx := r.state.getLastIndex() + 1
	r.state.storeEntriesFromIndex(idx, req.Entries)
	r.persistLogs(r.state.getLogs(idx))
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
					r.persistVotedFor()
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
	r.state.initLeaderVolatileState(r.peers)
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
			entry := log.LogEntry{Term: r.state.getCurrentTerm(), Command: req.Command, Sn: req.Sn}
			r.state.appendEntry(&entry)
			r.persistLogs(r.state.getLogs(r.state.getLastIndex()))
			// send the new entry to all peers
			responseTerm, commited := r.appendEntry(ctx, &wg)
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
	r.persistCurrentTerm()
	r.persistVotedFor()
	timer := newTimer(randomWaitTime(int64(r.getElectionTimeoutMin()), int64(r.getElectionTimeoutMax())))
	r.getVotes(ctx, wg, respChan)
	return timer, 1
}

func (r *RaftNode) getVotes(ctx context.Context, wg *sync.WaitGroup, respChan chan<- *server.RPCResponse) {
	r.logger.Debug("sending request vote", slog.Int("id", int(r.GetID())),
		slog.Int("currentTerm", int(r.state.getCurrentTerm())))
	var (
		lastIndex   uint64 = 0
		lastLogTerm uint64 = 0
	)
	if r.state.log.Length() > 0 {
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
		go func(index uint) {
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

func (r *RaftNode) prepareStateRevert(term uint64, leader bool, cancel context.CancelFunc, wg *sync.WaitGroup) {
	r.state.resetElectionFields(term, leader)
	r.persistCurrentTerm()
	r.persistVotedFor()
	cancel()
	wg.Wait()
}

func (r *RaftNode) appendEntry(ctx context.Context, wg *sync.WaitGroup) (uint64, bool) {
	currentTerm := r.state.getCurrentTerm()
	currentCommitIndex := r.state.getCommitIndex()
	respChan := make(chan *server.RPCResponse, len(r.GetPeers()))
	req := server.AppendEntries{
		Term:         currentTerm,
		LeaderId:     r.GetID(),
		LeaderCommit: r.state.getCommitIndex(),
	}

	for peer := range r.GetPeers() {
		index := r.state.getPeerNextIndex(peer)
		firstIndex := r.state.getFirstIndex()
		if index < firstIndex {
			// this means that a snapshot has been created and the log has been truncated
			req.PrevLogIndex = r.state.getLastIncludedIndex()
			req.PrevLogTerm = r.state.getLastIncludedTerm()
		} else {
			req.PrevLogIndex = index - 1
			req.PrevLogTerm = r.state.getLogTerm(req.PrevLogIndex)
		}
		if r.state.getLastIndex() >= index {
			wg.Add(1)
			go func(peer uint, index uint64, req server.AppendEntries) {
				req.Entries = r.state.getEntriesFromIndex(index)
				for {
					resp, err := r.RPCServer.SendAppendEntries(ctx, peer, req)
					if err != nil {
						if e, ok := err.(*errors.Error); !ok || e.StatusCode != errors.Canceled {
							r.logger.Error("while sending appendEntries rpc", slog.String("error", err.Error()))
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
						// term of conflicting entry and first index it stores for that term
						ok, idx := r.state.getConflictIndex(req.PrevLogIndex, resp.ConflictIdx, resp.ConflictTerm)
						if !ok {
							sResp, err := r.RPCServer.SendInstallSnapshot(ctx, peer, server.SnapshotRequest{
								Term:              r.state.getCurrentTerm(),
								LeaderId:          r.GetID(),
								LastIncludedIndex: idx,
								LastIncludedTerm:  r.state.getLogTerm(idx),
								Data:              r.state.getSnapshot(),
							})
							if err != nil {
								if e, ok := err.(*errors.Error); !ok || e.StatusCode != errors.Canceled {
									r.logger.Error("while sending installSnapshot rpc", slog.String("error", err.Error()))
								}
								respChan <- &server.RPCResponse{Term: r.state.getCurrentTerm(), Response: false}
								wg.Done()
								return
							}
							if sResp.Term > r.state.getCurrentTerm() {
								r.logger.Debug("received installSnapshot response with newer term, transitionnning to follower",
									slog.Int("id", int(r.GetID())), slog.Int("currentTerm", int(r.state.getCurrentTerm())),
									slog.Int("term", int(sResp.Term)), slog.Bool("response", sResp.Response))
								respChan <- sResp
								wg.Done()
								return
							}
							// update to first index of the log
							r.state.updatePeerNextIndex(peer, r.state.getFirstIndex())
						} else {
							r.state.updatePeerNextIndex(peer, idx)
						}
						continue
					}

					commitIndex := r.state.getCommitIndex()
					if resp.Response && resp.Term == r.state.getCurrentTerm() {
						r.state.updatePeerMatchIndex(peer, r.state.getLastIndex())
						r.state.updatePeerNextIndex(peer, r.state.getLastIndex()+1)
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
			}(peer, index, req)
		}
	}

	responseCount := 0
	for resp := range respChan {
		// if the term is greater than the current term, return the term and false
		// so that the state can be reverted to follower
		if resp.Term > r.state.getCurrentTerm() {
			return resp.Term, false
		}
		// successful or not, we increment the response count
		responseCount++
		// if we get a majority of responses, we can return if the commit index has been updated
		if responseCount >= len(r.GetPeers())/2 {
			// if currentCommitIndex has been updated, then we can return true
			if r.state.getCommitIndex() > currentCommitIndex {
				return currentTerm, true
			}
		}
		if responseCount >= len(r.GetPeers()) {
			// if we get responses from all peers, it means that we have no majority
			// so we can return false
			break
		}
	}
	return currentTerm, false
}

func (r *RaftNode) notifyCommitUpdate(req server.AppendEntries) {
	r.state.setCommitIndex(minValue(req.LeaderCommit, r.state.getLastIndex()))
	r.commitIndexChan <- struct{}{}
}

func (r *RaftNode) commitEntries(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.commitIndexChan:
			var entries []*log.LogEntry
			if r.state.getCommitIndex() > r.state.getLastApplied() {
				entries = r.state.getEntries(r.state.lastApplied+1, r.state.getCommitIndex()+1)
				r.state.setLastApplied(r.state.getCommitIndex())
			}
			if len(entries) > 0 {
				r.logger.Debug("committing entries", slog.Int("id", int(r.GetID())), slog.Int("currentTerm",
					int(r.state.getCurrentTerm())), slog.Int("commitIndex",
					int(r.state.getCommitIndex())), slog.String("state", "follower"))
			}
			for _, entry := range entries {
				select {
				case r.CommitChan <- &ApplyMsg{Command: entry.Command, CommandIndex: entry.Index, CommitType: Entry}:
				default:
					r.logger.Error("commit channel is full, dropping entry", slog.Int("id", int(r.GetID())),
						slog.Int("currentTerm", int(r.state.getCurrentTerm())), slog.Int("commitIndex",
							int(r.state.getCommitIndex())), slog.String("state", "follower"))
				}
			}
		}
	}
}

func minValue(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
