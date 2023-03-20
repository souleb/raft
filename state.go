package raft

import (
	"sync"
	"time"

	pb "github.com/souleb/raft/api"
	"github.com/souleb/raft/errors"
	"golang.org/x/exp/slog"
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
	commitIndex int
	// index of highest log entry applied to state machine (initialized to 0, increases monotonically).
	lastApplied int
	// log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	log LogEntries
	// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	nextIndex []int
	// for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	matchIndex []int
	// Lock to protect shared access
	mu sync.Mutex
}

func (s *state) setCurrentTerm(term int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.currentTerm = term
}

func (s *state) setVotedFor(votedFor int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.votedFor = votedFor
}

func (s *state) getVotedFor() int32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.votedFor
}

func (s *state) getCurrentTerm() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.currentTerm
}

func (s *state) setLeader(leader bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.isLeader = leader
}

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

			if req.msg.GetEntries() == nil {
				r.logger.Debug("received heartbeat", slog.Int("id", int(r.GetID())), slog.Int("currentTerm",
					int(r.state.getCurrentTerm())), slog.Int("term", int(req.msg.GetTerm())), slog.String("state", "follower"))
			}

			if req.msg.GetTerm() > r.state.getCurrentTerm() {
				r.logger.Debug("received request with newer term", slog.Int("id", int(r.GetID())),
					slog.Int("currentTerm", int(r.state.getCurrentTerm())), slog.Int("term", int(req.msg.GetTerm())),
					slog.String("state", "follower"))
				r.state.setCurrentTerm(req.msg.GetTerm())
				r.state.setVotedFor(-1)
			}
		case req := <-r.voteChan:
			term := r.state.getCurrentTerm()
			r.logger.Debug("received vote request", slog.Int("id", int(r.GetID())), slog.Int("currentTerm",
				int(term)), slog.Int("term", int(req.msg.GetTerm())), slog.Int("candidateID",
				int(req.msg.CandidateId)), slog.String("state", "follower"))
			vote := &pb.VoteResponse{
				Term:        term,
				VoteGranted: false,
			}
			if req.msg.GetTerm() > term {
				r.logger.Debug("received request with newer term", slog.Int("currentTerm", int(term)),
					slog.Int("term", int(req.msg.GetTerm())), slog.Int("candidateID", int(req.msg.CandidateId)),
					slog.String("state", "follower"))
				r.state.setCurrentTerm(req.msg.GetTerm())
				r.state.setVotedFor(-1)
			}
			votedFor := r.state.getVotedFor()
			if votedFor == -1 || votedFor == req.msg.GetCandidateId() {
				lastIndex := r.state.log.LastIndex()
				lastTerm := r.state.log.LastTerm()

				// if incoming request's log is at least as up-to-date as owned log, grant vote
				if req.msg.GetLastLogTerm() > lastTerm || (req.msg.GetLastLogTerm() == lastTerm && req.msg.GetLastLogIndex() >= lastIndex) {
					r.state.votedFor = req.msg.GetCandidateId()
					vote.VoteGranted = true
					resetTimer(timer, randomWaitTime(min, max))
					r.logger.Debug("voted for candidate", slog.Int("currentTerm", int(r.state.getCurrentTerm())),
						slog.Int("term", int(req.msg.GetTerm())), slog.Int("candidateID", int(req.msg.CandidateId)),
						slog.String("state", "follower"))
				}
			}
			req.reply <- vote
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
	respChan := make(chan *rpcResponse, len(r.GetPeers())*2)
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
			wg.Wait()
			r.errChan <- nil
			return nil
		case vote := <-respChan:
			r.logger.Debug("received vote", slog.Int("id", int(r.GetID())), slog.Int("currentTerm",
				int(r.state.getCurrentTerm())), slog.Int("term", int(vote.term)), slog.Bool("vote", vote.response))
			if vote.term > r.state.getCurrentTerm() {
				if !timer.Stop() {
					<-timer.C
				}
				r.prepareStateRevert(vote.term, cancel, &wg)
				r.logger.Debug("received response with newer term", slog.Int("id", int(r.GetID())),
					slog.Int("currentTerm", int(r.state.getCurrentTerm())), slog.Int("term", int(vote.term)),
					slog.String("state", "candidate"))
				return r.follower
			}
			if vote.response {
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
		case resp := <-r.appendChan:
			if resp.msg.GetTerm() >= r.state.getCurrentTerm() {
				if !timer.Stop() {
					<-timer.C
				}
				r.prepareStateRevert(resp.msg.GetTerm(), cancel, &wg)
				return r.follower
			}
		case req := <-r.voteChan:
			r.logger.Debug("received vote request", slog.Int("currentTerm", int(r.state.getCurrentTerm())),
				slog.Int("term", int(req.msg.GetTerm())), slog.String("state", "candidate"))
			if req.msg.GetTerm() > r.state.getCurrentTerm() {
				r.prepareStateRevert(req.msg.GetTerm(), cancel, &wg)
				req.reply <- &pb.VoteResponse{Term: req.msg.GetTerm(), VoteGranted: false}
				return r.follower
			}
			req.reply <- &pb.VoteResponse{Term: req.msg.GetTerm(), VoteGranted: false}
		}
	}
}

func (r *RaftNode) startElection(ctx context.Context, wg *sync.WaitGroup, respChan chan<- *rpcResponse) (*time.Timer, int) {
	r.logger.Debug("starting next election", slog.Int("id", int(r.GetID())),
		slog.Int("currentTerm", int(r.state.getCurrentTerm())))
	// increment current term
	r.state.setCurrentTerm(r.state.getCurrentTerm() + 1)
	// vote for self
	r.state.setVotedFor(r.GetID())
	// set election timer
	timer := newTimer(randomWaitTime(int64(r.getElectionTimeoutMin()), int64(r.getElectionTimeoutMax())))
	// send RequestVote RPCs to all other servers
	r.getVotes(ctx, wg, respChan)
	return timer, 1
}

func (r *RaftNode) leader(ctx context.Context) stateFn {
	r.logger.Debug("entering leader state", slog.Int("id", int(r.GetID())))
	respChan := make(chan *rpcResponse)
	ctx, cancel := context.WithCancel(ctx)
	wg := sync.WaitGroup{}
	r.sendHearbeats(ctx, &wg, respChan)
	ticker := time.NewTicker(time.Duration(r.getHeartbeatTimeout()) * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			wg.Wait()
			r.errChan <- nil
			return nil
		case resp := <-respChan:
			if resp.term > r.state.getCurrentTerm() {
				r.state.setLeader(false)
				r.prepareStateRevert(resp.term, cancel, &wg)
				return r.follower
			}
		case <-ticker.C:
			r.sendHearbeats(ctx, &wg, respChan)
			ticker.Reset(time.Duration(r.heartbeatTimeout) * time.Millisecond)
		case resp := <-r.appendChan:
			if resp.msg.GetTerm() > r.state.getCurrentTerm() {
				r.state.setLeader(false)
				r.prepareStateRevert(resp.msg.GetTerm(), cancel, &wg)
				return r.follower
			}
		case req := <-r.voteChan:
			if req.msg.GetTerm() > r.state.getCurrentTerm() {
				r.logger.Debug("received vote request with newer term, transitionnning to follower",
					slog.Int("id", int(r.GetID())), slog.Int("currentTerm", int(r.state.getCurrentTerm())),
					slog.Int("term", int(req.msg.GetTerm())), slog.String("state", "leader"))
				r.state.setLeader(false)
				r.prepareStateRevert(req.msg.GetTerm(), cancel, &wg)
				req.reply <- &pb.VoteResponse{Term: r.state.currentTerm, VoteGranted: false}
				return r.follower
			}
			req.reply <- &pb.VoteResponse{Term: req.msg.GetTerm(), VoteGranted: false}
		}
	}
}

func (r *RaftNode) prepareStateRevert(term int64, cancel context.CancelFunc, wg *sync.WaitGroup) {
	// If a candidate or leader discovers
	// that its term is out of date, it immediately reverts to follower state.
	r.state.setCurrentTerm(term)
	r.state.setVotedFor(-1)
	cancel()
	wg.Wait()
}

func (r *RaftNode) getVotes(ctx context.Context, wg *sync.WaitGroup, respChan chan<- *rpcResponse) {
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
	req := &pb.VoteRequest{
		Term:         r.state.getCurrentTerm(),
		CandidateId:  r.GetID(),
		LastLogIndex: lastIndex,
		LastLogTerm:  lastLogTerm,
	}

	for index := range r.GetPeersConn() {
		wg.Add(1)
		go func(index int) {
			response, err := r.sendRequestVote(ctx, index, req)
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

func (r *RaftNode) sendHearbeats(ctx context.Context, wg *sync.WaitGroup, respChan chan *rpcResponse) {
	var (
		lastIndex   int64 = 0
		lastLogTerm int64 = 0
	)
	if len(r.state.log) > 0 {
		lastIndex = r.state.log.LastIndex()
		lastLogTerm = r.state.log.LastTerm()
	}

	req := &pb.AppendEntriesRequest{
		Term:         r.state.getCurrentTerm(),
		LeaderId:     r.GetID(),
		PrevLogIndex: lastIndex,
		PrevLogTerm:  lastLogTerm,
	}
	for index := range r.GetPeersConn() {
		wg.Add(1)
		go func(index int) {
			resp, err := r.sendAppendEntries(ctx, index, req)
			if err != nil {
				if e, ok := err.(*errors.Error); !ok || e.StatusCode != errors.Canceled {
					r.logger.Error("while sending appendEntries rpc", slog.String("error", err.Error()))
				}
				wg.Done()
				return
			}
			respChan <- resp
			wg.Done()
		}(index)
	}
}
