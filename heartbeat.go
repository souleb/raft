package raft

import (
	"context"
	"sync"
	"time"

	"log/slog"

	"github.com/souleb/raft/errors"
	"github.com/souleb/raft/server"
)

type heartbeat struct {
	r        *RaftNode
	respChan chan *server.RPCResponse
	logger   *slog.Logger
}

func newHeartbeat(r *RaftNode, respChan chan *server.RPCResponse, logger *slog.Logger) *heartbeat {
	return &heartbeat{
		r:        r,
		respChan: respChan,
		logger:   logger,
	}
}

func (h *heartbeat) start(ctx context.Context) {
	go h.heartbeat(ctx)
}

// TODO: reset the timer when a new append entry has just been sent by the leader
// this will prevent the leader from sending a heartbeat when it has just sent an append entry
func (h *heartbeat) heartbeat(ctx context.Context) {
	// send the first heartbeat
	h.logger.Debug("sending first heartbeat", slog.Int("id", int(h.r.GetID())),
		slog.Int("currentTerm", int(h.r.state.getCurrentTerm())),
		slog.String("state", "leader"))
	wg := sync.WaitGroup{}
	h.send(ctx, &wg)

	ticker := time.NewTicker(time.Duration(h.r.getHeartbeatTimeout()) * time.Millisecond)

	for {
		select {
		case <-ticker.C:
			h.logger.Debug("tick!, sending heartbeat", slog.Int("id", int(h.r.GetID())),
				slog.Int("currentTerm", int(h.r.state.getCurrentTerm())),
				slog.String("state", "leader"))
			h.send(ctx, &wg)
			// if the term is greater than the current term, send the response to the leader
			ticker.Reset(time.Duration(h.r.heartbeatTimeout) * time.Millisecond)
		case <-ctx.Done():
			ticker.Stop()
			return
		}
	}
}

func (h *heartbeat) send(ctx context.Context, wg *sync.WaitGroup) {
	req := server.AppendEntries{
		Term:         h.r.state.getCurrentTerm(),
		LeaderId:     h.r.GetID(),
		LeaderCommit: h.r.state.getCommitIndex(),
	}
	for peer := range h.r.GetPeers() {
		index := h.r.state.getPeerNextIndex(peer)
		req.PrevLogIndex = index - 1
		req.PrevLogTerm = h.r.state.getLogTerm(req.PrevLogIndex)
		wg.Add(1)
		go func(peer uint, req server.AppendEntries) {
			resp, err := h.r.RPCServer.SendAppendEntries(ctx, peer, req)
			if err != nil {
				if e, ok := err.(*errors.Error); !ok || e.StatusCode != errors.Canceled {
					h.logger.Error("while sending heartbeat rpc", slog.String("error", err.Error()))
				}
				wg.Done()
				return
			}

			// if the term is greater than the current term, send the response to the leader
			if resp.Term > h.r.state.getCurrentTerm() {
				h.respChan <- resp
			}
			wg.Done()
		}(peer, req)
	}
}
