package raft

import (
	"context"
	"sync"
	"time"

	"github.com/souleb/raft/errors"
	"github.com/souleb/raft/server"
	"golang.org/x/exp/slog"
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

func (h *heartbeat) heartbeat(ctx context.Context) {
	// send the first heartbeat
	h.logger.Debug("sending first heartbeat", slog.Int("id", int(h.r.GetID())),
		slog.Int("currentTerm", int(h.r.state.getCurrentTerm())),
		slog.String("state", "leader"))
	prevLogIndex, prevLogTerm := h.r.state.getLastLogIndexAndTerm()
	wg := sync.WaitGroup{}
	h.send(ctx, &wg, prevLogIndex, prevLogTerm)

	// start a ticker to send heartbeat periodically
	ticker := time.NewTicker(time.Duration(h.r.getHeartbeatTimeout()) * time.Millisecond)

	for {
		select {
		case <-ticker.C:
			h.logger.Debug("tick!, sending heartbeat", slog.Int("id", int(h.r.GetID())),
				slog.Int("currentTerm", int(h.r.state.getCurrentTerm())),
				slog.String("state", "leader"))
			prevLogIndex, prevLogTerm := h.r.state.getLastLogIndexAndTerm()
			h.send(ctx, &wg, prevLogIndex, prevLogTerm)
			// if the term is greater than the current term, send the response to the leader
			ticker.Reset(time.Duration(h.r.heartbeatTimeout) * time.Millisecond)
		case <-ctx.Done():
			ticker.Stop()
			return
		}
	}
}

func (h *heartbeat) send(ctx context.Context, wg *sync.WaitGroup, prevLogIndex, prevLogTerm int64) {
	req := server.AppendEntries{
		Term:         h.r.state.getCurrentTerm(),
		LeaderId:     h.r.GetID(),
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
	}
	for index := range h.r.GetPeers() {
		wg.Add(1)
		go func(index int) {
			resp, err := h.r.RPCServer.SendAppendEntries(ctx, index, req)
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
		}(index)
	}
}
