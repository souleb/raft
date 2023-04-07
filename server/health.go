package server

import (
	"fmt"

	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

// adapted from https://github.com/Jille/raft-grpc-leader-rpc/blob/master/leaderhealth/leaderhealth.go

func (s *Server) setupHealthServer(services []string) error {
	if s.grpcServer == nil {
		return fmt.Errorf("a grpc server must be setup first")
	}
	hs := health.NewServer()
	s.report(hs, services)
	grpc_health_v1.RegisterHealthServer(s.grpcServer, hs)
	return nil
}

// Report starts a goroutine that updates the given health.Server with whether we are the Raft leader.
// It will set the given services as SERVING if we are the leader, and as NOT_SERVING otherwise.
func (s *Server) report(hs *health.Server, services []string) {
	ch := make(chan raft.Observation, 1)
	r.RegisterObserver(raft.NewObserver(ch, true, func(o *raft.Observation) bool {
		_, ok := o.Data.(raft.LeaderObservation)
		return ok
	}))

	_, state := s.getStateFunc()
	setServingStatus(hs, services, state)
	go func() {
		for range ch {
			_, state := s.getStateFunc()
			setServingStatus(hs, services, state)
		}
	}()
}

func setServingStatus(hs *health.Server, services []string, isLeader bool) {
	v := grpc_health_v1.HealthCheckResponse_NOT_SERVING
	if isLeader {
		v = grpc_health_v1.HealthCheckResponse_SERVING
	}
	for _, srv := range services {
		hs.SetServingStatus(srv, v)
	}
	hs.SetServingStatus("quis.RaftLeader", v)
}
