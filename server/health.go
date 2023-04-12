package server

import (
	"fmt"

	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func (s *RPCServer) setupHealthServer(services []string) error {
	if s.grpcServer == nil {
		return fmt.Errorf("a grpc server must be setup first")
	}
	s.hs = health.NewServer()
	s.report(services)
	grpc_health_v1.RegisterHealthServer(s.grpcServer, s.hs)
	return nil
}

func (s *RPCServer) report(services []string) {
	_, state := s.getStateFunc()
	s.setServingStatus(services, state)
	go func() {
		for state := range s.observerChan {
			s.setServingStatus(services, state)
		}
	}()
}

func (s *RPCServer) setServingStatus(services []string, isLeader bool) {
	v := grpc_health_v1.HealthCheckResponse_NOT_SERVING
	if isLeader {
		v = grpc_health_v1.HealthCheckResponse_SERVING
	}
	for _, srv := range services {
		s.hs.SetServingStatus(srv, v)
	}
}
