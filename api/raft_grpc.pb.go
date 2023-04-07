// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.6.1
// source: api/raft.proto

package raft

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// VoteClient is the client API for Vote service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type VoteClient interface {
	// RequestVote is called by candidates to gather votes.
	RequestVote(ctx context.Context, in *VoteRequest, opts ...grpc.CallOption) (*VoteResponse, error)
}

type voteClient struct {
	cc grpc.ClientConnInterface
}

func NewVoteClient(cc grpc.ClientConnInterface) VoteClient {
	return &voteClient{cc}
}

func (c *voteClient) RequestVote(ctx context.Context, in *VoteRequest, opts ...grpc.CallOption) (*VoteResponse, error) {
	out := new(VoteResponse)
	err := c.cc.Invoke(ctx, "/api.Vote/RequestVote", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// VoteServer is the server API for Vote service.
// All implementations must embed UnimplementedVoteServer
// for forward compatibility
type VoteServer interface {
	// RequestVote is called by candidates to gather votes.
	RequestVote(context.Context, *VoteRequest) (*VoteResponse, error)
	mustEmbedUnimplementedVoteServer()
}

// UnimplementedVoteServer must be embedded to have forward compatible implementations.
type UnimplementedVoteServer struct {
}

func (UnimplementedVoteServer) RequestVote(context.Context, *VoteRequest) (*VoteResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RequestVote not implemented")
}
func (UnimplementedVoteServer) mustEmbedUnimplementedVoteServer() {}

// UnsafeVoteServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to VoteServer will
// result in compilation errors.
type UnsafeVoteServer interface {
	mustEmbedUnimplementedVoteServer()
}

func RegisterVoteServer(s grpc.ServiceRegistrar, srv VoteServer) {
	s.RegisterService(&Vote_ServiceDesc, srv)
}

func _Vote_RequestVote_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(VoteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(VoteServer).RequestVote(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.Vote/RequestVote",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(VoteServer).RequestVote(ctx, req.(*VoteRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// Vote_ServiceDesc is the grpc.ServiceDesc for Vote service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var Vote_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "api.Vote",
	HandlerType: (*VoteServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "RequestVote",
			Handler:    _Vote_RequestVote_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "api/raft.proto",
}

// AppendEntriesClient is the client API for AppendEntries service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type AppendEntriesClient interface {
	// AppendEntries is invoked by leaders to replicate log entries and
	// to send heartbeats.
	AppendEntries(ctx context.Context, in *AppendEntriesRequest, opts ...grpc.CallOption) (*AppendEntriesResponse, error)
}

type appendEntriesClient struct {
	cc grpc.ClientConnInterface
}

func NewAppendEntriesClient(cc grpc.ClientConnInterface) AppendEntriesClient {
	return &appendEntriesClient{cc}
}

func (c *appendEntriesClient) AppendEntries(ctx context.Context, in *AppendEntriesRequest, opts ...grpc.CallOption) (*AppendEntriesResponse, error) {
	out := new(AppendEntriesResponse)
	err := c.cc.Invoke(ctx, "/api.AppendEntries/AppendEntries", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// AppendEntriesServer is the server API for AppendEntries service.
// All implementations must embed UnimplementedAppendEntriesServer
// for forward compatibility
type AppendEntriesServer interface {
	// AppendEntries is invoked by leaders to replicate log entries and
	// to send heartbeats.
	AppendEntries(context.Context, *AppendEntriesRequest) (*AppendEntriesResponse, error)
	mustEmbedUnimplementedAppendEntriesServer()
}

// UnimplementedAppendEntriesServer must be embedded to have forward compatible implementations.
type UnimplementedAppendEntriesServer struct {
}

func (UnimplementedAppendEntriesServer) AppendEntries(context.Context, *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AppendEntries not implemented")
}
func (UnimplementedAppendEntriesServer) mustEmbedUnimplementedAppendEntriesServer() {}

// UnsafeAppendEntriesServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to AppendEntriesServer will
// result in compilation errors.
type UnsafeAppendEntriesServer interface {
	mustEmbedUnimplementedAppendEntriesServer()
}

func RegisterAppendEntriesServer(s grpc.ServiceRegistrar, srv AppendEntriesServer) {
	s.RegisterService(&AppendEntries_ServiceDesc, srv)
}

func _AppendEntries_AppendEntries_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AppendEntriesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AppendEntriesServer).AppendEntries(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.AppendEntries/AppendEntries",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AppendEntriesServer).AppendEntries(ctx, req.(*AppendEntriesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// AppendEntries_ServiceDesc is the grpc.ServiceDesc for AppendEntries service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var AppendEntries_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "api.AppendEntries",
	HandlerType: (*AppendEntriesServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "AppendEntries",
			Handler:    _AppendEntries_AppendEntries_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "api/raft.proto",
}
