// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v4.23.4
// source: api/raft_client.proto

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

// ApplyEntryClient is the client API for ApplyEntry service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ApplyEntryClient interface {
	// RequestVote is called by candidates to gather votes.
	ApplyEntry(ctx context.Context, in *ApplyRequest, opts ...grpc.CallOption) (*ApplyResponse, error)
}

type applyEntryClient struct {
	cc grpc.ClientConnInterface
}

func NewApplyEntryClient(cc grpc.ClientConnInterface) ApplyEntryClient {
	return &applyEntryClient{cc}
}

func (c *applyEntryClient) ApplyEntry(ctx context.Context, in *ApplyRequest, opts ...grpc.CallOption) (*ApplyResponse, error) {
	out := new(ApplyResponse)
	err := c.cc.Invoke(ctx, "/api.ApplyEntry/ApplyEntry", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ApplyEntryServer is the server API for ApplyEntry service.
// All implementations must embed UnimplementedApplyEntryServer
// for forward compatibility
type ApplyEntryServer interface {
	// RequestVote is called by candidates to gather votes.
	ApplyEntry(context.Context, *ApplyRequest) (*ApplyResponse, error)
	mustEmbedUnimplementedApplyEntryServer()
}

// UnimplementedApplyEntryServer must be embedded to have forward compatible implementations.
type UnimplementedApplyEntryServer struct {
}

func (UnimplementedApplyEntryServer) ApplyEntry(context.Context, *ApplyRequest) (*ApplyResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ApplyEntry not implemented")
}
func (UnimplementedApplyEntryServer) mustEmbedUnimplementedApplyEntryServer() {}

// UnsafeApplyEntryServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ApplyEntryServer will
// result in compilation errors.
type UnsafeApplyEntryServer interface {
	mustEmbedUnimplementedApplyEntryServer()
}

func RegisterApplyEntryServer(s grpc.ServiceRegistrar, srv ApplyEntryServer) {
	s.RegisterService(&ApplyEntry_ServiceDesc, srv)
}

func _ApplyEntry_ApplyEntry_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ApplyRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ApplyEntryServer).ApplyEntry(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.ApplyEntry/ApplyEntry",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ApplyEntryServer).ApplyEntry(ctx, req.(*ApplyRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// ApplyEntry_ServiceDesc is the grpc.ServiceDesc for ApplyEntry service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var ApplyEntry_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "api.ApplyEntry",
	HandlerType: (*ApplyEntryServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "ApplyEntry",
			Handler:    _ApplyEntry_ApplyEntry_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "api/raft_client.proto",
}
