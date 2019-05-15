// Code generated by protoc-gen-go. DO NOT EDIT.
// source: MultiRowMutation.proto

//*
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pb

import (
	context "context"
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	grpc "google.golang.org/grpc"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type MultiRowMutationProcessorRequest struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *MultiRowMutationProcessorRequest) Reset()         { *m = MultiRowMutationProcessorRequest{} }
func (m *MultiRowMutationProcessorRequest) String() string { return proto.CompactTextString(m) }
func (*MultiRowMutationProcessorRequest) ProtoMessage()    {}
func (*MultiRowMutationProcessorRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_0d349fc249c590f4, []int{0}
}

func (m *MultiRowMutationProcessorRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_MultiRowMutationProcessorRequest.Unmarshal(m, b)
}
func (m *MultiRowMutationProcessorRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_MultiRowMutationProcessorRequest.Marshal(b, m, deterministic)
}
func (m *MultiRowMutationProcessorRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_MultiRowMutationProcessorRequest.Merge(m, src)
}
func (m *MultiRowMutationProcessorRequest) XXX_Size() int {
	return xxx_messageInfo_MultiRowMutationProcessorRequest.Size(m)
}
func (m *MultiRowMutationProcessorRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_MultiRowMutationProcessorRequest.DiscardUnknown(m)
}

var xxx_messageInfo_MultiRowMutationProcessorRequest proto.InternalMessageInfo

type MultiRowMutationProcessorResponse struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *MultiRowMutationProcessorResponse) Reset()         { *m = MultiRowMutationProcessorResponse{} }
func (m *MultiRowMutationProcessorResponse) String() string { return proto.CompactTextString(m) }
func (*MultiRowMutationProcessorResponse) ProtoMessage()    {}
func (*MultiRowMutationProcessorResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_0d349fc249c590f4, []int{1}
}

func (m *MultiRowMutationProcessorResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_MultiRowMutationProcessorResponse.Unmarshal(m, b)
}
func (m *MultiRowMutationProcessorResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_MultiRowMutationProcessorResponse.Marshal(b, m, deterministic)
}
func (m *MultiRowMutationProcessorResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_MultiRowMutationProcessorResponse.Merge(m, src)
}
func (m *MultiRowMutationProcessorResponse) XXX_Size() int {
	return xxx_messageInfo_MultiRowMutationProcessorResponse.Size(m)
}
func (m *MultiRowMutationProcessorResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_MultiRowMutationProcessorResponse.DiscardUnknown(m)
}

var xxx_messageInfo_MultiRowMutationProcessorResponse proto.InternalMessageInfo

type MutateRowsRequest struct {
	MutationRequest      []*MutationProto `protobuf:"bytes,1,rep,name=mutation_request,json=mutationRequest" json:"mutation_request,omitempty"`
	NonceGroup           *uint64          `protobuf:"varint,2,opt,name=nonce_group,json=nonceGroup" json:"nonce_group,omitempty"`
	Nonce                *uint64          `protobuf:"varint,3,opt,name=nonce" json:"nonce,omitempty"`
	XXX_NoUnkeyedLiteral struct{}         `json:"-"`
	XXX_unrecognized     []byte           `json:"-"`
	XXX_sizecache        int32            `json:"-"`
}

func (m *MutateRowsRequest) Reset()         { *m = MutateRowsRequest{} }
func (m *MutateRowsRequest) String() string { return proto.CompactTextString(m) }
func (*MutateRowsRequest) ProtoMessage()    {}
func (*MutateRowsRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_0d349fc249c590f4, []int{2}
}

func (m *MutateRowsRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_MutateRowsRequest.Unmarshal(m, b)
}
func (m *MutateRowsRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_MutateRowsRequest.Marshal(b, m, deterministic)
}
func (m *MutateRowsRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_MutateRowsRequest.Merge(m, src)
}
func (m *MutateRowsRequest) XXX_Size() int {
	return xxx_messageInfo_MutateRowsRequest.Size(m)
}
func (m *MutateRowsRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_MutateRowsRequest.DiscardUnknown(m)
}

var xxx_messageInfo_MutateRowsRequest proto.InternalMessageInfo

func (m *MutateRowsRequest) GetMutationRequest() []*MutationProto {
	if m != nil {
		return m.MutationRequest
	}
	return nil
}

func (m *MutateRowsRequest) GetNonceGroup() uint64 {
	if m != nil && m.NonceGroup != nil {
		return *m.NonceGroup
	}
	return 0
}

func (m *MutateRowsRequest) GetNonce() uint64 {
	if m != nil && m.Nonce != nil {
		return *m.Nonce
	}
	return 0
}

type MutateRowsResponse struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *MutateRowsResponse) Reset()         { *m = MutateRowsResponse{} }
func (m *MutateRowsResponse) String() string { return proto.CompactTextString(m) }
func (*MutateRowsResponse) ProtoMessage()    {}
func (*MutateRowsResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_0d349fc249c590f4, []int{3}
}

func (m *MutateRowsResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_MutateRowsResponse.Unmarshal(m, b)
}
func (m *MutateRowsResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_MutateRowsResponse.Marshal(b, m, deterministic)
}
func (m *MutateRowsResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_MutateRowsResponse.Merge(m, src)
}
func (m *MutateRowsResponse) XXX_Size() int {
	return xxx_messageInfo_MutateRowsResponse.Size(m)
}
func (m *MutateRowsResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_MutateRowsResponse.DiscardUnknown(m)
}

var xxx_messageInfo_MutateRowsResponse proto.InternalMessageInfo

func init() {
	proto.RegisterType((*MultiRowMutationProcessorRequest)(nil), "pb.MultiRowMutationProcessorRequest")
	proto.RegisterType((*MultiRowMutationProcessorResponse)(nil), "pb.MultiRowMutationProcessorResponse")
	proto.RegisterType((*MutateRowsRequest)(nil), "pb.MutateRowsRequest")
	proto.RegisterType((*MutateRowsResponse)(nil), "pb.MutateRowsResponse")
}

func init() { proto.RegisterFile("MultiRowMutation.proto", fileDescriptor_0d349fc249c590f4) }

var fileDescriptor_0d349fc249c590f4 = []byte{
	// 271 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x7c, 0x90, 0xbf, 0x4e, 0xf3, 0x40,
	0x10, 0xc4, 0x75, 0xc9, 0xf7, 0x35, 0x1b, 0x24, 0xc8, 0x29, 0x18, 0x2b, 0x0d, 0xc6, 0x34, 0x11,
	0xc5, 0x15, 0x69, 0xa1, 0x0a, 0x05, 0x14, 0x44, 0x42, 0x87, 0x44, 0x1b, 0x9d, 0x9d, 0xc5, 0xb6,
	0x14, 0x6e, 0x8f, 0xfb, 0x43, 0x5e, 0x21, 0x8f, 0xc1, 0xa3, 0x22, 0x7c, 0x09, 0x58, 0x16, 0xa2,
	0xdc, 0xdf, 0xce, 0xac, 0x66, 0x07, 0x92, 0x65, 0xd8, 0xf8, 0x46, 0xd2, 0x76, 0x19, 0xbc, 0xf2,
	0x0d, 0x69, 0x61, 0x2c, 0x79, 0xe2, 0x03, 0x53, 0x4c, 0x8f, 0x6e, 0x37, 0x0d, 0x6a, 0x1f, 0x49,
	0x9e, 0x43, 0xd6, 0xd7, 0x3e, 0x5a, 0x2a, 0xd1, 0x39, 0xb2, 0x12, 0xdf, 0x02, 0x3a, 0x9f, 0x5f,
	0xc2, 0xc5, 0x1f, 0x1a, 0x67, 0x48, 0x3b, 0xcc, 0x77, 0x0c, 0xc6, 0xed, 0x16, 0x25, 0x6d, 0xdd,
	0xde, 0xca, 0x6f, 0xe0, 0xe4, 0x75, 0x6f, 0x59, 0xd9, 0xc8, 0x52, 0x96, 0x0d, 0x67, 0xa3, 0xf9,
	0x58, 0x98, 0x42, 0x74, 0xce, 0x79, 0x92, 0xc7, 0x07, 0xe9, 0xc1, 0x7d, 0x0e, 0x23, 0x4d, 0xba,
	0xc4, 0x55, 0x65, 0x29, 0x98, 0x74, 0x90, 0xb1, 0xd9, 0x3f, 0x09, 0x2d, 0xba, 0xfb, 0x22, 0x7c,
	0x02, 0xff, 0xdb, 0x29, 0x1d, 0xb6, 0xab, 0x38, 0xe4, 0x13, 0xe0, 0xdd, 0x24, 0x31, 0xe0, 0xfc,
	0x19, 0xce, 0xfa, 0x5f, 0x3c, 0xa1, 0x7d, 0x6f, 0x4a, 0xe4, 0xd7, 0x00, 0x3f, 0x06, 0x7e, 0xfa,
	0x9d, 0xac, 0xfb, 0xca, 0x34, 0xe9, 0xe3, 0x78, 0x77, 0xf1, 0x00, 0x57, 0x64, 0x2b, 0xa1, 0x8c,
	0x2a, 0x6b, 0x14, 0xb5, 0x5a, 0x13, 0x19, 0x51, 0x17, 0xca, 0x61, 0xac, 0xb8, 0x08, 0x2f, 0xa2,
	0x42, 0x8d, 0x56, 0x79, 0x5c, 0x2f, 0x92, 0x5f, 0x9a, 0xf4, 0xe4, 0xee, 0xd9, 0x8e, 0xb1, 0x0f,
	0xc6, 0x3e, 0x03, 0x00, 0x00, 0xff, 0xff, 0x1d, 0xcf, 0x5a, 0xf8, 0xba, 0x01, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// MultiRowMutationServiceClient is the client API for MultiRowMutationService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type MultiRowMutationServiceClient interface {
	MutateRows(ctx context.Context, in *MutateRowsRequest, opts ...grpc.CallOption) (*MutateRowsResponse, error)
}

type multiRowMutationServiceClient struct {
	cc *grpc.ClientConn
}

func NewMultiRowMutationServiceClient(cc *grpc.ClientConn) MultiRowMutationServiceClient {
	return &multiRowMutationServiceClient{cc}
}

func (c *multiRowMutationServiceClient) MutateRows(ctx context.Context, in *MutateRowsRequest, opts ...grpc.CallOption) (*MutateRowsResponse, error) {
	out := new(MutateRowsResponse)
	err := c.cc.Invoke(ctx, "/pb.MultiRowMutationService/MutateRows", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// MultiRowMutationServiceServer is the server API for MultiRowMutationService service.
type MultiRowMutationServiceServer interface {
	MutateRows(context.Context, *MutateRowsRequest) (*MutateRowsResponse, error)
}

func RegisterMultiRowMutationServiceServer(s *grpc.Server, srv MultiRowMutationServiceServer) {
	s.RegisterService(&_MultiRowMutationService_serviceDesc, srv)
}

func _MultiRowMutationService_MutateRows_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(MutateRowsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MultiRowMutationServiceServer).MutateRows(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/pb.MultiRowMutationService/MutateRows",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MultiRowMutationServiceServer).MutateRows(ctx, req.(*MutateRowsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _MultiRowMutationService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "pb.MultiRowMutationService",
	HandlerType: (*MultiRowMutationServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "MutateRows",
			Handler:    _MultiRowMutationService_MutateRows_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "MultiRowMutation.proto",
}
