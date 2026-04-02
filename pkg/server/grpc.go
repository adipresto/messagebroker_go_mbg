package server

import (
	"context"
	"encoding/json"
	"fmt"
	"mbg/api/proto"
	"mbg/models"
	"mbg/pkg/broker"
	"time"
)

type GRPCServer struct {
	proto.UnimplementedBrokerServiceServer
	broker *broker.Broker[any]
}

func NewGRPCServer(b *broker.Broker[any]) *GRPCServer {
	return &GRPCServer{broker: b}
}

func (s *GRPCServer) Push(ctx context.Context, req *proto.PushRequest) (*proto.PushResponse, error) {
	msg := models.Message[any]{
		ID:        req.Id,
		Payload:   req.Payload, // string implicitly cast to any
		CreatedAt: time.Now().Unix(),
	}

	if err := s.broker.Push(msg); err != nil {
		return &proto.PushResponse{Success: false, Message: err.Error()}, nil
	}

	return &proto.PushResponse{Success: true, Message: "Message pushed successfully"}, nil
}

func (s *GRPCServer) Pop(ctx context.Context, req *proto.PopRequest) (*proto.PopResponse, error) {
	msg, err := s.broker.Pop()
	if err != nil {
		return nil, fmt.Errorf("failed to pop message: %w", err)
	}

	var payloadStr string
	switch v := msg.Payload.(type) {
	case string:
		payloadStr = v
	case nil:
		payloadStr = ""
	default:
		// If it's a structured object, marshal to JSON string for gRPC
		data, _ := json.Marshal(v)
		payloadStr = string(data)
	}

	return &proto.PopResponse{
		Id:        msg.ID,
		Payload:   payloadStr,
		CreatedAt: msg.CreatedAt,
	}, nil
}
