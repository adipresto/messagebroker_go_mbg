package server

import (
	"context"
	"fmt"
	"mbg/api/proto"
	"mbg/models"
	"mbg/pkg/broker"
	"time"
)

type GRPCServer struct {
	proto.UnimplementedBrokerServiceServer
	broker *broker.Broker[string]
}

func NewGRPCServer(b *broker.Broker[string]) *GRPCServer {
	return &GRPCServer{broker: b}
}

func (s *GRPCServer) Push(ctx context.Context, req *proto.PushRequest) (*proto.PushResponse, error) {
	msg := models.Message[string]{
		ID:        req.Id,
		Payload:   req.Payload,
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

	return &proto.PopResponse{
		Id:        msg.ID,
		Payload:   msg.Payload,
		CreatedAt: msg.CreatedAt,
	}, nil
}
