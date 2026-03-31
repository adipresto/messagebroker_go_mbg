package main

import (
	"context"
	"log"
	"mbg/api/proto"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := proto.NewBrokerServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	r, err := c.Push(ctx, &proto.PushRequest{Id: "GRPC-001", Payload: "Hello from gRPC"})
	if err != nil {
		log.Fatalf("could not push: %v", err)
	}
	log.Printf("Push Status: %v, Message: %s", r.Success, r.Message)

	res, err := c.Pop(ctx, &proto.PopRequest{})
	if err != nil {
		log.Fatalf("could not pop: %v", err)
	}
	log.Printf("Popped: %s - %s", res.Id, res.Payload)
}
