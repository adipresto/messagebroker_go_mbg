package main

import (
	"fmt"
	"log"
	"mbg/api/proto"
	"mbg/config"
	"mbg/pkg/broker"
	"mbg/pkg/circuitbreaker"
	"mbg/pkg/server"
	"net"
	"net/http"
	"time"

	"google.golang.org/grpc"
)

func main() {
	// 1. Load Configuration
	cfg, err := config.LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// 2. Setup Core Component
	cb := circuitbreaker.NewCircuitBreaker(
		cfg.CircuitBreaker.Threshold,
		time.Duration(cfg.CircuitBreaker.TimeoutSeconds)*time.Second,
	)
	b := broker.NewBroker[string](cfg.Broker.StoragePath)

	// 3. Setup Dispatcher (Active Delivery)
	disp := broker.NewDispatcher(b, cfg, cb)
	go disp.Start()

	// 4. Start gRPC Server
	grpcServer := grpc.NewServer()
	s := server.NewGRPCServer(b)
	proto.RegisterBrokerServiceServer(grpcServer, s)

	lic, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Broker.GRPCPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	go func() {
		log.Printf("Starting gRPC server on port %d", cfg.Broker.GRPCPort)
		if err := grpcServer.Serve(lic); err != nil {
			log.Fatalf("failed to serve gRPC: %v", err)
		}
	}()

	// 5. Start HTTP & Dashboard Server
	httpServer := server.NewHTTPServer(b, disp)
	
	// Start WebSocket streaming loop in background
	go httpServer.StartStreaming()

	log.Printf("Starting HTTP & Dashboard server on http://localhost:%d", cfg.Broker.HTTPPort)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", cfg.Broker.HTTPPort), httpServer.Handler()); err != nil {
		log.Fatalf("failed to serve HTTP: %v", err)
	}
}
