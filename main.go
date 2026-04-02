package main

import (
	"flag"
	"fmt"
	"log"
	"mbg/api/proto"
	"mbg/config"
	"mbg/pkg/broker"
	"mbg/pkg/circuitbreaker"
	"mbg/pkg/server"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/grpc"
)

func main() {
	// 0. Parse Flags
	noDispatcher := flag.Bool("no-dispatcher", false, "Disable the active dispatcher (active delivery)")
	flag.Parse()

	// 1. Load Configuration
	cfg, err := config.LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// 2. Setup Core Component (Isolated Circuit Breakers with Telemetry)
	os.MkdirAll("data/cb_log", 0755)
	cwd, _ := os.Getwd()
	cbLogPath := filepath.Join(cwd, "data", "cb_log", "cb_telemetry.log")

	log.Printf("Circuit Breaker Telemetry Log: %s", cbLogPath)

	storageCB := circuitbreaker.NewCircuitBreaker(
		"Storage",
		cfg.CircuitBreaker.Threshold,
		time.Duration(cfg.CircuitBreaker.TimeoutSeconds)*time.Second,
		cbLogPath,
	)
	networkCB := circuitbreaker.NewCircuitBreaker(
		"Network",
		cfg.CircuitBreaker.Threshold,
		time.Duration(cfg.CircuitBreaker.TimeoutSeconds)*time.Second,
		cbLogPath,
	)

	b := broker.NewBroker[string](cfg.Broker.StoragePath, cfg.Broker.DeadLetterPath, storageCB)

	// 3. Setup Dispatcher (Active Delivery)
	disp := broker.NewDispatcher(b, cfg, networkCB)
	if !*noDispatcher {
		go disp.Start()
	} else {
		log.Println("Maintenance Mode: Dispatcher is DISABLED.")
	}

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
