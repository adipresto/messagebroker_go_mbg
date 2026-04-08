package main

import (
	"flag"
	"fmt"
	"log"
	"mbg/config"
	"mbg/pkg/broker"
	"mbg/pkg/circuitbreaker"
	"mbg/pkg/server"
	"mbg/pkg/storage"
	"mbg/proto/proto"
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
	cfg, err := config.LoadConfig("config/config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// 2. Setup Core Component (Isolated Circuit Breakers with Telemetry)
	os.MkdirAll("data/cb_log", 0755)
	os.MkdirAll("data/db", 0755)
	cwd, _ := os.Getwd()
	cbLogPath := filepath.Join(cwd, "data", "cb_log", "cb_telemetry.log")
	dbPath := filepath.Join(cwd, "data", "db", "mbg.db")

	log.Printf("Circuit Breaker Telemetry Log: %s", cbLogPath)
	log.Printf("SQLite database path: %s", dbPath)

	targetStorage, err := storage.NewSQLiteTargetStorage(dbPath)
	if err != nil {
		log.Fatalf("Failed to initialize storage: %v", err)
	}
	defer targetStorage.Close()

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

	b := broker.NewBroker[any](cfg.Broker.StoragePath, cfg.Broker.DeadLetterPath, storageCB)
	b.SetConfig(cfg.Broker.MaxQueueSize, cfg.Broker.AckTimeoutSeconds)

	// 3. Setup Dispatcher (Active Delivery)
	disp := broker.NewDispatcher[any](b, cfg, networkCB, targetStorage)
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
	httpServer := server.NewHTTPServer(b, disp, cfg)

	// Start WebSocket streaming loop in background
	go httpServer.StartStreaming()

	log.Printf("Starting HTTP & Dashboard server on http://localhost:%d", cfg.Broker.HTTPPort)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", cfg.Broker.HTTPPort), httpServer.Handler()); err != nil {
		log.Fatalf("failed to serve HTTP: %v", err)
	}
}
