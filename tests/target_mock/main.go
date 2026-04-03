package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"mbg/proto/proto"
	"net"
	"net/http"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// MockTargetServer mengimplementasikan TargetServiceServer (gRPC) dan HTTP Handler
type MockTargetServer struct {
	proto.UnimplementedTargetServiceServer
	mu sync.Mutex
}

// Deliver adalah implementasi gRPC service sesuai kontrak target.proto
func (s *MockTargetServer) Deliver(ctx context.Context, req *proto.DeliveryRequest) (*proto.DeliveryResponse, error) {
	log.Printf("[gRPC] Received message ID: %s, Payload: %s", req.Id, req.Payload)

	// Capturing Metadata
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		log.Printf("[gRPC Metadata] %v", md)
	}

	// Capturing payload field 'Headers'
	if req.Headers != "" {
		log.Printf("[gRPC Payload Headers] %s", req.Headers)
	}

	// Simulasi pemrosesan dan modifikasi payload balik sesuai permintaan user
	return &proto.DeliveryResponse{
		Id:          req.Id,
		Status:      "SUCCESS",
		ProcessedAt: time.Now().Unix(),
		Message:     fmt.Sprintf("Echo: %s", req.Payload),
	}, nil
}

// HTTP Handlers
func (s *MockTargetServer) handleWebhook(w http.ResponseWriter, r *http.Request) {
	// Log HTTP Headers
	log.Printf("[HTTP Webhook] Headers: %v", r.Header)

	var body map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	log.Printf("[HTTP Webhook] Received task for Y: %v", body)

	// Logika Smart Worker: Jika ada 'reply_to', kirim balasan balik ke MBG
	if replyTo, ok := body["reply_to"].(string); ok {
		log.Printf("[HTTP Webhook] Smart Worker detected 'reply_to': %s. Triggering reply to MBG...", replyTo)
		
		go func() {
			// Kasih sedikit jeda simulasi pemrosesan
			time.Sleep(2 * time.Second)

			replyPayload := map[string]interface{}{
				"target": replyTo,
				"payload": map[string]interface{}{
					"task_id": body["task_id"],
					"status":  "COMPLETED",
					"worker":  "mock-worker-y",
				},
			}
			
			data, _ := json.Marshal(replyPayload)
			// POST balik ke MBG (Default port 8081)
			resp, err := http.Post("http://localhost:8081/api/messages", "application/json", bytes.NewBuffer(data))
			if err != nil {
				log.Printf("[HTTP Webhook] Failed to send reply to MBG: %v", err)
				return
			}
			defer resp.Body.Close()
			log.Printf("[HTTP Webhook] Reply sent to MBG status: %s", resp.Status)
		}()
	}

	// Kembalikan payload dengan penambahan metadata (ACK ke MBG)
	body["processed_by"] = "mock-worker-y"
	body["status"] = "RECEIVED"

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(body)
}

func (s *MockTargetServer) handleCallback(w http.ResponseWriter, r *http.Request) {
	var body map[string]interface{}
	json.NewDecoder(r.Body).Decode(&body)
	log.Printf("[HTTP Callback] Service X received 'DONE' confirmation! Payload: %v", body)
	
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Acknowledged by X"))
}

func (s *MockTargetServer) handleFail(w http.ResponseWriter, r *http.Request) {
	log.Printf("[HTTP Fail] Simulating failure...")
	http.Error(w, "Planned simulation error", http.StatusInternalServerError)
}

func main() {
	server := &MockTargetServer{}

	// 1. Jalankan gRPC Server (Port 50052)
	go func() {
		lis, err := net.Listen("tcp", ":50052")
		if err != nil {
			log.Fatalf("failed to listen gRPC: %v", err)
		}
		grpcSrv := grpc.NewServer()
		proto.RegisterTargetServiceServer(grpcSrv, server)
		log.Println("Mock Target gRPC Server listening on :50052")
		if err := grpcSrv.Serve(lis); err != nil {
			log.Fatalf("failed to serve gRPC: %v", err)
		}
	}()

	// 2. Jalankan HTTP Webhook Server (Port 9090)
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/webhook", server.handleWebhook)
		mux.HandleFunc("/api/callback", server.handleCallback)
		log.Println("Mock Target HTTP Webhook listening on :9090")
		if err := http.ListenAndServe(":9090", mux); err != nil {
			log.Fatalf("HTTP server 9090 failed: %v", err)
		}
	}()

	// 3. Jalankan HTTP Fail Server (Port 9091)
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/fail", server.handleFail)
		log.Println("Mock Target HTTP Fail Simulator listening on :9091")
		if err := http.ListenAndServe(":9091", mux); err != nil {
			log.Fatalf("HTTP server 9091 failed: %v", err)
		}
	}()

	// 4. Jalankan HTTP Dead Server (Port 9092) - Selalu return 500
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/dead", func(w http.ResponseWriter, r *http.Request) {
			log.Printf("[HTTP Dead] Always returning 500 for DLQ testing...")
			http.Error(w, "Always fail", http.StatusInternalServerError)
		})
		log.Println("Mock Target HTTP Dead (DLQ) Simulator listening on :9092")
		if err := http.ListenAndServe(":9092", mux); err != nil {
			log.Fatalf("HTTP server 9092 failed: %v", err)
		}
	}()

	// Jaga agar main tidak exit
	select {}
}
