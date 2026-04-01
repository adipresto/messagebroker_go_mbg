package server

import (
	"embed"
	"encoding/json"
	"io/fs"
	"log"
	"mbg/config"
	"mbg/models"
	"mbg/pkg/broker"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

//go:embed dashboard/*
var dashboardContent embed.FS

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type HTTPServer struct {
	broker     *broker.Broker[string]
	dispatcher *broker.Dispatcher[string]
	mu         sync.Mutex
	conns      map[*websocket.Conn]bool
}

func NewHTTPServer(b *broker.Broker[string], d *broker.Dispatcher[string]) *HTTPServer {
	return &HTTPServer{
		broker:     b,
		dispatcher: d,
		conns:      make(map[*websocket.Conn]bool),
	}
}

func (s *HTTPServer) StartStreaming() {
	updates := s.broker.Subscribe()
	for range updates {
		s.broadcastStats()
	}
}

func (s *HTTPServer) broadcastStats() {
	s.mu.Lock()
	defer s.mu.Unlock()

	queueSize, dlqSize := s.broker.GetStats()
	cbState := s.broker.GetCBState()
	cbFailures, cbThreshold := s.broker.GetCBMetrics()

	// If dispatcher has its own overrides or state to report, we could check here,
	// but since they share the same CB instance, broker is sufficient.
	if s.dispatcher != nil {
		cbState = s.dispatcher.GetCBState()
		cbFailures, cbThreshold = s.dispatcher.GetCBMetrics()
	}

	stats := map[string]interface{}{
		"queue_size":   queueSize,
		"dlq_size":     dlqSize,
		"cb_state":     cbState,
		"cb_failures":  cbFailures,
		"cb_threshold": cbThreshold,
		"timestamp":    time.Now().Unix(),
	}

	data, _ := json.Marshal(stats)
	for conn := range s.conns {
		if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
			conn.Close()
			delete(s.conns, conn)
		}
	}
}

func (s *HTTPServer) Handler() http.Handler {
	mux := http.NewServeMux()

	// REST API
	mux.HandleFunc("POST /api/messages", s.handlePush)
	mux.HandleFunc("GET /api/messages", s.handlePop)
	mux.HandleFunc("GET /api/messages/all", s.handleAll)
	mux.HandleFunc("GET /api/stats", s.handleStats)
	mux.HandleFunc("GET /api/health", s.handleHealth)
	mux.HandleFunc("POST /api/targets", s.handleRegisterTarget)
	mux.HandleFunc("POST /api/reset", s.handleReset)
	mux.HandleFunc("POST /api/test/storage-path", s.handleSetStoragePath)
	mux.HandleFunc("POST /api/test/config", s.handleSetConfig)

	// WebSocket
	mux.HandleFunc("/ws", s.handleWS)

	// Dashboard
	content, _ := fs.Sub(dashboardContent, "dashboard")
	mux.Handle("/", http.FileServer(http.FS(content)))

	return mux
}

func (s *HTTPServer) handlePush(w http.ResponseWriter, r *http.Request) {
	var msg models.Message[string]
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	msg.CreatedAt = time.Now().Unix()

	if err := s.broker.Push(msg); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"status": "pushed"})
}

func (s *HTTPServer) handlePop(w http.ResponseWriter, r *http.Request) {
	msg, err := s.broker.Pop()
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	json.NewEncoder(w).Encode(msg)
}

func (s *HTTPServer) handleAll(w http.ResponseWriter, r *http.Request) {
	var msgs []models.Message[string]
	for m := range s.broker.All() {
		msgs = append(msgs, m)
	}
	json.NewEncoder(w).Encode(msgs)
}

func (s *HTTPServer) handleStats(w http.ResponseWriter, r *http.Request) {
	queueSize, dlqSize := s.broker.GetStats()
	cbState := s.broker.GetCBState()
	cbFailures, cbThreshold := s.broker.GetCBMetrics()

	if s.dispatcher != nil {
		cbState = s.dispatcher.GetCBState()
		cbFailures, cbThreshold = s.dispatcher.GetCBMetrics()
	}
	stats := map[string]interface{}{
		"queue_size":   queueSize,
		"dlq_size":     dlqSize,
		"cb_state":     cbState,
		"cb_failures":  cbFailures,
		"cb_threshold": cbThreshold,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

func (s *HTTPServer) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("upgrade:", err)
		return
	}

	s.mu.Lock()
	s.conns[conn] = true
	s.mu.Unlock()

	// Send initial stats
	s.broadcastStats()

	// Keep connection alive
	go func() {
		defer func() {
			s.mu.Lock()
			delete(s.conns, conn)
			s.mu.Unlock()
			conn.Close()
		}()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				break
			}
		}
	}()
}
func (s *HTTPServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (s *HTTPServer) handleRegisterTarget(w http.ResponseWriter, r *http.Request) {
	var body config.TargetConfig
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if body.Name == "" || body.URL == "" {
		http.Error(w, "name and url are required", http.StatusBadRequest)
		return
	}

	if s.dispatcher != nil {
		s.dispatcher.AddTarget(body)
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{
			"status": "target registered", 
			"name":   body.Name,
			"url":    body.URL,
		})
	} else {
		http.Error(w, "dispatcher not initialized", http.StatusInternalServerError)
	}
}

func (s *HTTPServer) handleReset(w http.ResponseWriter, r *http.Request) {
	s.broker.Reset()
	if s.dispatcher != nil {
		s.dispatcher.Reset()
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "system reset"})
}

func (s *HTTPServer) handleSetStoragePath(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Path string `json:"path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.broker.SetStoragePath(body.Path)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "storage path updated", "new_path": body.Path})
}

func (s *HTTPServer) handleSetConfig(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Threshold int `json:"threshold"`
		Timeout   int `json:"timeout_seconds"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.broker.ResetConfig(body.Threshold, time.Duration(body.Timeout)*time.Second)
	if s.dispatcher != nil {
		s.dispatcher.ResetConfig(body.Threshold, time.Duration(body.Timeout)*time.Second)
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "config updated",
		"threshold": body.Threshold,
		"timeout":   body.Timeout,
	})
}
