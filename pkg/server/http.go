package server

import (
	"embed"
	"encoding/json"
	"io/fs"
	"log"
	"mbg/config"
	"mbg/pkg/models"
	"mbg/pkg/broker"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http/pprof"
)

//go:embed dashboard/*
var dashboardContent embed.FS

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type HTTPServer struct {
	broker     *broker.Broker[any]
	dispatcher *broker.Dispatcher[any]
	mu         sync.Mutex
	conns      map[*websocket.Conn]bool
}

func NewHTTPServer(b *broker.Broker[any], d *broker.Dispatcher[any]) *HTTPServer {
	return &HTTPServer{
		broker:     b,
		dispatcher: d,
		conns:      make(map[*websocket.Conn]bool),
	}
}

func (s *HTTPServer) StartStreaming() {
	brokerUpdates := s.broker.Subscribe()
	var dispatcherUpdates chan struct{}
	if s.dispatcher != nil {
		dispatcherUpdates = s.dispatcher.Subscribe()
	}

	debounceTimer := time.NewTimer(100 * time.Millisecond)
	if !debounceTimer.Stop() {
		<-debounceTimer.C
	}
	needsBroadcast := false

	for {
		select {
		case <-brokerUpdates:
			needsBroadcast = true
			debounceTimer.Reset(100 * time.Millisecond)
		case <-dispatcherUpdates:
			if dispatcherUpdates != nil {
				needsBroadcast = true
				debounceTimer.Reset(100 * time.Millisecond)
			}
		case <-debounceTimer.C:
			if needsBroadcast {
				s.broadcastStats()
				needsBroadcast = false
			}
		}
	}
}

func (s *HTTPServer) broadcastStats() {
	s.mu.Lock()
	// Copy connections to a slice to avoid holding the lock during network IO
	conns := make([]*websocket.Conn, 0, len(s.conns))
	for conn := range s.conns {
		conns = append(conns, conn)
	}
	s.mu.Unlock()

	if len(conns) == 0 {
		return
	}

	queueSize, dlqSize := s.broker.GetStats()
	storageState := s.broker.GetCBState()
	storageFailures, storageThreshold := s.broker.GetCBMetrics()

	networkState := "N/A"
	networkFailures, networkThreshold := 0, 0
	if s.dispatcher != nil {
		networkState = s.dispatcher.GetCBState()
		networkFailures, networkThreshold = s.dispatcher.GetCBMetrics()
	}

	stats := map[string]interface{}{
		"queue_size": queueSize,
		"dlq_size":   dlqSize,
		"storage_cb": map[string]interface{}{
			"state":     storageState,
			"failures":  storageFailures,
			"threshold": storageThreshold,
		},
		"network_cb": map[string]interface{}{
			"state":     networkState,
			"failures":  networkFailures,
			"threshold": networkThreshold,
		},
		"targets":   s.dispatcher.GetTargets(),
		"timestamp": time.Now().Unix(),
	}

	data, _ := json.Marshal(stats)
	for _, conn := range conns {
		// Spawn a goroutine for each write to prevent head-of-line blocking by slow consumers.
		go func(c *websocket.Conn) {
			c.SetWriteDeadline(time.Now().Add(1 * time.Second))
			if err := c.WriteMessage(websocket.TextMessage, data); err != nil {
				log.Printf("WS write error (removing client): %v", err)
				c.Close()
				s.mu.Lock()
				delete(s.conns, c)
				s.mu.Unlock()
			}
		}(conn)
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
	mux.HandleFunc("GET /api/targets", s.handleGetTargets)
	mux.HandleFunc("POST /api/targets", s.handleRegisterTarget)
	mux.HandleFunc("POST /api/reset", s.handleReset)
	mux.HandleFunc("POST /api/test/storage-path", s.handleSetStoragePath)
	mux.HandleFunc("POST /api/test/config", s.handleSetConfig)

	// WebSocket
	mux.HandleFunc("/ws", s.handleWS)

	// Metrics & Profiling
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	// Dashboard
	content, _ := fs.Sub(dashboardContent, "dashboard")
	mux.Handle("/", http.FileServer(http.FS(content)))

	return mux
}

func (s *HTTPServer) handlePush(w http.ResponseWriter, r *http.Request) {
	var msg models.Message[any]
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
	var msgs []models.Message[any]
	for m := range s.broker.All() {
		msgs = append(msgs, m)
	}
	json.NewEncoder(w).Encode(msgs)
}

func (s *HTTPServer) handleStats(w http.ResponseWriter, r *http.Request) {
	queueSize, dlqSize := s.broker.GetStats()
	storageState := s.broker.GetCBState()
	storageFailures, storageThreshold := s.broker.GetCBMetrics()

	networkState := "N/A"
	networkFailures, networkThreshold := 0, 0
	if s.dispatcher != nil {
		networkState = s.dispatcher.GetCBState()
		networkFailures, networkThreshold = s.dispatcher.GetCBMetrics()
	}

	stats := map[string]interface{}{
		"queue_size": queueSize,
		"dlq_size":   dlqSize,
		"storage_cb": map[string]interface{}{
			"state":     storageState,
			"failures":  storageFailures,
			"threshold": storageThreshold,
		},
		"network_cb": map[string]interface{}{
			"state":     networkState,
			"failures":  networkFailures,
			"threshold": networkThreshold,
		},
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

	// Send initial stats in background to not block the upgrade
	go s.broadcastStats()

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
	log.Printf(" [HTTP] Incoming request: POST /api/targets")
	var body config.TargetConfig
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		log.Printf(" [HTTP] Decode failed: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if body.Name == "" || body.URL == "" {
		log.Printf(" [HTTP] Validation failed: name and url required")
		http.Error(w, "name and url are required", http.StatusBadRequest)
		return
	}

	if s.dispatcher != nil {
		log.Printf(" [HTTP] Registering target: %s (%s)", body.Name, body.URL)
		s.dispatcher.AddTarget(body)
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{
			"status": "target registered",
			"name":   body.Name,
			"url":    body.URL,
		})
		log.Printf(" [HTTP] Target %s registered successfully", body.Name)
	} else {
		log.Printf(" [HTTP] Error: Dispatcher not initialized")
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

func (s *HTTPServer) handleGetTargets(w http.ResponseWriter, r *http.Request) {
	if s.dispatcher == nil {
		http.Error(w, "dispatcher not initialized", http.StatusInternalServerError)
		return
	}

	targets := s.dispatcher.GetTargets()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(targets)
}
