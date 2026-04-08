package server

import (
	"embed"
	"encoding/json"
	"io/fs"
	"log"
	"mbg/config"
	"mbg/pkg/models"
	"mbg/pkg/broker"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/time/rate"
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

	limiter        *rate.Limiter
	maxPayloadSize int64
	allowedDomains []string
}

func NewHTTPServer(b *broker.Broker[any], d *broker.Dispatcher[any], cfg *config.Config) *HTTPServer {
	var limiter *rate.Limiter
	if cfg != nil && cfg.Gatekeeper.RateLimitRPS > 0 {
		burst := cfg.Gatekeeper.RateLimitBurst
		if burst == 0 {
			burst = int(cfg.Gatekeeper.RateLimitRPS) // default burst
		}
		limiter = rate.NewLimiter(rate.Limit(cfg.Gatekeeper.RateLimitRPS), burst)
	}

	var maxPayload int64 = 1024 * 1024 // default 1MB
	var domains []string
	if cfg != nil {
		if cfg.Gatekeeper.MaxPayloadSize > 0 {
			maxPayload = cfg.Gatekeeper.MaxPayloadSize
		}
		domains = cfg.Gatekeeper.AllowedDomains
	}

	return &HTTPServer{
		broker:         b,
		dispatcher:     d,
		conns:          make(map[*websocket.Conn]bool),
		limiter:        limiter,
		maxPayloadSize: maxPayload,
		allowedDomains: domains,
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
	mux.HandleFunc("POST /api/messages/ack", s.handleAck)
	mux.HandleFunc("POST /api/messages/nack", s.handleNack)
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
	if s.limiter != nil && !s.limiter.Allow() {
		http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
		return
	}

	// Payload Size Limit (Gatekeeper)
	r.Body = http.MaxBytesReader(w, r.Body, s.maxPayloadSize)

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

func (s *HTTPServer) handleAck(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	
	if err := s.broker.Acknowledge(body.ID); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "acknowledged"})
}

func (s *HTTPServer) handleNack(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	
	if err := s.broker.NegativeAcknowledge(body.ID); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "nacked"})
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

func isPrivateIP(ip net.IP) bool {
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return true
	}
	
	// Check RFC 1918 ranges
	privateIPBlocks := []*net.IPNet{
		{IP: net.ParseIP("10.0.0.0"), Mask: net.CIDRMask(8, 32)},
		{IP: net.ParseIP("172.16.0.0"), Mask: net.CIDRMask(12, 32)},
		{IP: net.ParseIP("192.168.0.0"), Mask: net.CIDRMask(16, 32)},
	}

	for _, block := range privateIPBlocks {
		if block.Contains(ip) {
			return true
		}
	}
	return false
}

func isLocalhostAllowed(host string, allowed []string) bool {
	for _, d := range allowed {
		if d == "localhost" || d == "127.0.0.1" {
			if host == d { return true }
		}
	}
	return false
}

func (s *HTTPServer) handleTestConfigGatekeeper(w http.ResponseWriter, r *http.Request) {
	var body struct {
		RateLimitRPS   float64  `json:"rate_limit_rps"`
		MaxPayloadSize int64    `json:"max_payload_size"`
		AllowedDomains []string `json:"allowed_domains"`
		MaxQueueSize   int      `json:"max_queue_size"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if body.RateLimitRPS > 0 {
		s.limiter = rate.NewLimiter(rate.Limit(body.RateLimitRPS), int(body.RateLimitRPS))
	} else {
		s.limiter = nil
	}
	
	if body.MaxPayloadSize > 0 {
		s.maxPayloadSize = body.MaxPayloadSize
	}
	
	s.allowedDomains = body.AllowedDomains
	
	if body.MaxQueueSize > 0 {
		s.broker.SetConfig(body.MaxQueueSize, 0) // Update queue size only
	}

	w.WriteHeader(http.StatusOK)
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

	// Gatekeeper: SSRF Protection
	if len(s.allowedDomains) > 0 {
		parsedURL, err := url.Parse(body.URL)
		if err != nil {
			http.Error(w, "invalid URL format", http.StatusBadRequest)
			return
		}
		
		host := parsedURL.Hostname()
		
		// 1. Block Private IPs directly
		if ip := net.ParseIP(host); ip != nil {
			if isPrivateIP(ip) && !isLocalhostAllowed(host, s.allowedDomains) {
				log.Printf(" [HTTP] SSRF trigger: Private IP %s rejected", host)
				http.Error(w, "private IP addresses not allowed", http.StatusForbidden)
				return
			}
		}

		// 2. Check Domain Allow-list
		allowed := false
		for _, domain := range s.allowedDomains {
			if host == domain {
				allowed = true
				break
			}
		}
		
		if !allowed {
			log.Printf(" [HTTP] SSRF trigger: %s not in allowed_domains", host)
			http.Error(w, "target domain not allowed by policy", http.StatusForbidden)
			return
		}
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
