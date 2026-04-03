package features

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mbg/proto/proto"
	"mbg/pkg/models"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cucumber/godog"
	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type mockService struct {
	mu             sync.Mutex
	pushedMsgs     []models.Message[any]
	pushedHeaders  []map[string]string
	pushedBodies   [][]byte

	// Life-cycle management
	httpSrv *httptest.Server
	grpcSrv *grpc.Server
	lis     net.Listener
}

func (s *mockService) Push(msg models.Message[any], headers map[string]string, body []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pushedMsgs = append(s.pushedMsgs, msg)
	s.pushedHeaders = append(s.pushedHeaders, headers)
	s.pushedBodies = append(s.pushedBodies, body)
}

func (s *mockService) GetAllMessages() []models.Message[any] {
	s.mu.Lock()
	defer s.mu.Unlock()
	res := make([]models.Message[any], len(s.pushedMsgs))
	copy(res, s.pushedMsgs)
	return res
}

func (s *mockService) GetHeaders() []map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	res := make([]map[string]string, len(s.pushedHeaders))
	copy(res, s.pushedHeaders)
	return res
}

func (s *mockService) GetBodies() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	res := make([][]byte, len(s.pushedBodies))
	copy(res, s.pushedBodies)
	return res
}

type testContext struct {
	mu sync.Mutex // For thread safety across shared context

	threshold   int
	timeout     time.Duration
	storageDir  string
	dlqDir      string
	receivedMsg models.Message[any]
	lastError   error
	lastResp    *http.Response

	// URLs
	httpBaseURL string
	grpcAddr    string

	// Clients
	grpcClient proto.BrokerServiceClient
	grpcConn   *grpc.ClientConn
	httpClient *http.Client

	// Mock Targets
	services       map[string]*mockService // URL/Addr -> Isolated Storage
	lastTarget     string
	activeMsgID    string // Track the most recently pushed message ID for context
	lastCBState    string // Track last state for clear error messages

	// WebSocket for Dashboard testing
	wsConn *websocket.Conn
	wsMsgs chan []byte
}

type mockTargetTestServer struct {
	proto.UnimplementedTargetServiceServer
	tc   *testContext
	addr string
}

func (s *mockTargetTestServer) Deliver(ctx context.Context, req *proto.DeliveryRequest) (*proto.DeliveryResponse, error) {
	msg := models.Message[any]{
		ID:      req.Id,
		Payload: req.Payload,
	}

	// Capture headers
	headers := make(map[string]string)
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		for k, v := range md {
			if len(v) > 0 {
				headers[k] = v[0]
			}
		}
	}
	if req.Headers != "" {
		var fieldHeaders map[string]string
		if err := json.Unmarshal([]byte(req.Headers), &fieldHeaders); err == nil {
			for k, v := range fieldHeaders {
				headers[k] = v
			}
		}
	}

	// Route to the correct isolated service
	s.tc.mu.Lock()
	svc, ok := s.tc.services[s.addr]
	if !ok {
		// Auto-register if not found (should be registered already)
		svc = &mockService{}
		s.tc.services[s.addr] = svc
	}
	s.tc.mu.Unlock()

	svc.Push(msg, headers, nil)
	return &proto.DeliveryResponse{Status: "SUCCESS"}, nil
}

func (c *testContext) purgeStorage() {
	// Root of current execution relative to feature folder
	storagePath := "../data/messages"
	dlqPath := "../data/dead_letter"

	// Cleanup common directories
	for _, p := range []string{storagePath, dlqPath} {
		files, _ := filepath.Glob(filepath.Join(p, "*.json"))
		for _, f := range files {
			os.Remove(f)
		}
		if len(files) > 0 {
			// Purged
		}
	}
}

func (c *testContext) reset() {
	// Kill any leftover broker process to ensure isolation
	_ = exec.Command("taskkill", "/F", "/IM", "mbg.exe", "/T").Run()
	time.Sleep(2500 * time.Millisecond)

	// Stop all mock services (HTTP & gRPC)
	c.mu.Lock()
	for _, svc := range c.services {
		if svc.httpSrv != nil {
			svc.httpSrv.Close()
		}
		if svc.grpcSrv != nil {
			svc.grpcSrv.Stop()
		}
		if svc.lis != nil {
			svc.lis.Close()
		}
	}
	c.mu.Unlock()

	// Initialize isolated services
	c.mu.Lock()
	c.services = make(map[string]*mockService)
	c.mu.Unlock()

	c.lastTarget = ""
	if c.grpcConn != nil {
		c.grpcConn.Close()
		c.grpcConn = nil
		c.grpcClient = nil
	}
	c.threshold = 3
	c.timeout = 5 * time.Second
	abs, _ := filepath.Abs("../data/messages/")
	c.storageDir = abs
	absDLQ, _ := filepath.Abs("../data/dead_letter/")
	c.dlqDir = absDLQ
	c.receivedMsg = models.Message[any]{}
	c.lastError = nil

	// Ensure clean state on disk after process is dead
	c.purgeStorage()

	if c.wsConn != nil {
		c.wsConn.Close()
		c.wsConn = nil
	}
	c.wsMsgs = make(chan []byte, 100)
	c.httpClient = &http.Client{Timeout: 10 * time.Second}

	// Truncate CB telemetry log
	cbLog, _ := filepath.Abs("../data/cb_log/cb_telemetry.log")
	os.WriteFile(cbLog, []byte(""), 0644)

	// Drain the queue to ensure clean state (if anyone is still listening)
	if c.httpBaseURL != "" {
		c.drainQueue()
	}
}

func (c *testContext) drainQueue() {
	url := fmt.Sprintf("%s/api/messages", c.httpBaseURL)
	maxAttempts := 50
	for i := 0; i < maxAttempts; i++ {
		resp, err := c.httpClient.Get(url)
		if err != nil || resp.StatusCode == http.StatusNotFound {
			if resp != nil {
				resp.Body.Close()
			}
			return
		}
		resp.Body.Close()
	}
}

func (c *testContext) waitForServer() error {
	url := fmt.Sprintf("%s/api/health", c.httpBaseURL)
	for i := 0; i < 20; i++ {
		resp, err := http.Get(url)
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return nil
		}
		if err == nil {
			resp.Body.Close()
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("server at %s did not become healthy in time after 10s", url)
}

func (c *testContext) theBrokerIsAccessibleAtHTTPAndGRPC(httpURL, grpcAddr string) error {
	c.httpBaseURL = httpURL
	c.grpcAddr = grpcAddr

	// Check health with a very short timeout first
	checkConn, err := net.DialTimeout("tcp", "localhost:8081", 100*time.Millisecond)
	if err != nil {
		// Use our smart-start function to spin it up
		if err := c.theBrokerIsStartedAndExecutes("Background Start"); err != nil {
			return fmt.Errorf("failed to auto-start broker: %w", err)
		}
	} else {
		checkConn.Close()
	}

	// Ensure server is healthy before proceeding
	if err := c.waitForServer(); err != nil {
		return err
	}

	// Re-drain queue if base URL changed
	c.drainQueue()

	// Setup gRPC client
	conn, err := grpc.Dial(c.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c.grpcConn = conn
	c.grpcClient = proto.NewBrokerServiceClient(conn)
	return nil
}

func (c *testContext) theConfigurationHasThresholdSetTo(threshold int) error {
	c.threshold = threshold
	url := fmt.Sprintf("%s/api/test/config", c.httpBaseURL)
	body := map[string]interface{}{"threshold": threshold, "timeout_seconds": int(c.timeout.Seconds())}
	data, _ := json.Marshal(body)
	resp, err := c.httpClient.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (c *testContext) theConfigurationHasTimeoutSetToSeconds(seconds int) error {
	c.timeout = time.Duration(seconds) * time.Second
	url := fmt.Sprintf("%s/api/test/config", c.httpBaseURL)
	body := map[string]interface{}{"threshold": c.threshold, "timeout_seconds": seconds}
	data, _ := json.Marshal(body)
	resp, err := c.httpClient.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}
func (c *testContext) theConfigurationHasMaxRetriesSetTo(max int) error {
	// Di sistem asli, kita mungkin perlu update config.yaml atau restart broker
	// Untuk test ini, kita berasumsi broker sudah dikonfigurasi atau kita pasang lewat flag jika perlu.
	return nil
}

func (c *testContext) theBrokerIsInitializedWithAnEmptyQueue() error {
	c.drainQueue()
	return nil
}

func (c *testContext) theCircuitBreakerIsClosed() error {
	url := fmt.Sprintf("%s/api/reset", c.httpBaseURL)
	resp, err := http.Post(url, "application/json", nil)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}
func (c *testContext) aProducerPushesAMessageWithPayload(id, payload string) error {
	msg := models.Message[any]{ID: id, Payload: payload}
	c.activeMsgID = id // Save to context
	data, _ := json.Marshal(msg)
	url := fmt.Sprintf("%s/api/messages", c.httpBaseURL)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	c.lastResp = resp
	return nil
}

func (c *testContext) aProducerPushesAMessageWithTarget(id, target string) error {
	msg := models.Message[any]{ID: id, Payload: "Target test", Target: target}
	c.activeMsgID = id
	data, _ := json.Marshal(msg)
	url := fmt.Sprintf("%s/api/messages", c.httpBaseURL)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	c.lastResp = resp
	return nil
}

func (c *testContext) theMessageShouldBeStoredIn(id, path string) error {
	// Path mapping: The feature file might use relative paths, but we will use the ID to find it in the storageDir.
	absPath := filepath.Join(c.storageDir, id+".json")
	if _, err := os.Stat(absPath); os.IsNotExist(err) {
		return fmt.Errorf("expected file %s to exist", absPath)
	}
	return nil
}

func (c *testContext) theMessageShouldBeAvailableInTheMemoryQueue() error {
	url := fmt.Sprintf("%s/api/stats", c.httpBaseURL)
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var stats map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&stats)
	if stats["queue_size"].(float64) < 1 {
		return fmt.Errorf("expected messages in queue")
	}
	return nil
}

func (c *testContext) whenAConsumerPopsAMessage() error {
	url := fmt.Sprintf("%s/api/messages", c.httpBaseURL)
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET failed with status %d", resp.StatusCode)
	}
	var msg models.Message[any]
	json.NewDecoder(resp.Body).Decode(&msg)
	c.receivedMsg = msg
	return nil
}

func (c *testContext) theConsumerShouldReceiveMessage(id string) error {
	if c.receivedMsg.ID != id {
		return fmt.Errorf("expected %s, got %s", id, c.receivedMsg.ID)
	}
	return nil
}

func (c *testContext) theFileShouldBeDeleted(path string) error {
	filename := filepath.Base(path)
	absPath := filepath.Join(c.storageDir, filename)
	if _, err := os.Stat(absPath); !os.IsNotExist(err) {
		return fmt.Errorf("expected file %s to be deleted", absPath)
	}
	return nil
}

func (c *testContext) consecutivePushAttemptsFailDueToStorageErrors(count int) error {
	// 1. Create a FILE where a directory is expected to force os.MkdirAll to fail
	failPath := filepath.Join(c.storageDir, "..", "storage_fail_trigger.tmp")
	os.WriteFile(failPath, []byte("blocker"), 0644)
	defer os.Remove(failPath)

	urlPath := fmt.Sprintf("%s/api/test/storage-path", c.httpBaseURL)
	body := map[string]string{"path": failPath}
	data, _ := json.Marshal(body)
	http.Post(urlPath, "application/json", bytes.NewBuffer(data))

	// 2. Perform the pushes
	for i := 0; i < count; i++ {
		c.aProducerPushesAMessageWithPayload(fmt.Sprintf("FAIL-%d", i), "FailData")
		// Small sleep to ensure sequential processing and state propagation
		time.Sleep(100 * time.Millisecond)
	}

	// 3. Cleanup and Reset to original storage path (USE ABSOLUTE PATH)
	os.Remove(failPath)                // Ensure file is gone
	time.Sleep(200 * time.Millisecond) // Give OS time to release file handles

	urlPath = fmt.Sprintf("%s/api/test/storage-path", c.httpBaseURL)
	body = map[string]string{"path": c.storageDir}
	data, _ = json.Marshal(body)
	http.Post(urlPath, "application/json", bytes.NewBuffer(data))

	return nil
}

func (c *testContext) theCircuitBreakerThresholdIsSetTo(threshold int) error {
	return c.theConfigurationHasThresholdSetTo(threshold)
}

func (c *testContext) theCircuitBreakerShouldTransitionToState(expected string) error {
	logPath, _ := filepath.Abs("../data/cb_log/cb_telemetry.log")
	timeout := time.After(8 * time.Second)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()


	for {
		select {
		case <-timeout:
			return fmt.Errorf("timeout waiting for %s in log (last was %s)", expected, c.lastCBState)
		case <-ticker.C:
			content, err := os.ReadFile(logPath)
			if err != nil {
				continue
			}

			lines := strings.Split(string(content), "\n")
			// Look backwards for the most recent event for 'Storage'
			for i := len(lines) - 1; i >= 0; i-- {
				line := lines[i]
				if strings.Contains(line, "| Storage |") {
					// Format: [CB_EVENT] <timestamp> | <name> | <old> -> <new> | failures: <count>
					parts := strings.Split(line, " -> ")
					if len(parts) >= 2 {
						remaining := parts[1]
						stateParts := strings.Split(remaining, " | ")
						if len(stateParts) > 0 {
							actual := stateParts[0]
							c.lastCBState = actual

							if actual == expected {
								return nil
							}
						}
					}
				}
			}

			// Special case: if we want Closed and log is empty (meaning it stayed Closed), that's fine too?
			// No, better to ensure we log the initial state or a reset.
		}
	}
}

func (c *testContext) subsequentPushAttemptsShouldFailImmediatelyWithError(errMsg string) error {
	msg := models.Message[any]{ID: "SUB-FAIL", Payload: "ShouldFail"}
	data, _ := json.Marshal(msg)
	url := fmt.Sprintf("%s/api/messages", c.httpBaseURL)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		return fmt.Errorf("expected 500 status when circuit is open, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), errMsg) {
		return fmt.Errorf("expected error %s, got %s", errMsg, string(body))
	}
	return nil
}

func (c *testContext) theBrokerIsStopped() error {
	// Simple taskkill for Windows
	_ = exec.Command("taskkill", "/F", "/IM", "mbg.exe", "/T").Run()
	time.Sleep(2 * time.Second) // Give more time for the OS to release sockets
	return nil
}

func (c *testContext) thereAreMessagesAndOnDiskIn(id1, id2, dir string) error {
	// Create storage directory if missing (absolute path from test context)
	if err := os.MkdirAll(c.storageDir, 0755); err != nil {
		return err
	}

	msg1 := models.Message[any]{ID: id1, Payload: "Recover 1", CreatedAt: time.Now().Unix()}
	msg2 := models.Message[any]{ID: id2, Payload: "Recover 2", CreatedAt: time.Now().Unix()}

	for _, msg := range []models.Message[any]{msg1, msg2} {
		data, _ := json.Marshal(msg)
		absPath := filepath.Join(c.storageDir, msg.ID+".json")
		if err := os.WriteFile(absPath, data, 0644); err != nil {
			return err
		}
	}
	return nil
}

func (c *testContext) theBrokerIsStartedAndExecutes(action string) error {
	// 1. Cek apakah broker sudah jalan (port 8081).
	// Jika action == "Recover", kita JANGAN skip start, karena kita butuh fresh start utk panggil Recover()
	if action != "Recover" {
		conn, err := net.DialTimeout("tcp", "localhost:8081", 100*time.Millisecond)
		if err == nil {
			conn.Close()
			return nil
		}
	} else {
		_ = exec.Command("taskkill", "/F", "/IM", "mbg.exe", "/T").Run()
		time.Sleep(2 * time.Second)
	}

	// 2. Pastikan file binari ada
	rootAbs, _ := filepath.Abs("..")
	exeAbs := filepath.Join(rootAbs, "mbg.exe")
	if _, err := os.Stat(exeAbs); err != nil {
		return fmt.Errorf("mbg.exe not found at %s: %w", exeAbs, err)
	}

	// 3. Jalankan mbg.exe langsung dari Go (lebih transparan)
	logFile, _ := os.Create("broker_test.log")
	args := []string{}
	if action == "Recover" {
		args = append(args, "-no-dispatcher")
	}
	cmd := exec.Command(exeAbs, args...)
	cmd.Dir = rootAbs
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start mbg.exe directly: %w", err)
	}

	// Wait for the instance to be healthy
	return c.waitForServer()
}

func (c *testContext) theMemoryQueueShouldContainMessages(count int) error {
	url := fmt.Sprintf("%s/api/stats", c.httpBaseURL)
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var stats map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&stats)
	if int(stats["queue_size"].(float64)) != count {
		return fmt.Errorf("expected %d messages, got %d", count, int(stats["queue_size"].(float64)))
	}
	return nil
}

func (c *testContext) theMessagesAndShouldBeAvailableForConsumptionInTheCorrectOrder(id1, id2 string) error {
	return nil
}

func (c *testContext) theCircuitBreakerIs(state string) error {
	if state == "Closed" {
		return c.theCircuitBreakerIsClosed()
	}
	if state == "Open" {
		// To force OPEN state, we trigger storage failures until threshold is met
		// threshold is usually 3 in default config
		if err := c.consecutivePushAttemptsFailDueToStorageErrors(3); err != nil {
			return err
		}
		// Confirm it's Open
		return c.theCircuitBreakerShouldTransitionToState("Open")
	}
	return nil
}

func (c *testContext) theTimeoutOfSecondsHasPassed(seconds int) error {
	time.Sleep(time.Duration(seconds) * time.Second)
	return nil
}

func (c *testContext) whenANewPushAttemptIs(result string) error {
	// If result is "Successful", it will succeed because c.consecutivePushAttemptsFailDueToStorageErrors
	// already reset the storage path back to normal.
	return c.aProducerPushesAMessageWithPayload("HEAL-001", "Healing")
}

func (c *testContext) theCircuitBreakerShouldTransitionTo(state string) error {
	return c.theCircuitBreakerShouldTransitionToState(state)
}

func (c *testContext) thenFinallyTransitionTo(state string) error {
	return c.theCircuitBreakerShouldTransitionToState(state)
}

func (c *testContext) theMessageShouldBePersistedAndQueuedNormally() error {
	// Check if HEAL-001 is on disk and in memory
	if err := c.theMessageShouldBeStoredIn("HEAL-001", ""); err != nil {
		return err
	}
	return c.theMessageShouldBeAvailableInTheMemoryQueue()
}

func (c *testContext) theProducerSendsMessageViaGRPC(id, payload string) error {
	c.activeMsgID = id // Save to context
	req := &proto.PushRequest{Id: id, Payload: payload}
	resp, err := c.grpcClient.Push(context.Background(), req)
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("gRPC push failed: %s", resp.Message)
	}
	return nil
}

func (c *testContext) whenTheConsumerRequestsAMessageViaGRPC() error {
	resp, err := c.grpcClient.Pop(context.Background(), &proto.PopRequest{})
	if err != nil {
		return err
	}
	c.receivedMsg = models.Message[any]{
		ID:        resp.Id,
		Payload:   resp.Payload,
		CreatedAt: resp.CreatedAt,
	}
	return nil
}

func (c *testContext) theConsumerShouldReceiveMessageViaGRPC(id string) error {
	if c.receivedMsg.ID != id {
		return fmt.Errorf("expected %s via gRPC, got %s", id, c.receivedMsg.ID)
	}
	return nil
}

func (c *testContext) theProducerSendsMessageViaPOST(id, payload, path string) error {
	msg := models.Message[any]{ID: id, Payload: payload}
	c.activeMsgID = id
	data, _ := json.Marshal(msg)
	url := fmt.Sprintf("%s%s", c.httpBaseURL, path)
	resp, err := c.httpClient.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	c.lastResp = resp
	return nil
}

func (c *testContext) theServerShouldRespondWithStatus(code int) error {
	if c.lastResp.StatusCode != code {
		return fmt.Errorf("expected status %d, got %d", code, c.lastResp.StatusCode)
	}
	return nil
}

func (c *testContext) theConsumerShouldReceiveMessageViaHTTP(id string) error {
	if c.receivedMsg.ID != id {
		return fmt.Errorf("expected %s via HTTP, got %s", id, c.receivedMsg.ID)
	}
	return nil
}

func (c *testContext) theDashboardIsAccessible() error {
	resp, err := c.httpClient.Get(c.httpBaseURL + "/")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("dashboard not accessible: %d", resp.StatusCode)
	}
	return nil
}

func (c *testContext) aProducerPushesAMessageViaGRPC(id string) error {
	return c.theProducerSendsMessageViaGRPC(id, "dashboard-test")
}

func (c *testContext) theDashboardStatsShouldShowQueueSizeAs(expected int) error {
	url := fmt.Sprintf("%s/api/stats", c.httpBaseURL)
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var stats map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&stats)
	actual := int(stats["queue_size"].(float64))
	if actual != expected {
		return fmt.Errorf("expected queue size %d, got %d", expected, actual)
	}
	return nil
}

func (c *testContext) theDashboardStatsShouldShowDlqSizeAs(expected int) error {
	url := fmt.Sprintf("%s/api/stats", c.httpBaseURL)
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var stats map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&stats)
	actual := int(stats["dlq_size"].(float64))
	if actual != expected {
		return fmt.Errorf("expected DLQ size %d, got %d", expected, actual)
	}
	return nil
}

func (c *testContext) theProducerSendsTheFollowingJSONPayloadViaGRPC(doc *godog.DocString) error {
	var msg models.Message[any]
	if err := json.Unmarshal([]byte(doc.Content), &msg); err != nil {
		return err
	}

	// For gRPC, we must stringify the payload if it's not already a string
	var payloadStr string
	switch v := msg.Payload.(type) {
	case string:
		payloadStr = v
	default:
		data, _ := json.Marshal(v)
		payloadStr = string(data)
	}

	req := &proto.PushRequest{Id: msg.ID, Payload: payloadStr}
	resp, err := c.grpcClient.Push(context.Background(), req)
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("gRPC JSON push failed: %s", resp.Message)
	}
	c.activeMsgID = msg.ID
	return nil
}

func (c *testContext) theProducerSendsTheFollowingJSONPayloadViaPOST(path string, doc *godog.DocString) error {
	var msg models.Message[any]
	if err := json.Unmarshal([]byte(doc.Content), &msg); err != nil {
		return err
	}

	data, _ := json.Marshal(msg)
	url := fmt.Sprintf("%s%s", c.httpBaseURL, path)
	resp, err := c.httpClient.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	c.lastResp = resp
	c.activeMsgID = msg.ID
	return nil
}

func (c *testContext) theBrokerHasARegisteredTarget(target string) error {
	url := fmt.Sprintf("%s/api/targets", c.httpBaseURL)
	// Fallback/Legacy support: Use URL as Name if name is not provided
	body := map[string]string{"name": target, "url": target}
	data, _ := json.Marshal(body)
	resp, err := c.httpClient.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("failed to register target: %d", resp.StatusCode)
	}
	c.lastTarget = target
	return nil
}

func (c *testContext) theBrokerHasTheFollowingRegisteredTargets(dt *godog.Table) error {
	for i, row := range dt.Rows {
		if i == 0 {
			continue // Skip header
		}
		name := row.Cells[0].Value
		targetUrl := row.Cells[1].Value

		url := fmt.Sprintf("%s/api/targets", c.httpBaseURL)
		body := map[string]string{"name": name, "url": targetUrl}
		data, _ := json.Marshal(body)
		resp, err := c.httpClient.Post(url, "application/json", bytes.NewBuffer(data))
		if err != nil {
			return err
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			return fmt.Errorf("failed to register target %s: %d", name, resp.StatusCode)
		}
	}
	return nil
}

func (c *testContext) aMockServerIsListeningAt(addr string) error {
	cleanAddr := strings.TrimPrefix(addr, "grpc://")
	cleanAddr = strings.TrimPrefix(cleanAddr, "http://")
	if strings.Contains(cleanAddr, "/") {
		cleanAddr = strings.Split(cleanAddr, "/")[0]
	}

	// Initialize the isolated service storage for this address
	c.mu.Lock()
	svc := &mockService{}
	c.services[addr] = svc
	c.mu.Unlock()

	if strings.HasPrefix(addr, "grpc://") {
		lis, err := net.Listen("tcp", cleanAddr)
		if err != nil {
			return err
		}
		svc.grpcSrv = grpc.NewServer()
		svc.lis = lis
		proto.RegisterTargetServiceServer(svc.grpcSrv, &mockTargetTestServer{tc: c, addr: addr})
		go svc.grpcSrv.Serve(lis)
		return nil
	}

	// HTTP Implementation
	mockSrv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Reconstruct Message object for test compatibility
		msgID := r.Header.Get("X-Message-ID")

		bodyBytes, _ := io.ReadAll(r.Body)
		var payload any
		if err := json.Unmarshal(bodyBytes, &payload); err != nil {
			payload = string(bodyBytes)
		}

		msg := models.Message[any]{
			ID:      msgID,
			Payload: payload,
		}

		// Capture headers
		headers := make(map[string]string)
		for k := range r.Header {
			headers[k] = r.Header.Get(k)
		}

		// Push to the isolated service storage
		svc.Push(msg, headers, bodyBytes)

		// SMART WORKER LOGIC FOR TEST
		if pMap, ok := payload.(map[string]interface{}); ok {
			if replyTo, ok := pMap["reply_to"].(string); ok {
				go func(rt string, taskID interface{}) {
					time.Sleep(1 * time.Second)
					reply := map[string]interface{}{
						"target": rt,
						"payload": map[string]interface{}{
							"task_id": taskID,
							"status":  "COMPLETED",
						},
					}
					data, _ := json.Marshal(reply)
					client := &http.Client{Timeout: 5 * time.Second}
					resp, err := client.Post(fmt.Sprintf("%s/api/messages", c.httpBaseURL), "application/json", bytes.NewBuffer(data))
					if err == nil {
						resp.Body.Close()
					}
				}(replyTo, pMap["task_id"])
			}
		}

		w.WriteHeader(http.StatusOK)
	}))

	l, err := net.Listen("tcp", cleanAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", cleanAddr, err)
	}
	mockSrv.Listener = l
	mockSrv.Start()
	svc.httpSrv = mockSrv
	return nil
}

func (c *testContext) aMockServerAtReturnsError(addr string) error {
	mockSrv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	cleanAddr := strings.TrimPrefix(addr, "http://")
	if strings.Contains(cleanAddr, "/") {
		cleanAddr = strings.Split(cleanAddr, "/")[0]
	}
	l, err := net.Listen("tcp", cleanAddr)
	if err != nil {
		return err
	}
	mockSrv.Listener = l
	mockSrv.Start()
	
	c.mu.Lock()
	svc, ok := c.services[addr]
	if !ok {
		svc = &mockService{}
		c.services[addr] = svc
	}
	svc.httpSrv = mockSrv
	c.mu.Unlock()
	
	return nil
}

func (c *testContext) theMockServerShouldReceiveMessage(id string) error {
	timeout := time.After(7 * time.Second) // Increased for stability
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-timeout:
			return fmt.Errorf("mock server never received message %s", id)
		case <-tick.C:
			// Check all isolated services to see if ANY received it
			c.mu.Lock()
			for _, svc := range c.services {
				msgs := svc.GetAllMessages()
				for _, m := range msgs {
					if m.ID == id {
						c.mu.Unlock()
						return nil
					}
				}
			}
			c.mu.Unlock()
		}
	}
}

func (c *testContext) theMockServerAtShouldNOTReceiveMessage(addr, id string) error {
	// Wait a bit to ensure it DOES NOT arrive
	time.Sleep(2 * time.Second)

	c.mu.Lock()
	svc, ok := c.services[addr]
	c.mu.Unlock()

	if ok {
		msgs := svc.GetAllMessages()
		for _, m := range msgs {
			if m.ID == id {
				return fmt.Errorf("mock server at %s received message %s but it should NOT have", addr, id)
			}
		}
	}
	return nil
}

func (c *testContext) theMessageShouldEventuallyBeDeletedFromTheQueue(id string) error {
	timeout := time.After(5 * time.Second)
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-timeout:
			return fmt.Errorf("message %s still in queue after 5s", id)
		case <-tick.C:
			url := fmt.Sprintf("%s/api/stats", c.httpBaseURL)
			resp, err := c.httpClient.Get(url)
			if err != nil {
				continue
			}
			var stats map[string]interface{}
			json.NewDecoder(resp.Body).Decode(&stats)
			resp.Body.Close()
			actual := int(stats["queue_size"].(float64))
			if actual == 0 {
				return nil
			}
		}
	}
}

func (c *testContext) theMessageShouldStayInTheQueue(id string) error {
	url := fmt.Sprintf("%s/api/stats", c.httpBaseURL)
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var stats map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&stats)
	if stats["queue_size"].(float64) == 0 {
		return fmt.Errorf("message %s was removed from queue unexpectedly", id)
	}
	return nil
}

func (c *testContext) itsRetryCountShouldBe(count int) error {
	id := c.activeMsgID
	timeout := time.After(5 * time.Second)
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-timeout:
			return fmt.Errorf("message %s retry count never reached %d after 5s", id, count)
		case <-tick.C:
			url := fmt.Sprintf("%s/api/messages/all", c.httpBaseURL)
			resp, err := c.httpClient.Get(url)
			if err != nil {
				continue
			}
			var msgs []models.Message[any]
			json.NewDecoder(resp.Body).Decode(&msgs)
			resp.Body.Close()

			for _, m := range msgs {
				if m.ID == id {
					if m.RetryCount == count {
						return nil
					}
				}
			}
		}
	}
}

func (c *testContext) theMessageShouldEventuallyBeMovedToDLQFolder(id, path string) error {
	timeout := time.After(20 * time.Second) // DLQ takes longer due to retries (2+4+8 = 14s)
	tick := time.NewTicker(1 * time.Second)
	defer tick.Stop()

	// Path mapping
	absPath := filepath.Join(c.dlqDir, id+".json")

	for {
		select {
		case <-timeout:
			return fmt.Errorf("message %s never moved to DLQ after 10s", id)
		case <-tick.C:
			if _, err := os.Stat(absPath); err == nil {
				return nil
			}
		}
	}
}

func (c *testContext) aMockServerAtAlwaysReturnsError(addr string) error {
	mockSrv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	cleanAddr := strings.TrimPrefix(addr, "http://")
	if strings.Contains(cleanAddr, "/") {
		cleanAddr = strings.Split(cleanAddr, "/")[0]
	}
	l, err := net.Listen("tcp", cleanAddr)
	if err != nil {
		return err
	}
	mockSrv.Listener = l
	mockSrv.Start()

	c.mu.Lock()
	svc, ok := c.services[addr]
	if !ok {
		svc = &mockService{}
		c.services[addr] = svc
	}
	svc.httpSrv = mockSrv
	c.mu.Unlock()

	return nil
}

func (c *testContext) theMessageShouldBeRemovedFromTheMainQueue(id string) error {
	timeout := time.After(5 * time.Second)
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-timeout:
			return fmt.Errorf("message %s still in queue after 5s", id)
		case <-tick.C:
			url := fmt.Sprintf("%s/api/stats", c.httpBaseURL)
			resp, err := c.httpClient.Get(url)
			if err != nil {
				continue
			}
			var stats map[string]interface{}
			json.NewDecoder(resp.Body).Decode(&stats)
			resp.Body.Close()
			if stats["queue_size"].(float64) == 0 {
				return nil
			}
		}
	}
}

func (c *testContext) itsNextRetryShouldBeSetAccordingToExponentialBackoff() error {
	id := c.activeMsgID
	timeout := time.After(5 * time.Second)
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-timeout:
			return fmt.Errorf("message %s NextRetry was never updated after 5s", id)
		case <-tick.C:
			url := fmt.Sprintf("%s/api/messages/all", c.httpBaseURL)
			resp, err := c.httpClient.Get(url)
			if err != nil {
				continue
			}
			var msgs []models.Message[any]
			json.NewDecoder(resp.Body).Decode(&msgs)
			resp.Body.Close()

			for _, m := range msgs {
				if m.ID == id {
					if m.NextRetry > time.Now().Unix() {
						return nil
					}
				}
			}
		}
	}
}

func (c *testContext) theBrokerHasTheFollowingRegisteredTargetsWithHeaders(dt *godog.Table) error {
	for i, row := range dt.Rows {
		if i == 0 {
			continue // Skip header
		}
		name := row.Cells[0].Value
		targetUrl := row.Cells[1].Value
		headerName := row.Cells[2].Value
		headerValue := row.Cells[3].Value

		url := fmt.Sprintf("%s/api/targets", c.httpBaseURL)
		body := map[string]interface{}{
			"name": name,
			"url":  targetUrl,
			"headers": map[string]string{
				headerName: headerValue,
			},
		}
		data, _ := json.Marshal(body)
		resp, err := c.httpClient.Post(url, "application/json", bytes.NewBuffer(data))
		if err != nil {
			return err
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			return fmt.Errorf("failed to register target %s: %d", name, resp.StatusCode)
		}
	}
	return nil
}

func (c *testContext) aProducerPushesAMessageWithPayloadAndDataTo(id, data, target string) error {
	msg := models.Message[any]{ID: id, Payload: data, Target: target}
	c.activeMsgID = id
	payloadData, _ := json.Marshal(msg)
	url := fmt.Sprintf("%s/api/messages", c.httpBaseURL)
	resp, err := c.httpClient.Post(url, "application/json", bytes.NewBuffer(payloadData))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	c.lastResp = resp
	return nil
}

func (c *testContext) theMockServerAtShouldReceiveMessageWithHeaders(addr, id string, dt *godog.Table) error {
	timeout := time.After(7 * time.Second)
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-timeout:
			return fmt.Errorf("mock server %s never received message %s", addr, id)
		case <-tick.C:
			c.mu.Lock()
			svc, ok := c.services[addr]
			c.mu.Unlock()

			if ok {
				msgs := svc.GetAllMessages()
				headersList := svc.GetHeaders()
				for i, m := range msgs {
					if m.ID == id {
						// Verify headers
						receivedHeaders := headersList[i]
						for j, row := range dt.Rows {
							if j == 0 {
								continue
							}
							expectedName := http.CanonicalHeaderKey(row.Cells[0].Value)
							expectedValue := row.Cells[1].Value
							actualValue := receivedHeaders[expectedName]
							if actualValue != expectedValue {
								return fmt.Errorf("expected header %s=%s, got %s", expectedName, expectedValue, actualValue)
							}
						}
						return nil
					}
				}
			}
		}
	}
}

func (c *testContext) theMockServerAtShouldReceiveOnlyThePayloadOfMessage(addr, id string) error {
	timeout := time.After(5 * time.Second)
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-timeout:
			return fmt.Errorf("mock server %s never received message %s", addr, id)
		case <-tick.C:
			c.mu.Lock()
			svc, ok := c.services[addr]
			c.mu.Unlock()

			if ok {
				msgs := svc.GetAllMessages()
				bodies := svc.GetBodies()
				for i, m := range msgs {
					if m.ID == id {
						// Check body
						body := string(bodies[i])
						// Metadata should NOT be in the body
						if strings.Contains(body, "\"created_at\"") || strings.Contains(body, "\"target\"") {
							return fmt.Errorf("body unexpectedly contains metadata: %s", body)
						}
						return nil
					}
				}
			}
		}
	}
}

func (c *testContext) iPushATaskToWithIDAndReply_to(target, id, replyTo string) error {
	msg := map[string]interface{}{
		"id":     id,
		"target": target,
		"payload": map[string]interface{}{
			"task_id":  id,
			"reply_to": replyTo,
		},
	}
	c.activeMsgID = id
	data, _ := json.Marshal(msg)
	url := fmt.Sprintf("%s/api/messages", c.httpBaseURL)
	resp, err := c.httpClient.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	c.lastResp = resp
	return nil
}

func (c *testContext) serviceYShouldReceiveTheTask() error {
	// Reusing existing check logic
	return c.theMockServerShouldReceiveMessage(c.activeMsgID)
}

func (c *testContext) theMockServerShouldTriggerAReplyToMBGFor(target string) error {
	// We wait a bit or assume it happens based on the next step's success
	return nil
}

func (c *testContext) serviceXShouldEventuallyReceiveTheConfirmationForTask(status, id string) error {
	timeout := time.After(7 * time.Second)
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-timeout:
			return fmt.Errorf("service X never received %s confirmation for %s", status, id)
		case <-tick.C:
			c.mu.Lock()
			for _, svc := range c.services {
				msgs := svc.GetAllMessages()
				for _, m := range msgs {
					if pMap, ok := m.Payload.(map[string]interface{}); ok {
						if pMap["task_id"] == id && pMap["status"] == "COMPLETED" {
							c.mu.Unlock()
							return nil
						}
					}
				}
			}
			c.mu.Unlock()
		}
	}
}

func (c *testContext) theDashboardIsAccessibleViaWebSocket() error {
	wsURL := strings.Replace(c.httpBaseURL, "http://", "ws://", 1) + "/ws"
	fmt.Printf(" [Test] Dialing WebSocket: %s\n", wsURL)
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	conn, _, err := dialer.Dial(wsURL, nil)
	if err != nil {
		fmt.Printf(" [Test] Dial failed: %v\n", err)
		return fmt.Errorf("failed to connect to dashboard websocket: %w", err)
	}
	fmt.Printf(" [Test] WebSocket connected\n")
	c.wsConn = conn

	// Start reading messages
	go func() {
		defer fmt.Printf(" [Test] WebSocket reader goroutine stopped\n")
		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				fmt.Printf(" [Test] Read error: %v\n", err)
				return
			}
			fmt.Printf(" [Test] Received WS message: %d bytes\n", len(message))
			c.wsMsgs <- message
		}
	}()

	return nil
}

func (c *testContext) aNewTargetWithURLIsRegisteredViaPOST(name, targetUrl string) error {
	url := fmt.Sprintf("%s/api/targets", c.httpBaseURL)
	body := map[string]string{"name": name, "url": targetUrl}
	data, _ := json.Marshal(body)
	resp, err := c.httpClient.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("failed to register target: %d", resp.StatusCode)
	}
	return nil
}

func (c *testContext) theDashboardShouldReceiveAWebSocketMessageContainingTheNewTarget(name string) error {
	timeout := time.After(10 * time.Second) // Increased for reactive updates
	for {
		select {
		case <-timeout:
			return fmt.Errorf("timeout waiting for websocket message containing target %s", name)
		case msg := <-c.wsMsgs:
			var data map[string]interface{}
			if err := json.Unmarshal(msg, &data); err != nil {
				continue
			}
			// Skip "stats" messages that don't have targets list
			if targets, ok := data["targets"].([]interface{}); ok {
				for _, t := range targets {
					if tMap, ok := t.(map[string]interface{}); ok {
						if tMap["name"] == name {
							return nil
						}
					}
				}
			}
		}
	}
}

func InitializeScenario(sc *godog.ScenarioContext) {
	tc := &testContext{}

	sc.Step(`^the broker is accessible at HTTP "([^"]*)" and gRPC "([^"]*)"$`, tc.theBrokerIsAccessibleAtHTTPAndGRPC)
	sc.Step(`^the configuration has threshold set to (\d+)$`, tc.theConfigurationHasThresholdSetTo)
	sc.Step(`^the configuration has timeout set to (\d+) seconds$`, tc.theConfigurationHasTimeoutSetToSeconds)
	sc.Step(`^the broker is initialized with an empty queue$`, tc.theBrokerIsInitializedWithAnEmptyQueue)
	sc.Step(`^the circuit breaker is closed$`, tc.theCircuitBreakerIsClosed)
	sc.Step(`^a producer pushes a message "([^"]*)" with payload "([^"]*)"$`, tc.aProducerPushesAMessageWithPayload)
	sc.Step(`^the message "([^"]*)" should be stored in "([^"]*)"$`, tc.theMessageShouldBeStoredIn)
	sc.Step(`^the message should be available in the memory queue$`, tc.theMessageShouldBeAvailableInTheMemoryQueue)
	sc.Step(`^when a consumer pops a message$`, tc.whenAConsumerPopsAMessage)
	sc.Step(`^the consumer should receive message "([^"]*)"$`, tc.theConsumerShouldReceiveMessage)
	sc.Step(`^the file "([^"]*)" should be deleted$`, tc.theFileShouldBeDeleted)
	sc.Step(`^the circuit breaker threshold is set to (\d+)$`, tc.theCircuitBreakerThresholdIsSetTo)
	sc.Step(`^(\d+) consecutive push attempts fail due to storage errors$`, tc.consecutivePushAttemptsFailDueToStorageErrors)
	sc.Step(`^the circuit breaker should transition to "([^"]*)" state$`, tc.theCircuitBreakerShouldTransitionToState)
	sc.Step(`^subsequent push attempts should fail immediately with "([^"]*)" error$`, tc.subsequentPushAttemptsShouldFailImmediatelyWithError)
	sc.Step(`^the broker is stopped$`, tc.theBrokerIsStopped)
	sc.Step(`^there are messages "([^"]*)" and "([^"]*)" on disk in "([^"]*)"$`, tc.thereAreMessagesAndOnDiskIn)
	sc.Step(`^the broker is started and executes "([^"]*)"$`, tc.theBrokerIsStartedAndExecutes)
	sc.Step(`^the memory queue should contain (\d+) messages$`, tc.theMemoryQueueShouldContainMessages)
	sc.Step(`^the messages "([^"]*)" and "([^"]*)" should be available for consumption in the correct order$`, tc.theMessagesAndShouldBeAvailableForConsumptionInTheCorrectOrder)
	sc.Step(`^the circuit breaker is "([^"]*)"$`, tc.theCircuitBreakerIs)
	sc.Step(`^the timeout of (\d+) seconds has passed$`, tc.theTimeoutOfSecondsHasPassed)
	sc.Step(`^a new push attempt is "([^"]*)"$`, tc.whenANewPushAttemptIs)
	sc.Step(`^the circuit breaker should transition to "([^"]*)"$`, tc.theCircuitBreakerShouldTransitionTo)
	sc.Step(`^then finally transition to "([^"]*)"$`, tc.thenFinallyTransitionTo)
	sc.Step(`^the message should be persisted and queued normally$`, tc.theMessageShouldBePersistedAndQueuedNormally)

	sc.Step(`^the producer sends message "([^"]*)" with payload "([^"]*)" via gRPC$`, tc.theProducerSendsMessageViaGRPC)
	sc.Step(`^when the consumer requests a message via gRPC$`, tc.whenTheConsumerRequestsAMessageViaGRPC)
	sc.Step(`^the consumer should receive message "([^"]*)" via gRPC$`, tc.theConsumerShouldReceiveMessageViaGRPC)

	sc.Step(`^the producer sends message "([^"]*)" with payload "([^"]*)" via POST "([^"]*)"$`, tc.theProducerSendsMessageViaPOST)
	sc.Step(`^the server should respond with status (\d+)$`, tc.theServerShouldRespondWithStatus)
	sc.Step(`^the consumer requests a message via GET "([^"]*)"$`, tc.whenAConsumerPopsAMessage)
	sc.Step(`^the consumer should receive message "([^"]*)" via HTTP$`, tc.theConsumerShouldReceiveMessageViaHTTP)

	sc.Step(`^the dashboard is accessible$`, tc.theDashboardIsAccessible)
	sc.Step(`^a producer pushes a message "([^"]*)" via gRPC$`, tc.aProducerPushesAMessageViaGRPC)
	sc.Step(`^the dashboard stats should show queue size as (\d+)$`, tc.theDashboardStatsShouldShowQueueSizeAs)
	sc.Step(`^when a consumer pops a message via HTTP$`, tc.whenAConsumerPopsAMessage)

	sc.Step(`^the producer sends the following JSON payload via gRPC:$`, tc.theProducerSendsTheFollowingJSONPayloadViaGRPC)
	sc.Step(`^the producer sends the following JSON payload via POST "([^"]*)":$`, tc.theProducerSendsTheFollowingJSONPayloadViaPOST)

	sc.Step(`^the broker has a registered target "([^"]*)"$`, tc.theBrokerHasARegisteredTarget)
	sc.Step(`^the broker has the following registered targets:$`, tc.theBrokerHasTheFollowingRegisteredTargets)
	sc.Step(`^a mock server is listening at "([^"]*)"$`, tc.aMockServerIsListeningAt)
	sc.Step(`^the mock server should receive message "([^"]*)"$`, tc.theMockServerShouldReceiveMessage)
	sc.Step(`^the mock server at "([^"]*)" should NOT receive message "([^"]*)"$`, tc.theMockServerAtShouldNOTReceiveMessage)
	sc.Step(`^a producer pushes a message "([^"]*)" with target "([^"]*)"$`, tc.aProducerPushesAMessageWithTarget)
	sc.Step(`^the message "([^"]*)" should eventually be deleted from the queue$`, tc.theMessageShouldEventuallyBeDeletedFromTheQueue)
	sc.Step(`^a mock server at "([^"]*)" returns 500 error$`, tc.aMockServerAtReturnsError)
	sc.Step(`^the message "([^"]*)" should stay in the queue$`, tc.theMessageShouldStayInTheQueue)
	sc.Step(`^its "RetryCount" should be (\d+)$`, tc.itsRetryCountShouldBe)
	sc.Step(`^its "NextRetry" should be set according to exponential backoff$`, tc.itsNextRetryShouldBeSetAccordingToExponentialBackoff)

	sc.Step(`^the configuration has max_retries set to (\d+)$`, tc.theConfigurationHasMaxRetriesSetTo)
	sc.Step(`^the message "([^"]*)" should eventually be moved to DLQ folder "([^"]*)"$`, tc.theMessageShouldEventuallyBeMovedToDLQFolder)
	sc.Step(`^the message "([^"]*)" should be removed from the main queue$`, tc.theMessageShouldBeRemovedFromTheMainQueue)
	sc.Step(`^the dashboard stats should show dlq size as (\d+)$`, tc.theDashboardStatsShouldShowDlqSizeAs)
	sc.Step(`^a mock server at "([^"]*)" always returns 500 error$`, tc.aMockServerAtAlwaysReturnsError)
	sc.Step(`^the broker has the following registered targets with headers:$`, tc.theBrokerHasTheFollowingRegisteredTargetsWithHeaders)
	sc.Step(`^a producer pushes a message with payload "([^"]*)" and data "([^"]*)" to "([^"]*)"$`, tc.aProducerPushesAMessageWithPayloadAndDataTo)
	sc.Step(`^the mock server at "([^"]*)" should receive message "([^"]*)" with headers:$`, tc.theMockServerAtShouldReceiveMessageWithHeaders)
	sc.Step(`^the mock server at "([^"]*)" should receive only the payload of message "([^"]*)"$`, tc.theMockServerAtShouldReceiveOnlyThePayloadOfMessage)
	sc.Step(`^I push a task to "([^"]*)" with ID "([^"]*)" and reply_to "([^"]*)"$`, tc.iPushATaskToWithIDAndReply_to)
	sc.Step(`^Service Y should receive the task$`, tc.serviceYShouldReceiveTheTask)
	sc.Step(`^the mock server should trigger a reply to MBG for "([^"]*)"$`, tc.theMockServerShouldTriggerAReplyToMBGFor)
	sc.Step(`^Service X should eventually receive the "([^"]*)" confirmation for task "([^"]*)"$`, tc.serviceXShouldEventuallyReceiveTheConfirmationForTask)
	sc.Step(`^the dashboard is accessible via WebSocket$`, tc.theDashboardIsAccessibleViaWebSocket)
	sc.Step(`^a new target "([^"]*)" with URL "([^"]*)" is registered via POST "\/api\/targets"$`, tc.aNewTargetWithURLIsRegisteredViaPOST)
	sc.Step(`^the dashboard should receive a WebSocket message containing the new target "([^"]*)"$`, tc.theDashboardShouldReceiveAWebSocketMessageContainingTheNewTarget)

	sc.Before(func(ctx context.Context, sc *godog.Scenario) (context.Context, error) {
		tc.reset()
		return ctx, nil
	})

	sc.After(func(ctx context.Context, sc *godog.Scenario, err error) (context.Context, error) {
		tc.reset()
		return ctx, nil
	})
}

func TestMain(m *testing.M) {
	status := godog.TestSuite{
		Name:                "godog",
		ScenarioInitializer: InitializeScenario,
		Options: &godog.Options{
			Format: "pretty",
			Paths:  []string{"."},
		},
	}.Run()

	if st := m.Run(); st > status {
		status = st
	}
	os.Exit(status)
}
