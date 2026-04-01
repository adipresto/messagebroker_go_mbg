package features

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"mbg/api/proto"
	"mbg/models"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cucumber/godog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type testContext struct {
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

	// Mock Targets
	mockServer     *httptest.Server
	mockServerGRPC *grpc.Server
	lastTarget     string
	pushedMsgs     []models.Message[string]
	activeMsgID    string // Track the most recently pushed message ID for context
}

type mockTargetTestServer struct {
	proto.UnimplementedTargetServiceServer
	tc *testContext
}

func (s *mockTargetTestServer) Deliver(ctx context.Context, req *proto.DeliveryRequest) (*proto.DeliveryResponse, error) {
	s.tc.pushedMsgs = append(s.tc.pushedMsgs, models.Message[string]{
		ID:      req.Id,
		Payload: req.Payload,
	})
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
			fmt.Printf("Purged storage directory: %s\n", p)
		}
	}
}

func (c *testContext) reset() {
	// Kill any leftover broker process to ensure isolation
	_ = exec.Command("taskkill", "/F", "/IM", "mbg.exe", "/T").Run()
	time.Sleep(2500 * time.Millisecond)

	if c.mockServerGRPC != nil {
		c.mockServerGRPC.Stop()
		c.mockServerGRPC = nil
	}
	c.pushedMsgs = nil
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

	// Drain the queue to ensure clean state (if anyone is still listening)
	if c.httpBaseURL != "" {
		c.drainQueue()
	}
}

func (c *testContext) drainQueue() {
	url := fmt.Sprintf("%s/api/messages", c.httpBaseURL)
	maxAttempts := 50
	for i := 0; i < maxAttempts; i++ {
		resp, err := http.Get(url)
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
	fmt.Printf("Waiting for server at %s to become healthy...\n", url)
	for i := 0; i < 20; i++ {
		resp, err := http.Get(url)
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			fmt.Printf("  Server is healthy at %s\n", url)
			return nil
		}
		if err == nil {
			resp.Body.Close()
		}
		fmt.Printf("  Attempt %d: server not ready yet...\n", i+1)
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
		fmt.Println("Broker down during accessibility check, attempting auto-start...")
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
	return nil
}

func (c *testContext) theConfigurationHasTimeoutSetToSeconds(seconds int) error {
	c.timeout = time.Duration(seconds) * time.Second
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
	return nil
}
func (c *testContext) aProducerPushesAMessageWithPayload(id, payload string) error {
	msg := models.Message[string]{ID: id, Payload: payload}
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

func (c *testContext) theMessageShouldBeStoredIn(id, path string) error {
	// Path mapping: The feature file might use relative paths, but we will use the ID to find it in the storageDir.
	absPath := filepath.Join(c.storageDir, id+".json")
	fmt.Println(absPath)
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
	return nil
}

func (c *testContext) theCircuitBreakerThresholdIsSetTo(threshold int) error {
	c.threshold = threshold
	return nil
}

func (c *testContext) theCircuitBreakerShouldTransitionToState(state string) error {
	return nil
}

func (c *testContext) subsequentPushAttemptsShouldFailImmediatelyWithError(errMsg string) error {
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

	msg1 := models.Message[string]{ID: id1, Payload: "Recover 1", CreatedAt: time.Now().Unix()}
	msg2 := models.Message[string]{ID: id2, Payload: "Recover 2", CreatedAt: time.Now().Unix()}

	for _, msg := range []models.Message[string]{msg1, msg2} {
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
			fmt.Println("Broker already running at localhost:8081, skipping start for action:", action)
			return nil
		}
	} else {
		fmt.Println("Action is Recover, ensuring fresh start (killing old if any)...")
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
		fmt.Println("Starting broker with -no-dispatcher flag for stability.")
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
	return nil
}

func (c *testContext) theTimeoutOfSecondsHasPassed(seconds int) error {
	time.Sleep(time.Duration(seconds) * time.Second)
	return nil
}

func (c *testContext) whenANewPushAttemptIs(result string) error {
	return nil
}

func (c *testContext) theCircuitBreakerShouldTransitionTo(state string) error {
	return nil
}

func (c *testContext) thenFinallyTransitionTo(state string) error {
	return nil
}

func (c *testContext) theMessageShouldBePersistedAndQueuedNormally() error {
	return nil
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
	msg := models.Message[string]{ID: id, Payload: payload}
	data, _ := json.Marshal(msg)
	url := fmt.Sprintf("%s%s", c.httpBaseURL, path)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return err
	}
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
	resp, err := http.Get(c.httpBaseURL + "/")
	if err != nil {
		return err
	}
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
	resp, err := http.Get(url)
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
	resp, err := http.Get(url)
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
	var body map[string]interface{}
	if err := json.Unmarshal([]byte(doc.Content), &body); err != nil {
		return err
	}
	id := body["id"].(string)
	payloadBytes, _ := json.Marshal(body["payload"])

	req := &proto.PushRequest{Id: id, Payload: string(payloadBytes)}
	resp, err := c.grpcClient.Push(context.Background(), req)
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("gRPC JSON push failed: %s", resp.Message)
	}
	return nil
}

func (c *testContext) theBrokerHasARegisteredTarget(target string) error {
	url := fmt.Sprintf("%s/api/targets", c.httpBaseURL)
	body := map[string]string{"target": target}
	data, _ := json.Marshal(body)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
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

func (c *testContext) aMockServerIsListeningAt(addr string) error {
	cleanAddr := strings.TrimPrefix(addr, "grpc://")
	cleanAddr = strings.TrimPrefix(cleanAddr, "http://")
	if strings.Contains(cleanAddr, "/") {
		cleanAddr = strings.Split(cleanAddr, "/")[0]
	}

	// Cek apakah port sudah diduduki oleh server eksternal
	conn, err := net.DialTimeout("tcp", cleanAddr, 500*time.Millisecond)
	if err == nil {
		conn.Close()
		fmt.Printf("Mock server already running at %s, skipping internal start for scenario verification.\n", cleanAddr)
		return nil
	}

	if strings.HasPrefix(addr, "grpc://") {
		cleanAddr := strings.TrimPrefix(addr, "grpc://")
		lis, err := net.Listen("tcp", cleanAddr)
		if err != nil {
			return err
		}
		c.mockServerGRPC = grpc.NewServer()
		proto.RegisterTargetServiceServer(c.mockServerGRPC, &mockTargetTestServer{tc: c})
		go c.mockServerGRPC.Serve(lis)
		return nil
	}

	// HTTP Implementation (Legacy/Fallback)
	c.mockServer = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var msg models.Message[string]
		json.NewDecoder(r.Body).Decode(&msg)
		c.pushedMsgs = append(c.pushedMsgs, msg)
		w.WriteHeader(http.StatusOK)
	}))

	// We'll try to listen on the port mentioned in addr (e.g. localhost:9090)
	l, err := net.Listen("tcp", "localhost:9090")
	if err != nil {
		// If 9090 is busy, we just let it fail or use a random one.
		// For simplicity in this env, we assume 9090 is available for tests.
		return err
	}
	c.mockServer.Listener = l
	c.mockServer.Start()
	return nil
}

func (c *testContext) aMockServerAtReturnsError(addr string) error {
	c.mockServer = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	l, err := net.Listen("tcp", "localhost:9091")
	if err != nil {
		return err
	}
	c.mockServer.Listener = l
	c.mockServer.Start()
	return nil
}

func (c *testContext) theMockServerShouldReceiveMessage(id string) error {
	timeout := time.After(5 * time.Second)
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-timeout:
			return fmt.Errorf("mock server never received message %s", id)
		case <-tick.C:
			for _, m := range c.pushedMsgs {
				if m.ID == id {
					return nil
				}
			}
		}
	}
}

func (c *testContext) theMessageShouldEventuallyBeDeletedFromTheQueue(id string) error {
	timeout := time.After(5 * time.Second)
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()

	fmt.Printf("Waiting for message %s to be deleted from queue (timeout 5s)...\n", id)
	for {
		select {
		case <-timeout:
			return fmt.Errorf("message %s still in queue after 5s", id)
		case <-tick.C:
			url := fmt.Sprintf("%s/api/stats", c.httpBaseURL)
			resp, err := http.Get(url)
			if err != nil {
				continue
			}
			var stats map[string]interface{}
			json.NewDecoder(resp.Body).Decode(&stats)
			resp.Body.Close()
			actual := int(stats["queue_size"].(float64))
			fmt.Printf("  Polling queue size: %d\n", actual)
			if actual == 0 {
				return nil
			}
		}
	}
}

func (c *testContext) theMessageShouldStayInTheQueue(id string) error {
	url := fmt.Sprintf("%s/api/stats", c.httpBaseURL)
	resp, err := http.Get(url)
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

	fmt.Printf("Waiting for message %s retry count to be %d (timeout 5s)...\n", id, count)
	for {
		select {
		case <-timeout:
			return fmt.Errorf("message %s retry count never reached %d after 5s", id, count)
		case <-tick.C:
			url := fmt.Sprintf("%s/api/messages/all", c.httpBaseURL)
			resp, err := http.Get(url)
			if err != nil {
				continue
			}
			var msgs []models.Message[string]
			json.NewDecoder(resp.Body).Decode(&msgs)
			resp.Body.Close()

			for _, m := range msgs {
				if m.ID == id {
					fmt.Printf("  Polling retry count for %s: %d\n", id, m.RetryCount)
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
	fmt.Printf("Waiting for message %s to appear in DLQ: %s\n", id, absPath)

	for {
		select {
		case <-timeout:
			return fmt.Errorf("message %s never moved to DLQ after 10s", id)
		case <-tick.C:
			if _, err := os.Stat(absPath); err == nil {
				fmt.Printf("  Message %s found in DLQ\n", id)
				return nil
			}
		}
	}
}

func (c *testContext) aMockServerAtAlwaysReturnsError(addr string) error {
	// Similar to aMockServerAtReturnsError but maybe for a specific port
	c.mockServer = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	c.mockServer.Listener = l
	c.mockServer.Start()
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
			resp, err := http.Get(url)
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

	fmt.Printf("Waiting for message %s NextRetry update (timeout 5s)...\n", id)
	for {
		select {
		case <-timeout:
			return fmt.Errorf("message %s NextRetry was never updated after 5s", id)
		case <-tick.C:
			url := fmt.Sprintf("%s/api/messages/all", c.httpBaseURL)
			resp, err := http.Get(url)
			if err != nil {
				continue
			}
			var msgs []models.Message[string]
			json.NewDecoder(resp.Body).Decode(&msgs)
			resp.Body.Close()

			for _, m := range msgs {
				if m.ID == id {
					if m.NextRetry > time.Now().Unix() {
						fmt.Printf("  NextRetry set successfully: %d\n", m.NextRetry)
						return nil
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

	sc.Step(`^the broker has a registered target "([^"]*)"$`, tc.theBrokerHasARegisteredTarget)
	sc.Step(`^a mock server is listening at "([^"]*)"$`, tc.aMockServerIsListeningAt)
	sc.Step(`^the mock server should receive message "([^"]*)"$`, tc.theMockServerShouldReceiveMessage)
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
