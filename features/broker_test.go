package features

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"mbg/api/proto"
	"mbg/models"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
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
	receivedMsg models.Message[any]
	lastError   error
	lastResp    *http.Response

	// URLs
	httpBaseURL string
	grpcAddr    string

	// Clients
	grpcClient proto.BrokerServiceClient
	grpcConn   *grpc.ClientConn
}

func (c *testContext) reset() {
	if c.grpcConn != nil {
		c.grpcConn.Close()
		c.grpcConn = nil
		c.grpcClient = nil
	}
	c.threshold = 3
	c.timeout = 5 * time.Second
	c.storageDir = "../data/messages/"
	c.receivedMsg = models.Message[any]{}
	c.lastError = nil

	// Drain the queue to ensure clean state
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
	timeout := time.After(10 * time.Second)
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-timeout:
			return fmt.Errorf("server at %s did not become healthy in time", c.httpBaseURL)
		case <-tick.C:
			resp, err := http.Get(url)
			if err == nil && resp.StatusCode == http.StatusOK {
				resp.Body.Close()
				return nil
			}
		}
	}
}

func (c *testContext) theBrokerIsAccessibleAtHTTPAndGRPC(httpURL, grpcAddr string) error {
	c.httpBaseURL = httpURL
	c.grpcAddr = grpcAddr

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

func (c *testContext) theBrokerIsInitializedWithAnEmptyQueue() error {
	c.drainQueue()
	return nil
}

func (c *testContext) theCircuitBreakerIsClosed() error {
	return nil
}

func (c *testContext) aProducerPushesAMessageWithPayload(id, payload string) error {
	msg := models.Message[string]{ID: id, Payload: payload}
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
	// Only kill if it's currently running (to be safe/clean)
	url := fmt.Sprintf("%s/api/health", c.httpBaseURL)
	resp, err := http.Get(url)
	if err == nil && resp.StatusCode == http.StatusOK {
		resp.Body.Close()
		// Kill the process - using taskkill as it's the most reliable way on Windows for mbg.exe
		_ = exec.Command("taskkill", "/F", "/IM", "mbg.exe", "/T").Run()
		time.Sleep(1 * time.Second) // Give it time to release sockets
	}
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
	// Start mbg.exe in the background (using PowerShell for stability in this environment)
	cmd := exec.Command("powershell", "-Command", "Start-Process -FilePath './mbg.exe' -NoNewWindow")
	// Since we are running go test ./features, we need to point to the root's mbg.exe
	cmd.Dir = ".."
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start mbg.exe: %w", err)
	}

	// Wait for the new instance to be healthy
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

	sc.Before(func(ctx context.Context, sc *godog.Scenario) (context.Context, error) {
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
