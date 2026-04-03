package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"mbg/proto/proto"
	"mbg/config"
	"mbg/pkg/models"
	"mbg/pkg/circuitbreaker"
	"net/http"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type Dispatcher[T any] struct {
	broker      *Broker[T]
	cfg         *config.Config
	cb          *circuitbreaker.CircuitBreaker
	client      *http.Client
	mu          sync.RWMutex
	targets     []config.TargetConfig
	taskChan    chan models.Message[T]
	inProgress  sync.Map // Track messages currently being handled by workers
}

func NewDispatcher[T any](b *Broker[T], cfg *config.Config, cb *circuitbreaker.CircuitBreaker) *Dispatcher[T] {
	return &Dispatcher[T]{
		broker:   b,
		cfg:      cfg,
		cb:       cb,
		client:   &http.Client{Timeout: 10 * time.Second},
		targets:  cfg.Dispatcher.Targets,
		taskChan: make(chan models.Message[T], 100),
	}
}

func (d *Dispatcher[T]) AddTarget(target config.TargetConfig) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.targets = append(d.targets, target)
}

func (d *Dispatcher[T]) getTargets() []config.TargetConfig {
	d.mu.RLock()
	defer d.mu.RUnlock()
	targets := make([]config.TargetConfig, len(d.targets))
	copy(targets, d.targets)
	return targets
}

func (d *Dispatcher[T]) Start() {
	fmt.Println("Dispatcher started with RabbitMQ-style Competing Consumers...")

	// Launch worker pool
	workerCount := d.cfg.Dispatcher.WorkerCount
	if workerCount <= 0 {
		workerCount = 1
	}
	for i := 0; i < workerCount; i++ {
		go d.worker(i)
	}

	notify := d.broker.Subscribe()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-notify:
			d.fillTaskQueue()
		case <-ticker.C:
			d.fillTaskQueue()
		}
	}
}

func (d *Dispatcher[T]) worker(id int) {
	fmt.Printf(" [Worker %d] ready\n", id)
	for msg := range d.taskChan {
		d.processMessage(msg)
		d.inProgress.Delete(msg.ID)
	}
}

func (d *Dispatcher[T]) fillTaskQueue() {
	now := time.Now().Unix()
	
	// Scan broker for ready messages
	for msg := range d.broker.All() {
		if msg.NextRetry <= now {
			// Only add if not already in flight
			if _, ok := d.inProgress.LoadOrStore(msg.ID, true); !ok {
				select {
				case d.taskChan <- msg:
					// Pushed to worker
				default:
					// Worker queue full, release progress lock
					d.inProgress.Delete(msg.ID)
				}
			}
		}
	}
}

func (d *Dispatcher[T]) processMessage(msg models.Message[T]) {
	allTargets := d.getTargets()
	if len(allTargets) == 0 {
		return
	}

	// Filter targets if msg.Target is specified
	var targets []config.TargetConfig
	if msg.Target != "" {
		for _, t := range allTargets {
			if t.Name == msg.Target {
				targets = append(targets, t)
			}
		}
		if len(targets) == 0 {
			fmt.Printf(" [Warning] Targeted delivery failed: No target named '%s' found. Falling back to default.\n", msg.Target)
			targets = allTargets
		}
	} else {
		targets = allTargets
	}

	var lastErr error
	for _, target := range targets {
		err := d.cb.Execute(func() error {
			return d.sendToTarget(target, msg)
		})

		if err == nil {
			fmt.Printf(" [Success] Message %s delivered to %s (%s)\n", msg.ID, target.Name, target.URL)
			d.broker.Acknowledge(msg.ID)
			return
		}
		lastErr = err
	}

	// Jika semua target gagal
	fmt.Printf(" [Failure] Message %s failed for all targets: %v\n", msg.ID, lastErr)
	d.handleFailure(msg, lastErr)
}

func (d *Dispatcher[T]) sendToTarget(target config.TargetConfig, msg models.Message[T]) error {
	if strings.HasPrefix(target.URL, "grpc://") {
		return d.sendToGRPCTarget(target, msg)
	}

	// Default to HTTP
	return d.sendToHTTPTarget(target, msg)
}

func (d *Dispatcher[T]) getMergedHeaders(target config.TargetConfig, msg models.Message[T]) map[string]string {
	merged := make(map[string]string)

	// 1. Load target-level default headers
	for k, v := range target.Headers {
		merged[k] = v
	}

	// 2. Load message-level headers (if any)
	if msg.Headers != nil {
		// Attempt to parse msg.Headers as map[string]interface{} or map[string]string
		switch h := msg.Headers.(type) {
		case map[string]string:
			for k, v := range h {
				merged[k] = v
			}
		case map[string]interface{}:
			for k, v := range h {
				merged[k] = fmt.Sprintf("%v", v)
			}
		case string:
			// If it's a string, maybe it's JSON?
			var jsonMap map[string]interface{}
			if err := json.Unmarshal([]byte(h), &jsonMap); err == nil {
				for k, v := range jsonMap {
					merged[k] = fmt.Sprintf("%v", v)
				}
			}
		}
	}

	// 3. Add system headers
	merged["X-Message-ID"] = msg.ID

	return merged
}

func (d *Dispatcher[T]) sendToHTTPTarget(target config.TargetConfig, msg models.Message[T]) error {
	data, err := json.Marshal(msg.Payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", target.URL, bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	// Apply merged headers
	headers := d.getMergedHeaders(target, msg)
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("target returned status: %d", resp.StatusCode)
	}

	return nil
}

func (d *Dispatcher[T]) sendToGRPCTarget(target config.TargetConfig, msg models.Message[T]) error {
	addr := strings.TrimPrefix(target.URL, "grpc://")
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to dial grpc target: %w", err)
	}
	defer conn.Close()

	client := proto.NewTargetServiceClient(conn)

	// Ubah payload ke string JSON jika perlu
	var payloadStr string
	switch v := any(msg.Payload).(type) {
	case string:
		payloadStr = v
	default:
		data, _ := json.Marshal(v)
		payloadStr = string(data)
	}

	// Prepare headers for both field and metadata
	headers := d.getMergedHeaders(target, msg)
	headersData, _ := json.Marshal(headers)

	req := &proto.DeliveryRequest{
		Id:              msg.ID,
		Payload:         payloadStr,
		OriginTimestamp: msg.CreatedAt,
		Headers:         string(headersData),
	}

	// Create context with metadata
	md := metadata.New(headers)
	ctx := metadata.NewOutgoingContext(context.Background(), md)
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	resp, err := client.Deliver(ctx, req)
	if err != nil {
		return fmt.Errorf("grpc delivery failed: %w", err)
	}

	if resp.Status != "SUCCESS" {
		return fmt.Errorf("target returned failure status: %s", resp.Status)
	}

	return nil
}

func (d *Dispatcher[T]) handleFailure(msg models.Message[T], err error) {
	if msg.RetryCount >= d.cfg.Dispatcher.MaxRetries {
		fmt.Printf(" [!] Message %s reached MAX retries (%d). Moving to DLQ.\n", msg.ID, d.cfg.Dispatcher.MaxRetries)
		d.broker.MoveToDLQ(msg.ID)
		return
	}

	msg.RetryCount++
	// Exponential Backoff: Base * 2^RetryCount
	backoff := int64(d.cfg.Dispatcher.BaseInterval) * int64(math.Pow(2, float64(msg.RetryCount)))
	msg.NextRetry = time.Now().Unix() + backoff

	fmt.Printf(" [Retry] Message %s rescheduled in %ds (attempt %d)\n", msg.ID, backoff, msg.RetryCount)
	d.broker.Update(msg)
}

func (d *Dispatcher[T]) Reset() {
	d.cb.Reset()
	d.inProgress = sync.Map{}
}

func (d *Dispatcher[T]) GetCBState() string {
	return d.cb.GetState()
}

func (d *Dispatcher[T]) GetCBMetrics() (int, int) {
	return d.cb.GetMetrics()
}

func (d *Dispatcher[T]) ResetConfig(threshold int, timeout time.Duration) {
	d.cb.ResetConfig(threshold, timeout)
}
