package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"mbg/api/proto"
	"mbg/config"
	"mbg/models"
	"mbg/pkg/circuitbreaker"
	"net/http"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Dispatcher[T any] struct {
	broker      *Broker[T]
	cfg         *config.Config
	cb          circuitbreaker.CircuitBreaker
	client      *http.Client
	mu          sync.RWMutex
	targets     []string
	taskChan    chan models.Message[T]
	inProgress  sync.Map // Track messages currently being handled by workers
}

func NewDispatcher[T any](b *Broker[T], cfg *config.Config, cb circuitbreaker.CircuitBreaker) *Dispatcher[T] {
	return &Dispatcher[T]{
		broker:   b,
		cfg:      cfg,
		cb:       cb,
		client:   &http.Client{Timeout: 10 * time.Second},
		targets:  cfg.Dispatcher.Targets,
		taskChan: make(chan models.Message[T], 100),
	}
}

func (d *Dispatcher[T]) AddTarget(target string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.targets = append(d.targets, target)
}

func (d *Dispatcher[T]) getTargets() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	targets := make([]string, len(d.targets))
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
	targets := d.getTargets()
	if len(targets) == 0 {
		return
	}

	// Sesuai desain Competing Consumers: Kita kirim ke SALAH SATU target (Round-robin/First success)
	// Namun jika Anda ingin kirim ke SEMUA, kita bisa iterasi. 
	// RabbitMQ biasanya mengirim ke satu consumer per pesan.
	// Di sini kita coba kirim ke target pertama yang sukses.
	
	var lastErr error
	for _, target := range targets {
		err := d.cb(func() error {
			return d.sendToTarget(target, msg)
		})

		if err == nil {
			fmt.Printf(" [Success] Message %s delivered to %s\n", msg.ID, target)
			d.broker.Acknowledge(msg.ID)
			return
		}
		lastErr = err
	}

	// Jika semua target gagal
	fmt.Printf(" [Failure] Message %s failed for all targets: %v\n", msg.ID, lastErr)
	d.handleFailure(msg, lastErr)
}

func (d *Dispatcher[T]) sendToTarget(target string, msg models.Message[T]) error {
	if strings.HasPrefix(target, "grpc://") {
		return d.sendToGRPCTarget(strings.TrimPrefix(target, "grpc://"), msg)
	}

	// Default to HTTP
	return d.sendToHTTPTarget(target, msg)
}

func (d *Dispatcher[T]) sendToHTTPTarget(target string, msg models.Message[T]) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	resp, err := d.client.Post(target, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("target returned status: %d", resp.StatusCode)
	}

	return nil
}

func (d *Dispatcher[T]) sendToGRPCTarget(addr string, msg models.Message[T]) error {
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

	req := &proto.DeliveryRequest{
		Id:              msg.ID,
		Payload:         payloadStr,
		OriginTimestamp: msg.CreatedAt,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
