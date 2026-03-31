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
	broker  *Broker[T]
	cfg     *config.Config
	cb      circuitbreaker.CircuitBreaker
	client  *http.Client
	mu      sync.RWMutex
	targets []string
}

func NewDispatcher[T any](b *Broker[T], cfg *config.Config, cb circuitbreaker.CircuitBreaker) *Dispatcher[T] {
	return &Dispatcher[T]{
		broker:  b,
		cfg:     cfg,
		cb:      cb,
		client:  &http.Client{Timeout: 10 * time.Second},
		targets: cfg.Dispatcher.Targets,
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
	notify := d.broker.Subscribe()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	fmt.Println("Dispatcher started...")
	for {
		select {
		case <-notify:
			d.dispatchAll()
		case <-ticker.C:
			d.dispatchAll()
		}
	}
}

func (d *Dispatcher[T]) dispatchAll() {
	now := time.Now().Unix()
	var candidates []models.Message[T]

	// Ambil kandidat pesan yang siap dikirim (NextRetry <= now)
	// Kita iterasi dulu untuk menghindari deadlock saat modifikasi state broker
	for msg := range d.broker.All() {
		if msg.NextRetry <= now {
			candidates = append(candidates, msg)
		}
	}

	for _, msg := range candidates {
		targets := d.getTargets()
		for _, target := range targets {
			err := d.cb(func() error {
				return d.sendToTarget(target, msg)
			})

			if err == nil {
				fmt.Printf("Message %s successfully delivered to %s\n", msg.ID, target)
				d.broker.Acknowledge(msg.ID)
			} else {
				fmt.Printf("Failed to deliver message %s to %s: %v\n", msg.ID, target, err)
				d.handleFailure(msg, err)
			}
		}
	}
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
		fmt.Printf("Message %s reached MAX retries (%d). Giving up.\n", msg.ID, d.cfg.Dispatcher.MaxRetries)
		// Tetap di queue tapi tidak akan diproses lagi karena NextRetry akan sangat jauh di masa depan?
		// Atau bisa dihapus/pindah ke DLQ. Untuk sekarang kita biarkan di queue dengan NextRetry yang sangat lama.
		msg.NextRetry = time.Now().Unix() + 31536000 // 1 year
		d.broker.Update(msg)
		return
	}

	msg.RetryCount++
	// Exponential Backoff: Base * 2^RetryCount
	backoff := int64(d.cfg.Dispatcher.BaseInterval) * int64(math.Pow(2, float64(msg.RetryCount)))
	msg.NextRetry = time.Now().Unix() + backoff

	fmt.Printf("Message %s rescheduled in %ds (failure count: %d)\n", msg.ID, backoff, msg.RetryCount)
	d.broker.Update(msg)
}
