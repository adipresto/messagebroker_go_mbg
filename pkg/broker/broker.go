package broker

import (
	"encoding/json"
	"fmt"
	"iter"
	"mbg/pkg/models"
	"mbg/pkg/circuitbreaker"
	"mbg/pkg/telemetry"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Broker[T any] struct {
	mu             sync.RWMutex
	messages       []models.Message[T]
	storagePath    string
	deadLetterPath string
	listeners      []chan struct{}
	msgListeners   []chan models.Message[T]
	cb             *circuitbreaker.CircuitBreaker
	dlqSize        int
}

func NewBroker[T any](storagePath string, deadLetterPath string, cb *circuitbreaker.CircuitBreaker) *Broker[T] {
	// Create directories if not exists
	os.MkdirAll(storagePath, 0755)
	os.MkdirAll(deadLetterPath, 0755)

	b := &Broker[T]{
		messages:       make([]models.Message[T], 0),
		storagePath:    storagePath,
		deadLetterPath: deadLetterPath,
		listeners:      make([]chan struct{}, 0),
		msgListeners:   make([]chan models.Message[T], 0),
		cb:             cb,
	}

	// Load existing messages from disk on startup (Outbox Recovery)
	b.Recover()
	return b
}

// saveToDisk menyimpan pesan sebagai file JSON (Persistence)
func (b *Broker[T]) SaveToDisk(msg models.Message[T]) error {
	filename := filepath.Join(b.storagePath, fmt.Sprintf("%s.json", msg.ID))
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filename, data, 0644); err != nil {
		return fmt.Errorf("os.WriteFile failed for path %s: %w", filename, err)
	}
	return nil
}

// removeFromDisk menghapus file JSON setelah diproses (Acknowledge)
func (b *Broker[T]) RemoveFromDisk(id string) error {
	b.mu.RLock()
	path := b.storagePath
	b.mu.RUnlock()
	filename := filepath.Join(path, fmt.Sprintf("%s.json", id))
	return os.Remove(filename)
}

// SetStoragePath updates the storage path, useful for simulating errors in tests
func (b *Broker[T]) SetStoragePath(path string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.storagePath = path
}

// Recover memuat ulang pesan dari storage_path saat startup
func (b *Broker[T]) Recover() {
	absPath, _ := filepath.Abs(b.storagePath)
	fmt.Printf("Recovering messages from: %s (Absolute: %s)\n", b.storagePath, absPath)
	files, err := os.ReadDir(b.storagePath)
	if err != nil {
		fmt.Printf("Failed to read storage directory %s: %v\n", b.storagePath, err)
		return
	}

	count := 0
	for _, file := range files {
		if filepath.Ext(file.Name()) == ".json" {
			path := filepath.Join(b.storagePath, file.Name())
			data, err := os.ReadFile(path)
			if err != nil {
				fmt.Printf("Failed to read message file %s: %v\n", path, err)
				continue
			}

			var msg models.Message[T]
			if err := json.Unmarshal(data, &msg); err == nil {
				b.messages = append(b.messages, msg)
				fmt.Printf(" [Recovery] Loaded Message ID: %s\n", msg.ID)
				count++
			} else {
				fmt.Printf("Failed to unmarshal message %s: %v\n", path, err)
			}
		}
	}
	fmt.Printf("Successfully recovered %d messages\n", count)
	telemetry.QueueSize.Set(float64(count))

	// One-time scan of DLQ size
	dlqFiles, _ := os.ReadDir(b.deadLetterPath)
	b.dlqSize = 0
	for _, f := range dlqFiles {
		if !f.IsDir() && filepath.Ext(f.Name()) == ".json" {
			b.dlqSize++
		}
	}
}

// Push menambahkan pesan ke persistensi dulu, baru ke memori (Outbox style)
func (b *Broker[T]) Push(msg models.Message[T]) error {
	if b.cb != nil {
		return b.cb.Execute(func() error {
			return b.doPush(msg)
		})
	}
	return b.doPush(msg)
}

func (b *Broker[T]) doPush(msg models.Message[T]) error {
	// 1. Simpan ke disk dulu
	if err := b.SaveToDisk(msg); err != nil {
		return fmt.Errorf("failed to persist message: %w", err)
	}

	// 2. Tambahkan ke memori
	b.mu.Lock()
	b.messages = append(b.messages, msg)
	qSize := len(b.messages)
	b.mu.Unlock()

	telemetry.MessagesPushed.Inc()
	telemetry.QueueSize.Set(float64(qSize))
	b.notify()
	b.broadcast(msg)
	return nil
}

func (b *Broker[T]) broadcast(msg models.Message[T]) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, ch := range b.msgListeners {
		select {
		case ch <- msg:
		default:
			// Listener is full, skip to avoid blocking the push path
		}
	}
}

// Pop mengambil pesan, dan menghapusnya dari disk (Commit)
func (b *Broker[T]) Pop() (models.Message[T], error) {
	b.mu.Lock()
	if len(b.messages) == 0 {
		b.mu.Unlock()
		var zero models.Message[T]
		return zero, fmt.Errorf("queue_size is zero")
	}

	msg := b.messages[0]
	b.messages = b.messages[1:]
	qSize := len(b.messages)
	b.mu.Unlock()

	telemetry.QueueSize.Set(float64(qSize))
	// Notify after unlock
	b.notify()

	// Remove from disk AFTER memory update and unlock (latency reduction)
	if err := b.RemoveFromDisk(msg.ID); err != nil {
		return msg, fmt.Errorf("failed to remove from persistence: %w", err)
	}

	return msg, nil
}

// Peek melihat pesan terdepan tanpa menghapusnya
func (b *Broker[T]) Peek() (models.Message[T], error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if len(b.messages) == 0 {
		var zero models.Message[T]
		return zero, fmt.Errorf("queue is empty")
	}
	return b.messages[0], nil
}

// MoveToDLQ memindahkan pesan ke folder dead_letter secara permanen
func (b *Broker[T]) MoveToDLQ(id string) error {
	b.mu.Lock()
	index := -1
	var msg models.Message[T]
	for i, m := range b.messages {
		if m.ID == id {
			index = i
			msg = m
			break
		}
	}

	if index == -1 {
		b.mu.Unlock()
		return fmt.Errorf("message %s not found in memory", id)
	}

	// Remove ALL instances from memory (pola Competing Consumers)
	newMessages := make([]models.Message[T], 0, len(b.messages))
	for _, m := range b.messages {
		if m.ID != id {
			newMessages = append(newMessages, m)
		}
	}
	b.messages = newMessages
	qSize := len(b.messages)
	b.mu.Unlock()

	telemetry.QueueSize.Set(float64(qSize))
	b.notify()

	// Move file
	oldPath := filepath.Join(b.storagePath, fmt.Sprintf("%s.json", id))
	newPath := filepath.Join(b.deadLetterPath, fmt.Sprintf("%s.json", id))

	// Simpan versi terbaru (dengan RetryCount maskimal) sebelum dipindah
	data, _ := json.Marshal(msg)
	os.WriteFile(oldPath, data, 0644)

	err := os.Rename(oldPath, newPath)
	if err != nil {
		// Jika rename gagal (beda drive dsb), coba copy & remove
		data, err := os.ReadFile(oldPath)
		if err != nil {
			return err
		}
		err = os.WriteFile(newPath, data, 0644)
		if err != nil {
			return err
		}
		os.Remove(oldPath)
	}

	fmt.Printf(" [DLQ] Message %s moved to dead letter queue\n", id)
	b.mu.Lock()
	b.dlqSize++
	b.mu.Unlock()
	return nil
}

// Acknowledge menghapus pesan berdasarkan ID (Commit)
func (b *Broker[T]) Acknowledge(id string) error {
	b.mu.Lock()
	index := -1
	for i, m := range b.messages {
		if m.ID == id {
			index = i
			break
		}
	}

	if index == -1 {
		b.mu.Unlock()
		return fmt.Errorf("message %s not found in memory (already popped or sent?)", id)
	}

	// Remove ALL instances from memory
	newMessages := make([]models.Message[T], 0, len(b.messages))
	for _, m := range b.messages {
		if m.ID != id {
			newMessages = append(newMessages, m)
		}
	}
	b.messages = newMessages
	qSize := len(b.messages)
	b.mu.Unlock()

	telemetry.QueueSize.Set(float64(qSize))
	b.notify()

	// Remove from disk outside lock
	return b.RemoveFromDisk(id)
}

// Update memperbarui isi pesan di memori dan disk (misal untuk retry count)
func (b *Broker[T]) Update(msg models.Message[T]) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	found := false
	for i, m := range b.messages {
		if m.ID == msg.ID {
			b.messages[i] = msg
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("message %s not found in memory", msg.ID)
	}

	// Simpan ke disk hanya jika ada di memori
	if err := b.SaveToDisk(msg); err != nil {
		return err
	}
	return nil
}

// All tetap menggunakan iterator untuk pembacaan aman
func (b *Broker[T]) All() iter.Seq[models.Message[T]] {
	return func(yield func(models.Message[T]) bool) {
		// No longer snapshots the whole slice to avoid memory spikes.
		// Instead, we lock briefly to get the current count and iterate with RLock protection.
		b.mu.RLock()
		count := len(b.messages)
		b.mu.RUnlock()

		for i := 0; i < count; i++ {
			b.mu.RLock()
			// Re-check bounds in case messages were popped/cleared during iteration
			if i >= len(b.messages) {
				b.mu.RUnlock()
				break
			}
			msg := b.messages[i]
			b.mu.RUnlock()

			if !yield(msg) {
				return
			}
		}
	}
}
// Subscribe returns a channel that receives an empty struct whenever the broker state changes.
func (b *Broker[T]) Subscribe() chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	ch := make(chan struct{}, 1)
	b.listeners = append(b.listeners, ch)
	return ch
}

// SubscribeMessages returns a channel that receives the actual message pushed.
func (b *Broker[T]) SubscribeMessages() chan models.Message[T] {
	b.mu.Lock()
	defer b.mu.Unlock()
	ch := make(chan models.Message[T], 100)
	b.msgListeners = append(b.msgListeners, ch)
	return ch
}

func (b *Broker[T]) notify() {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, ch := range b.listeners {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// GetStats returns the current number of messages in the queue and DLQ.
func (b *Broker[T]) GetStats() (int, int) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.messages), b.dlqSize
}

// GetCBState returns the current state of the circuit breaker.
func (b *Broker[T]) GetCBState() string {
	if b.cb == nil {
		return "N/A"
	}
	return b.cb.GetState()
}

// GetCBMetrics returns the current failure count and threshold.
func (b *Broker[T]) GetCBMetrics() (int, int) {
	if b.cb == nil {
		return 0, 0
	}
	return b.cb.GetMetrics()
}

// ResetConfig updates the circuit breaker configuration.
func (b *Broker[T]) ResetConfig(threshold int, timeout time.Duration) {
	if b.cb != nil {
		b.cb.ResetConfig(threshold, timeout)
	}
}

// Reset resets the circuit breaker.
func (b *Broker[T]) Reset() {
	if b.cb != nil {
		b.cb.Reset()
	}
}
