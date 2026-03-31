package broker

import (
	"encoding/json"
	"fmt"
	"iter"
	"mbg/models"
	"os"
	"path/filepath"
	"sync"
)

type Broker[T any] struct {
	mu          sync.RWMutex
	messages    []models.Message[T]
	storagePath string
	listeners   []chan struct{}
}

func NewBroker[T any](storagePath string) *Broker[T] {
	// Create directory if not exists
	os.MkdirAll(storagePath, 0755)

	b := &Broker[T]{
		messages:    make([]models.Message[T], 0),
		storagePath: storagePath,
		listeners:   make([]chan struct{}, 0),
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
	return os.WriteFile(filename, data, 0644)
}

// removeFromDisk menghapus file JSON setelah diproses (Acknowledge)
func (b *Broker[T]) RemoveFromDisk(id string) error {
	filename := filepath.Join(b.storagePath, fmt.Sprintf("%s.json", id))
	return os.Remove(filename)
}

// Recover memuat ulang pesan dari storage_path saat startup
func (b *Broker[T]) Recover() {
	files, _ := os.ReadDir(b.storagePath)
	for _, file := range files {
		if filepath.Ext(file.Name()) == ".json" {
			data, _ := os.ReadFile(filepath.Join(b.storagePath, file.Name()))
			var msg models.Message[T]
			if err := json.Unmarshal(data, &msg); err == nil {
				b.messages = append(b.messages, msg)
			}
		}
	}
}

// Push menambahkan pesan ke persistensi dulu, baru ke memori (Outbox style)
func (b *Broker[T]) Push(msg models.Message[T]) error {
	// 1. Simpan ke disk dulu
	if err := b.SaveToDisk(msg); err != nil {
		return fmt.Errorf("failed to persist message: %w", err)
	}

	// 2. Tambahkan ke memori
	b.mu.Lock()
	b.messages = append(b.messages, msg)
	b.mu.Unlock()

	b.notify()
	return nil
}

// Pop mengambil pesan, dan menghapusnya dari disk (Commit)
func (b *Broker[T]) Pop() (models.Message[T], error) {
	b.mu.Lock()
	if len(b.messages) == 0 {
		b.mu.Unlock()
		var zero models.Message[T]
		return zero, fmt.Errorf("queue is empty")
	}

	msg := b.messages[0]

	// Hapus dari disk setelah berhasil diambil (Acknowledge)
	if err := b.RemoveFromDisk(msg.ID); err != nil {
		b.mu.Unlock()
		return msg, fmt.Errorf("failed to remove from persistence: %w", err)
	}

	b.messages = b.messages[1:]
	b.mu.Unlock()

	b.notify()
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

// Acknowledge menghapus pesan berdasarkan ID (Commit)
func (b *Broker[T]) Acknowledge(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	index := -1
	for i, m := range b.messages {
		if m.ID == id {
			index = i
			break
		}
	}

	if index == -1 {
		return fmt.Errorf("message %s not found", id)
	}

	// Hapus dari disk
	if err := b.RemoveFromDisk(id); err != nil {
		return err
	}

	// Hapus dari memori
	b.messages = append(b.messages[:index], b.messages[index+1:]...)
	
	b.notify()
	return nil
}

// Update memperbarui isi pesan di memori dan disk (misal untuk retry count)
func (b *Broker[T]) Update(msg models.Message[T]) error {
	// Simpan ke disk dulu
	if err := b.SaveToDisk(msg); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	for i, m := range b.messages {
		if m.ID == msg.ID {
			b.messages[i] = msg
			return nil
		}
	}

	return fmt.Errorf("message %s not found in memory", msg.ID)
}

// All tetap menggunakan iterator untuk pembacaan aman
func (b *Broker[T]) All() iter.Seq[models.Message[T]] {
	return func(yield func(models.Message[T]) bool) {
		b.mu.RLock()
		defer b.mu.RUnlock()
		for _, m := range b.messages {
			if !yield(m) {
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

// GetStats returns the current number of messages in the queue.
func (b *Broker[T]) GetStats() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.messages)
}
