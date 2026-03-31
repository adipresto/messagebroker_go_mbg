package circuitbreaker

import (
	"fmt"
	"sync"
	"time"
)

type State int

const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

type CircuitBreaker func(action func() error) error

func NewCircuitBreaker(threshold int, timeout time.Duration) CircuitBreaker {
	var mu sync.Mutex
	failures := 0
	state := StateClosed
	lastFailure := time.Now()

	return func(action func() error) error {
		mu.Lock()
		// Cek apakah perlu transisi dari Open ke Half-Open
		if state == StateOpen && time.Since(lastFailure) > timeout {
			state = StateHalfOpen
		}

		if state == StateOpen {
			mu.Unlock()
			return fmt.Errorf("circuit is open")
		}
		mu.Unlock()

		err := action()

		mu.Lock()
		defer mu.Unlock()
		if err != nil {
			failures++
			lastFailure = time.Now()
			if failures >= threshold {
				state = StateOpen
			}
			return err
		}

		// Reset jika sukses (Closed) atau transisi dari Half-Open ke Closed
		failures = 0
		state = StateClosed
		return nil
	}
}
