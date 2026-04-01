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

type CircuitBreaker struct {
	mu          sync.Mutex
	failures    int
	threshold   int
	timeout     time.Duration
	state       State
	lastFailure time.Time
	inTrial     bool
}

func NewCircuitBreaker(threshold int, timeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		threshold: threshold,
		timeout:   timeout,
		state:     StateClosed,
	}
}

func (cb *CircuitBreaker) Execute(action func() error) error {
	cb.mu.Lock()
	// Cek apakah perlu transisi dari Open ke Half-Open
	if cb.state == StateOpen && time.Since(cb.lastFailure) >= cb.timeout {
		cb.state = StateHalfOpen
		cb.failures = 0 // Reset failures for trial run
	}

	if cb.state == StateOpen {
		cb.mu.Unlock()
		return fmt.Errorf("circuit is open")
	}

	// Single-Trial Logic: Jika Half-Open, hanya boleh satu request yang mencoba
	if cb.state == StateHalfOpen && cb.inTrial {
		cb.mu.Unlock()
		return fmt.Errorf("circuit is half-open and trial is in progress")
	}

	if cb.state == StateHalfOpen {
		cb.inTrial = true
		fmt.Printf(" [CB] Trial run starting...\n")
	}
	cb.mu.Unlock()

	err := action()

	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state == StateHalfOpen {
		cb.inTrial = false
	}

	if err != nil {
		cb.failures++
		cb.lastFailure = time.Now()
		fmt.Printf(" [CB] Failure #%d: %v\n", cb.failures, err)
		
		// Instant Trip: Jika di Half-Open gagal satu kali saja, langsung balik ke OPEN
		if cb.state == StateHalfOpen || cb.failures >= cb.threshold {
			cb.state = StateOpen
			fmt.Printf(" [CB] Trip! State transitioned to OPEN (State was: %v)\n", cb.state)
		}
		return err
	}

	// Reset jika sukses (Closed) atau transisi dari Half-Open ke Closed
	if cb.state != StateClosed {
		fmt.Printf(" [CB] Success! Transitioned back to CLOSED\n")
	}
	cb.failures = 0
	cb.state = StateClosed
	return nil
}

func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures = 0
	cb.state = StateClosed
	fmt.Printf(" [CB] Manual Reset triggered\n")
}

func (cb *CircuitBreaker) ResetConfig(threshold int, timeout time.Duration) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.threshold = threshold
	cb.timeout = timeout
	cb.failures = 0
	cb.state = StateClosed
	fmt.Printf(" [CB] Config Reset: Threshold=%d, Timeout=%v\n", threshold, timeout)
}

func (cb *CircuitBreaker) GetState() string {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	switch cb.state {
	case StateOpen:
		return "Open"
	case StateHalfOpen:
		return "Half-Open"
	default:
		return "Closed"
	}
}

func (cb *CircuitBreaker) GetMetrics() (int, int) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.failures, cb.threshold
}
