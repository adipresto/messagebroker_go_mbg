package circuitbreaker

import (
	"fmt"
	"os"
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
	name        string
	logPath     string
}

func NewCircuitBreaker(name string, threshold int, timeout time.Duration, logPath string) *CircuitBreaker {
	fmt.Printf(" [CB Debug] Created %s with logPath: %s\n", name, logPath)
	return &CircuitBreaker{
		name:      name,
		threshold: threshold,
		timeout:   timeout,
		state:     StateClosed,
		logPath:   logPath,
	}
}

func (cb *CircuitBreaker) logEvent(oldState, newState State) {
	if cb.logPath == "" {
		return
	}
	fmt.Printf(" [CB Debug] Logging event for %s: %v -> %v to %s\n", cb.name, oldState, newState, cb.logPath)
	f, err := os.OpenFile(cb.logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf(" [CB Error] Failed to open telemetry log: %v\n", err)
		return
	}
	defer f.Close()

	timestamp := time.Now().Format("2006-01-02 15:04:05.000")
	oldStr := cb.stateToString(oldState)
	newStr := cb.stateToString(newState)

	event := fmt.Sprintf("[CB_EVENT] %s | %s | %s -> %s | failures: %d\n",
		timestamp, cb.name, oldStr, newStr, cb.failures)

	f.WriteString(event)
	f.Sync() // Force disk write for Gherkin tests
}

func (cb *CircuitBreaker) stateToString(s State) string {
	switch s {
	case StateOpen:
		return "Open"
	case StateHalfOpen:
		return "Half-Open"
	default:
		return "Closed"
	}
}

func (cb *CircuitBreaker) Execute(action func() error) error {
	cb.mu.Lock()
	oldState := cb.state
	// Cek apakah perlu transisi dari Open ke Half-Open
	if cb.state == StateOpen && time.Since(cb.lastFailure) >= cb.timeout {
		cb.state = StateHalfOpen
		cb.failures = 0 // Reset failures for trial run
		cb.logEvent(oldState, cb.state)
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

	oldStateFinal := cb.state
	if err != nil {
		cb.failures++
		cb.lastFailure = time.Now()
		fmt.Printf(" [CB] Failure #%d: %v\n", cb.failures, err)

		// Instant Trip: Jika di Half-Open gagal satu kali saja, langsung balik ke OPEN
		if cb.state == StateHalfOpen || cb.failures >= cb.threshold {
			cb.state = StateOpen
			fmt.Printf(" [CB] Trip! State transitioned to OPEN (State was: %v)\n", oldStateFinal)
			cb.logEvent(oldStateFinal, cb.state)
		}
		return err
	}

	// Reset jika sukses (Closed) atau transisi dari Half-Open ke Closed
	if cb.state != StateClosed {
		fmt.Printf(" [CB] Success! Transitioned back to CLOSED\n")
		cb.logEvent(oldStateFinal, StateClosed)
	}
	cb.failures = 0
	cb.state = StateClosed
	return nil
}

func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	old := cb.state
	cb.failures = 0
	cb.state = StateClosed
	cb.logEvent(old, StateClosed)
	fmt.Printf(" [CB] Manual Reset triggered\n")
}

func (cb *CircuitBreaker) ResetConfig(threshold int, timeout time.Duration) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	old := cb.state
	cb.threshold = threshold
	cb.timeout = timeout
	cb.failures = 0
	cb.state = StateClosed
	cb.logEvent(old, StateClosed)
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
