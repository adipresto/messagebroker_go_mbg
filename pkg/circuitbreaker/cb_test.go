package circuitbreaker

import (
	"errors"
	"os"
	"sync"
	"testing"
	"time"
)

func TestCircuitBreaker_StateTransitions(t *testing.T) {
	logPath := "data/cb_telemetry.log"
	os.MkdirAll("data", 0755)
	os.WriteFile(logPath, []byte(""), 0644) // Clear old

	cb := NewCircuitBreaker("TestCB", 3, 100*time.Millisecond, logPath)

	// 1. Initial State: Closed
	if cb.GetState() != "Closed" {
		t.Errorf("Expected initial state Closed, got %s", cb.GetState())
	}

	// 2. Trip to Open
	for i := 0; i < 3; i++ {
		cb.Execute(func() error { return errors.New("fail") })
	}
	if cb.GetState() != "Open" {
		t.Errorf("Expected state Open after 3 failures, got %s", cb.GetState())
	}

	// 3. Stay Open before timeout
	err := cb.Execute(func() error { return nil })
	if err == nil || err.Error() != "circuit is open" {
		t.Errorf("Expected 'circuit is open' error, got %v", err)
	}

	// 4. Transition to Half-Open after timeout
	time.Sleep(150 * time.Millisecond)
	// State is still Open until Execute is called
	if cb.GetState() != "Open" {
		t.Errorf("Expected state Open until Execute is called, got %s", cb.GetState())
	}

	// First call after timeout triggers Half-Open
	var trialExecuted bool
	err = cb.Execute(func() error {
		trialExecuted = true
		if cb.state != StateHalfOpen {
			t.Errorf("Expected state Half-Open during execution, got %v", cb.state)
		}
		return nil
	})

	if err != nil {
		t.Errorf("Expected successful trial run, got %v", err)
	}
	if !trialExecuted {
		t.Error("Trial action was not executed")
	}

	// 5. Success in Half-Open transitions to Closed
	if cb.GetState() != "Closed" {
		t.Errorf("Expected state Closed after successful trial, got %s", cb.GetState())
	}
}

func TestCircuitBreaker_HalfOpenToOpen(t *testing.T) {
	cb := NewCircuitBreaker("TestCB_Short", 1, 50*time.Millisecond, "")

	// Trip to Open
	cb.Execute(func() error { return errors.New("fail") })

	time.Sleep(100 * time.Millisecond)

	// Fail in Half-Open
	cb.Execute(func() error { return errors.New("fail trailing") })

	if cb.GetState() != "Open" {
		t.Errorf("Expected state Open after failed trial, got %s", cb.GetState())
	}
}

func TestCircuitBreaker_SingleTrialEnforcement(t *testing.T) {
	cb := NewCircuitBreaker("TestCB_Trial", 1, 100*time.Millisecond, "")
	cb.Execute(func() error { return errors.New("fail") })

	time.Sleep(150 * time.Millisecond)

	startSignal := make(chan struct{})
	doneSignal := make(chan struct{})

	// Start a long-running trial
	go func() {
		cb.Execute(func() error {
			close(startSignal)
			time.Sleep(50 * time.Millisecond)
			return nil
		})
		close(doneSignal)
	}()

	<-startSignal
	// While trial is in progress, any other call should fail immediately
	err := cb.Execute(func() error { return nil })
	if err == nil || err.Error() != "circuit is half-open and trial is in progress" {
		t.Errorf("Expected concurrency error during trial, got %v", err)
	}

	<-doneSignal
	if cb.GetState() != "Closed" {
		t.Errorf("Expected state Closed after trial finished, got %s", cb.GetState())
	}
}

func TestCircuitBreaker_Concurrency(t *testing.T) {
	cb := NewCircuitBreaker("TestCB_Concur", 100, 1*time.Second, "")
	var wg sync.WaitGroup
	workers := 10
	iterations := 100

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				cb.Execute(func() error {
					return nil
				})
			}
		}()
	}

	wg.Wait()
	fail, threshold := cb.GetMetrics()
	if fail != 0 {
		t.Errorf("Expected 0 failures, got %d", fail)
	}
	if threshold != 100 {
		t.Errorf("Expected threshold 100, got %d", threshold)
	}
}
