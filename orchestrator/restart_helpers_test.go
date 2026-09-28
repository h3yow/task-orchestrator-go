package orchestrator

import (
	"context"
	"testing"
	"time"
)

// runEngine starts Run under a cancellable context and returns a stop function
// that cancels and waits for Run to return.
func runEngine(t *testing.T, e *Engine) (stop func(error)) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	runCh := make(chan error, 1)
	go func() { runCh <- e.Run(ctx) }()
	return func(want error) {
		t.Helper()
		cancel()
		select {
		case err := <-runCh:
			if want != nil && err != want {
				t.Fatalf("Run returned %v, want %v", err, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Run did not return after cancel")
		}
	}
}

// waitForLog polls a capture logger for a substring.
func waitForLog(log *captureLogger, sub string, t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if log.contains(sub) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("log never contained %q; full log:\n%s", sub, log.String())
}
