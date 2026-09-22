package orchestrator

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestFileStoreRoundTrip: an engine run writes JSON that a second engine can
// load and resume from, on disk.
func TestFileStoreRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	defs := []TaskDef{
		{ID: "a", Retry: RetryPolicy{MaxAttempts: 2, InitialBackoff: time.Minute}},
		{ID: "b", Deps: []string{"a"}},
	}

	exec1 := newScriptedExecutor().
		set("a", Result{OK: false, Retryable: true, Reason: "flaky"}, Result{OK: true}).
		set("b", Result{OK: true})
	clock := NewFakeClock(time.Unix(5000, 0))
	e1, err := New(defs, Options{
		Store: NewFileStore(path), Executor: exec1, Clock: clock,
		Logger: testLogger(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = e1.Run(context.Background()) }()
	pollStatus(t, e1, "a", StatusRetryWait, 2*time.Second)
	clock.WaitForTimers(1)
	clock.Advance(time.Hour)
	pollTerminal(t, e1, 2*time.Second)

	if info, err := os.Stat(path); err != nil || info.Size() == 0 {
		t.Fatalf("state file not durably written: %v size=%d", err, func() int64 {
			if info != nil {
				return info.Size()
			}
			return 0
		}())
	}

	// Reload from disk: both tasks are already done, nothing re-runs.
	exec2 := newScriptedExecutor()
	e2, err := New(defs, Options{
		Store: NewFileStore(path), Executor: exec2,
		Clock: NewFakeClock(time.Unix(9999, 0)), Logger: testLogger(t),
	})
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if err := e2.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if exec2.callCount("a") != 0 || exec2.callCount("b") != 0 {
		t.Fatalf("tasks re-executed after disk reload: a=%d b=%d", exec2.callCount("a"), exec2.callCount("b"))
	}
}

// TestFileStoreLoadMissingReturnsNoSnapshot:
func TestFileStoreLoadMissingReturnsNoSnapshot(t *testing.T) {
	_, err := NewFileStore(filepath.Join(t.TempDir(), "missing.json")).Load()
	if !errors.Is(err, ErrNoSnapshot) {
		t.Fatalf("err=%v want ErrNoSnapshot", err)
	}
}

// TestConcurrentTriggersAfterDepsProduceOneSuccess: many goroutines trigger a
// blocked downstream task at the moment its dependency completes; only one
// attempt and one success effect may ever be committed.
func TestConcurrentTriggersAfterDepsProduceOneSuccess(t *testing.T) {
	exec := newScriptedExecutor()
	gateB := exec.blockOn("b")
	exec.set("a", Result{OK: true})
	exec.set("b", Result{OK: true})
	defs := []TaskDef{{ID: "a"}, {ID: "b", Deps: []string{"a"}}}
	e, err := New(defs, Options{
		Store: NewMemoryStore(), Executor: exec,
		Clock: NewFakeClock(time.Unix(0, 0)), Logger: testLogger(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = e1Noop(e) }()

	// b auto-dispatches when a completes; wait until running, then hammer.
	pollStatus(t, e, "b", StatusRunning, 2*time.Second)
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = e.Trigger(context.Background(), "b")
		}()
	}
	wg.Wait()
	close(gateB)
	st := pollStatus(t, e, "b", StatusSucceeded, 2*time.Second)
	if len(st.Attempts) != 1 || st.Attempts[0].Class != AttemptSucceeded {
		t.Fatalf("unexpected attempts: %+v", st.Attempts)
	}
	if exec.callCount("b") != 1 {
		t.Fatalf("b executed %d times, want 1", exec.callCount("b"))
	}
}

// e1Noop keeps the engine reference used by the goroutine naming above honest.
func e1Noop(e *Engine) error { return e.Run(context.Background()) }
