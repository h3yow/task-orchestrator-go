package orchestrator

import (
	"context"
	"testing"
	"time"
)

func pollStatus(t *testing.T, e *Engine, id string, want Status, d time.Duration) TaskState {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		st, _ := e.State(id)
		if st.Status == want {
			return st
		}
		time.Sleep(time.Millisecond)
	}
	st, _ := e.State(id)
	t.Fatalf("task %s status=%s, never reached %s", id, st.Status, want)
	return st
}

// pollAttempts waits until the task recorded at least n attempts (used after
// advancing the fake clock, since the timer event is delivered asynchronously).
func pollAttempts(t *testing.T, e *Engine, id string, n int, d time.Duration) TaskState {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		st, _ := e.State(id)
		if len(st.Attempts) >= n {
			return st
		}
		time.Sleep(time.Millisecond)
	}
	st, _ := e.State(id)
	t.Fatalf("task %s attempts=%d, never reached %d (status=%s)", id, len(st.Attempts), n, st.Status)
	return st
}

func pollTerminal(t *testing.T, e *Engine, d time.Duration) map[string]TaskState {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		select {
		case <-e.settled:
			out := map[string]TaskState{}
			for id := range e.defs {
				st, _ := e.State(id)
				out[id] = st
			}
			return out
		case <-time.After(2 * time.Millisecond):
		}
	}
	t.Fatal("engine never settled")
	return nil
}

// TestRetrySucceedsOnThirdAttempt verifies attempt accounting, per-attempt
// reasons, and FakeClock-driven backoff with zero real waiting.
func TestRetrySucceedsOnThirdAttempt(t *testing.T) {
	clock := NewFakeClock(time.Unix(1000, 0))
	exec := newScriptedExecutor().set("job",
		Result{OK: false, Retryable: true, Reason: "transient-1"},
		Result{OK: false, Retryable: true, Reason: "transient-2"},
		Result{OK: true, Reason: "done"},
	)
	defs := []TaskDef{{ID: "job", Retry: RetryPolicy{
		MaxAttempts:    3,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     time.Second,
	}}}
	log := newCaptureLogger(t)
	e, err := New(defs, Options{Store: NewMemoryStore(), Executor: exec, Clock: clock, Logger: log})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = e.Run(context.Background()) }()

	st := pollStatus(t, e, "job", StatusRetryWait, 2*time.Second)
	if len(st.Attempts) != 1 || st.Attempts[0].Reason != "transient-1" {
		t.Fatalf("attempt record wrong: %+v", st.Attempts)
	}
	if !clock.WaitForTimers(1) {
		t.Fatal("retry timer never armed")
	}
	clock.Advance(11 * time.Millisecond)

	// Timer event delivery is async; wait until attempt #2 has been recorded.
	st = pollAttempts(t, e, "job", 2, 2*time.Second)
	pollStatus(t, e, "job", StatusRetryWait, 2*time.Second)
	if st.Attempts[1].Reason != "transient-2" {
		t.Fatalf("attempt record wrong: %+v", st.Attempts)
	}
	if !clock.WaitForTimers(1) {
		t.Fatal("second retry timer never armed")
	}
	// Linear backoff: attempt #3 waits 2*InitialBackoff = 20ms.
	clock.Advance(21 * time.Millisecond)

	st = pollStatus(t, e, "job", StatusSucceeded, 2*time.Second)
	if len(st.Attempts) != 3 || st.Attempts[2].Class != AttemptSucceeded {
		t.Fatalf("attempt record wrong: %+v", st.Attempts)
	}
	if exec.callCount("job") != 3 {
		t.Fatalf("executor ran %d times, want 3", exec.callCount("job"))
	}
	if !log.contains("retryable failure") || !log.contains("retry timer fired") {
		t.Fatal("logs missing scheduling/retry decisions")
	}
}

// TestNonRetryableFailsImmediately: retryable and non-retryable failures are
// treated differently.
func TestNonRetryableFailsImmediately(t *testing.T) {
	clock := NewFakeClock(time.Unix(1000, 0))
	exec := newScriptedExecutor().set("job",
		Result{OK: false, Retryable: false, Reason: "poison message"},
	)
	defs := []TaskDef{{ID: "job", Retry: RetryPolicy{MaxAttempts: 5, InitialBackoff: time.Hour}}}
	log := newCaptureLogger(t)
	e, err := New(defs, Options{Store: NewMemoryStore(), Executor: exec, Clock: clock, Logger: log})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	st, _ := e.State("job")
	if st.Status != StatusFailed {
		t.Fatalf("status=%s", st.Status)
	}
	if len(st.Attempts) != 1 {
		t.Fatalf("attempts=%d, non-retryable task must not be retried", len(st.Attempts))
	}
	if st.Attempts[0].Retryable {
		t.Fatal("attempt marked retryable")
	}
	if exec.callCount("job") != 1 {
		t.Fatalf("ran %d times", exec.callCount("job"))
	}
	if !log.contains("non-retryable") {
		t.Fatal("log missing non-retryable decision")
	}
}

// TestRetryBudgetExhausted: all retryable failures -> deterministic terminal
// failed once MaxAttempts is reached.
func TestRetryBudgetExhausted(t *testing.T) {
	clock := NewFakeClock(time.Unix(0, 0))
	exec := newScriptedExecutor().set("job",
		Result{OK: false, Retryable: true, Reason: "boom-1"},
		Result{OK: false, Retryable: true, Reason: "boom-2"},
		Result{OK: false, Retryable: true, Reason: "boom-3"},
	)
	defs := []TaskDef{{ID: "job", Retry: RetryPolicy{MaxAttempts: 3, InitialBackoff: 5 * time.Millisecond}}}
	e, err := New(defs, Options{Store: NewMemoryStore(), Executor: exec, Clock: clock, Logger: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = e.Run(context.Background()) }()

	for i := 0; i < 2; i++ {
		pollStatus(t, e, "job", StatusRetryWait, 2*time.Second)
		if !clock.WaitForTimers(1) {
			t.Fatal("retry timer not armed")
		}
		clock.Advance(6 * time.Millisecond)
		pollAttempts(t, e, "job", i+2, 2*time.Second)
	}
	st := pollStatus(t, e, "job", StatusFailed, 2*time.Second)
	if len(st.Attempts) != 3 {
		t.Fatalf("attempts=%d want 3", len(st.Attempts))
	}
	if st.Attempts[2].Reason != "boom-3" {
		t.Fatalf("last reason=%q", st.Attempts[2].Reason)
	}
	if exec.callCount("job") != 3 {
		t.Fatalf("ran %d times", exec.callCount("job"))
	}
}

// TestAttemptTimeoutRetriedThenExhausted drives the per-attempt timeout timer
// via the FakeClock: both attempts time out, yielding a deterministic failure.
func TestAttemptTimeoutRetriedThenExhausted(t *testing.T) {
	clock := NewFakeClock(time.Unix(0, 0))
	exec := &alwaysBlockExecutor{}
	defs := []TaskDef{{ID: "slow", Timeout: 10 * time.Millisecond, Retry: RetryPolicy{MaxAttempts: 2, InitialBackoff: 3 * time.Millisecond}}}
	log := newCaptureLogger(t)
	e, err := New(defs, Options{Store: NewMemoryStore(), Executor: exec, Clock: clock, Logger: log})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = e.Run(context.Background()) }()

	for attempt := 1; attempt <= 2; attempt++ {
		pollAttempts(t, e, "slow", attempt, 2*time.Second)
		pollStatus(t, e, "slow", StatusRunning, 2*time.Second)
		if !clock.WaitForTimers(1) {
			t.Fatalf("timeout timer for attempt %d not armed", attempt)
		}
		clock.Advance(11 * time.Millisecond)
		if attempt == 1 {
			// attempt 1 -> retry_wait, backoff timer armed
			pollStatus(t, e, "slow", StatusRetryWait, 2*time.Second)
			if !clock.WaitForTimers(1) {
				t.Fatal("backoff timer not armed")
			}
			clock.Advance(4 * time.Millisecond)
		}
	}
	st := pollStatus(t, e, "slow", StatusFailed, 2*time.Second)
	if len(st.Attempts) != 2 {
		t.Fatalf("attempts=%d want 2", len(st.Attempts))
	}
	for i, a := range st.Attempts {
		if a.Class != AttemptTimeout {
			t.Fatalf("attempt %d class=%s want timeout", i+1, a.Class)
		}
	}
	if exec.count() != 2 {
		t.Fatalf("executor ran %d times want 2", exec.count())
	}
	if !log.contains("TIMED OUT") {
		t.Fatal("timeout decision not logged")
	}
}
