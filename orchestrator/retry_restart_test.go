package orchestrator

import (
	"context"
	"testing"
	"time"
)

const (
	retryRestartBackoff = 10 * time.Second
)

// startFailingEngine runs one engine until "job" has failed once and parked in
// retry_wait with a future deadline, then returns it together with the store
// and clock so tests can stop and restart over the same state.
func startFailingEngine(t *testing.T, start time.Time, defs []TaskDef) (*Engine, *FakeClock, Store) {
	t.Helper()
	store := NewMemoryStore()
	clock := NewFakeClock(start)
	exec := newScriptedExecutor().
		set("job", Result{OK: false, Retryable: true, Reason: "transient before shutdown"}).
		set("b", Result{OK: true, Reason: "after restart"})
	log := newCaptureLogger(t)
	e, err := New(defs, Options{Store: store, Executor: exec, Clock: clock, Logger: log})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = e.Run(context.Background()) }()
	st := pollStatus(t, e, "job", StatusRetryWait, 2*time.Second)
	if !st.NextRunAt.Equal(start.Add(retryRestartBackoff)) {
		t.Fatalf("NextRunAt=%s want %s", st.NextRunAt, start.Add(retryRestartBackoff))
	}
	if !clock.WaitForTimers(1) {
		t.Fatal("backoff timer not armed before shutdown")
	}
	return e, clock, store
}

// gracefulStop shuts an engine down with no grace-period force and asserts Run
// returns ErrGracefulShutdown.
func gracefulStop(t *testing.T, e *Engine) {
	t.Helper()
	if err := e.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// TestRetryWaitSurvivesRestartBeforeDeadline: the process stops while a task is
// inside its retry backoff and comes back BEFORE the original deadline. The
// wait is not re-timed: the retry fires at the persisted deadline, and a
// pending dependent task scheduled before the shutdown still runs afterwards.
func TestRetryWaitSurvivesRestartBeforeDeadline(t *testing.T) {
	t0 := time.Unix(1000, 0)
	defs := []TaskDef{
		{ID: "job", Retry: RetryPolicy{MaxAttempts: 3, InitialBackoff: retryRestartBackoff, MaxBackoff: time.Minute}},
		{ID: "b", Deps: []string{"job"}},
	}
	e1, _, store := startFailingEngine(t, t0, defs)
	gracefulStop(t, e1)

	// Restart 4s into the 10s window: 6s of the ORIGINAL wait must remain.
	exec2 := newScriptedExecutor().
		set("job", Result{OK: true, Reason: "success after preserved wait"}).
		set("b", Result{OK: true})
	clock2 := NewFakeClock(t0.Add(4 * time.Second))
	log2 := newCaptureLogger(t)
	e2, err := New(defs, Options{Store: store, Executor: exec2, Clock: clock2, Logger: log2})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = e2.Run(context.Background()) }()

	// 5s later (t0+9) the original deadline (t0+10) has not been reached: the
	// job must still be parked and must NOT have executed again.
	if !clock2.WaitForTimers(1) {
		t.Fatal("restart did not re-arm the persisted retry deadline")
	}
	clock2.Advance(5 * time.Second)
	time.Sleep(50 * time.Millisecond)
	if n := exec2.callCount("job"); n != 0 {
		t.Fatalf("job executed %d times before original deadline", n)
	}
	st, _ := e2.State("job")
	if st.Status != StatusRetryWait {
		t.Fatalf("status=%s want retry_wait until original deadline", st.Status)
	}

	// Crossing the original deadline dispatches exactly one more attempt.
	clock2.Advance(2 * time.Second) // t0+11
	st = pollStatus(t, e2, "job", StatusSucceeded, 2*time.Second)
	if len(st.Attempts) != 2 || st.Attempts[1].Class != AttemptSucceeded {
		t.Fatalf("attempts=%+v want failed+success", st.Attempts)
	}
	if exec2.callCount("job") != 1 {
		t.Fatalf("job ran %d times after restart, want 1", exec2.callCount("job"))
	}
	// The pre-shutdown plan ("b will run once job succeeds") did not vanish:
	// once job succeeds, b is dispatched by the restarted scheduler.
	if st := pollStatus(t, e2, "b", StatusSucceeded, 2*time.Second); st.Attempts == nil {
		t.Fatal("dependent task never ran after the wait elapsed")
	}
	if exec2.callCount("b") != 1 {
		t.Fatalf("b ran %d times, want 1", exec2.callCount("b"))
	}
	if !log2.contains("re-arming original deadline") {
		t.Fatal("log does not explain the preserved retry deadline")
	}
}

// TestRetryWaitElapsedBeforeRestartDispatchesImmediately: coming back AFTER the
// persisted deadline dispatches the retry straight away — the task must not
// sit parked, and no extra wait is added.
func TestRetryWaitElapsedBeforeRestartDispatchesImmediately(t *testing.T) {
	t0 := time.Unix(1000, 0)
	defs := []TaskDef{{ID: "job", Retry: RetryPolicy{MaxAttempts: 3, InitialBackoff: retryRestartBackoff}}}
	e1, _, store := startFailingEngine(t, t0, defs)
	gracefulStop(t, e1)

	exec2 := newScriptedExecutor().set("job", Result{OK: true})
	clock2 := NewFakeClock(t0.Add(20 * time.Second)) // well past t0+10
	log2 := newCaptureLogger(t)
	e2, err := New(defs, Options{Store: store, Executor: exec2, Clock: clock2, Logger: log2})
	if err != nil {
		t.Fatal(err)
	}
	if err := e2.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	st, _ := e2.State("job")
	if st.Status != StatusSucceeded || len(st.Attempts) != 2 {
		t.Fatalf("status=%s attempts=%d, want immediate retry success", st.Status, len(st.Attempts))
	}
	if exec2.callCount("job") != 1 {
		t.Fatalf("job ran %d times, want 1", exec2.callCount("job"))
	}
	if !log2.contains("already elapsed") {
		t.Fatal("log does not explain the immediate eligibility")
	}
}

// TestRepeatedRestartsDoNotResetRetryWait: two consecutive restarts inside the
// same backoff window must not restart the clock; the retry is still due at the
// single original deadline, so total waiting equals the original backoff
// instead of growing with each restart.
func TestRepeatedRestartsDoNotResetRetryWait(t *testing.T) {
	t0 := time.Unix(1000, 0)
	defs := []TaskDef{{ID: "job", Retry: RetryPolicy{MaxAttempts: 3, InitialBackoff: retryRestartBackoff}}}
	e1, _, store := startFailingEngine(t, t0, defs)
	gracefulStop(t, e1)

	stopRestart := func(now time.Time) *Engine {
		exec := newScriptedExecutor().set("job", Result{OK: true})
		clock := NewFakeClock(now)
		e, err := New(defs, Options{Store: store, Executor: exec, Clock: clock, Logger: testLogger(t)})
		if err != nil {
			t.Fatal(err)
		}
		go func() { _ = e.Run(context.Background()) }()
		if !clock.WaitForTimers(1) {
			t.Fatal("restart did not re-arm deadline")
		}
		gracefulStop(t, e)
		return e
	}
	stopRestart(t0.Add(4 * time.Second))
	// Third incarnation starts at t0+8. A reset implementation would arm the
	// timer for t0+18; the preserved deadline must still be t0+10.
	exec3 := newScriptedExecutor().set("job", Result{OK: true})
	clock3 := NewFakeClock(t0.Add(8 * time.Second))
	e3, err := New(defs, Options{Store: store, Executor: exec3, Clock: clock3, Logger: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = e3.Run(context.Background()) }()
	if !clock3.WaitForTimers(1) {
		t.Fatal("timer not re-armed after second restart")
	}
	clock3.Advance(1 * time.Second) // t0+9: still before the original t0+10
	time.Sleep(50 * time.Millisecond)
	if n := exec3.callCount("job"); n != 0 {
		t.Fatalf("wait was reset: job ran at t0+9, %d call(s)", n)
	}
	clock3.Advance(2 * time.Second) // t0+11: past original deadline
	pollStatus(t, e3, "job", StatusSucceeded, 2*time.Second)
	if exec3.callCount("job") != 1 {
		t.Fatalf("job ran %d times, want 1", exec3.callCount("job"))
	}
}

// TestAttemptFailingDuringDrainCommitsRetryWait: the in-flight attempt finishes
// with a retryable failure while draining. Its retry_wait state (with the
// absolute NextRunAt) is committed before Shutdown returns; restart honors the
// deadline exactly like a pre-existing wait.
func TestAttemptFailingDuringDrainCommitsRetryWait(t *testing.T) {
	t0 := time.Unix(500, 0)
	defs := []TaskDef{{ID: "job", Retry: RetryPolicy{MaxAttempts: 3, InitialBackoff: retryRestartBackoff}}}
	store := NewMemoryStore()
	clock := NewFakeClock(t0)
	exec := newScriptedExecutor()
	exec.blockOn("job")
	exec.set("job", Result{OK: false, Retryable: true, Reason: "dies during drain"})
	log := newCaptureLogger(t)
	e1, err := New(defs, Options{Store: store, Executor: exec, Clock: clock, Logger: log})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = e1.Run(context.Background()) }()
	pollStatus(t, e1, "job", StatusRunning, 2*time.Second)

	done := make(chan struct{})
	go func() { _ = e1.Shutdown(context.Background()); close(done) }()
	time.Sleep(50 * time.Millisecond) // drain requested while attempt blocked
	close(exec.block["job"])          // attempt fails during the drain
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not return after attempt failed")
	}
	st, _ := e1.State("job")
	if st.Status != StatusRetryWait {
		t.Fatalf("status=%s want committed retry_wait", st.Status)
	}
	if !st.NextRunAt.Equal(t0.Add(retryRestartBackoff)) {
		t.Fatalf("NextRunAt=%s want %s", st.NextRunAt, t0.Add(retryRestartBackoff))
	}
	if n := exec.callCount("job"); n != 1 {
		t.Fatalf("attempt ran %d times during drain, want 1", n)
	}

	// Restart at the deadline: one more attempt succeeds.
	exec2 := newScriptedExecutor().set("job", Result{OK: true})
	e2, err := New(defs, Options{Store: store, Executor: exec2, Clock: NewFakeClock(t0.Add(retryRestartBackoff)), Logger: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := e2.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	st, _ = e2.State("job")
	if st.Status != StatusSucceeded || len(st.Attempts) != 2 {
		t.Fatalf("after restart status=%s attempts=%d", st.Status, len(st.Attempts))
	}
	if exec2.callCount("job") != 1 {
		t.Fatalf("job replayed %d times, want 1", exec2.callCount("job"))
	}
}
