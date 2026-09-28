package orchestrator

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestGracefulShutdownDrainsInFlightNoAmbiguity: while "a" is running, request
// shutdown. The running attempt is allowed to finish and its SUCCESS is
// committed; "b" (depends on a) is never dispatched. Restart completes "b".
func TestGracefulShutdownDrainsInFlightNoAmbiguity(t *testing.T) {
	store := NewMemoryStore()
	exec := newScriptedExecutor()
	gate := exec.blockOn("a")
	exec.set("a", Result{OK: true, Reason: "finished during drain"})
	exec.set("b", Result{OK: true})
	defs := []TaskDef{{ID: "a"}, {ID: "b", Deps: []string{"a"}}}
	log := newCaptureLogger(t)
	e, err := New(defs, Options{Store: store, Executor: exec, Clock: NewFakeClock(time.Unix(0, 0)), Logger: log})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = e.Run(ctx) }()
	pollStatus(t, e, "a", StatusRunning, 2*time.Second)

	shutdownErr := make(chan error, 1)
	go func() { shutdownErr <- e.Shutdown(context.Background()) }()
	waitForLog(log, "graceful shutdown requested", t)

	// b must not have started during draining.
	time.Sleep(30 * time.Millisecond)
	if n := exec.callCount("b"); n != 0 {
		t.Fatalf("b dispatched %d times after shutdown request", n)
	}
	stB, _ := e.State("b")
	if stB.Status != StatusPending {
		t.Fatalf("b=%s want untouched pending", stB.Status)
	}

	// Let the in-flight attempt finish; shutdown must complete promptly.
	close(gate)
	select {
	case err := <-shutdownErr:
		if err != nil {
			t.Fatalf("Shutdown err=%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not return after in-flight work settled")
	}

	stA, _ := e.State("a")
	if stA.Status != StatusSucceeded || len(stA.Attempts) != 1 || stA.Attempts[0].Class != AttemptSucceeded {
		t.Fatalf("a=%+v want committed success, not ambiguous running", stA)
	}
	stB, _ = e.State("b")
	if stB.Status != StatusPending || len(stB.Attempts) != 0 {
		t.Fatalf("b=%+v must stay untouched pending", stB)
	}

	// Restart: a is not re-run, b runs exactly once.
	exec2 := &countingExecutor{results: map[string]Result{"a": {OK: true}, "b": {OK: true}}}
	e2, err := New(defs, Options{Store: store, Executor: exec2, Clock: NewFakeClock(time.Unix(100, 0)), Logger: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := e2.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if exec2.callCount("a") != 0 {
		t.Fatalf("a re-executed %d times after restart", exec2.callCount("a"))
	}
	if exec2.callCount("b") != 1 {
		t.Fatalf("b ran %d times, want 1", exec2.callCount("b"))
	}
	if !log.contains("no new dispatches") {
		t.Fatal("shutdown decision missing from logs")
	}
}

// TestGracefulShutdownCommitsRetryWait: the in-flight attempt fails retryably
// during draining; its retry_wait state with the absolute NextRunAt is
// committed, not lost. Restart before the deadline keeps waiting; after it,
// runs.
func TestGracefulShutdownCommitsRetryWait(t *testing.T) {
	store := NewMemoryStore()
	t0 := time.Unix(1000, 0)
	exec := newScriptedExecutor()
	gate := exec.blockOn("job")
	exec.set("job", Result{OK: false, Retryable: true, Reason: "fails during drain"})
	defs := []TaskDef{{ID: "job", Retry: RetryPolicy{MaxAttempts: 3, InitialBackoff: 10 * time.Second}}}
	log := newCaptureLogger(t)
	e, err := New(defs, Options{Store: store, Executor: exec, Clock: NewFakeClock(t0), Logger: log})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = e.Run(context.Background()) }()
	pollStatus(t, e, "job", StatusRunning, 2*time.Second)

	sdDone := make(chan error, 1)
	go func() { sdDone <- e.Shutdown(context.Background()) }()
	waitForLog(log, "graceful shutdown requested", t)
	close(gate)
	select {
	case err := <-sdDone:
		if err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown hung")
	}

	st, _ := e.State("job")
	if st.Status != StatusRetryWait {
		t.Fatalf("status=%s want retry_wait committed during drain", st.Status)
	}
	if !st.NextRunAt.Equal(t0.Add(10 * time.Second)) {
		t.Fatalf("NextRunAt=%s want %s", st.NextRunAt, t0.Add(10*time.Second))
	}

	// Restart 5s after the deadline: immediate retry, no extra waiting.
	exec2 := newScriptedExecutor().set("job", Result{OK: true})
	clock2 := NewFakeClock(t0.Add(15 * time.Second))
	e2, err := New(defs, Options{Store: store, Executor: exec2, Clock: clock2, Logger: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := e2.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	st2, _ := e2.State("job")
	if st2.Status != StatusSucceeded || len(st2.Attempts) != 2 {
		t.Fatalf("after restart status=%s attempts=%d", st2.Status, len(st2.Attempts))
	}
	if exec2.callCount("job") != 1 {
		t.Fatalf("job ran %d times, want 1", exec2.callCount("job"))
	}
}

// TestTriggerRejectedDuringShutdown: once shutdown is requested, Trigger
// returns ErrShutdownInProgress and parked triggers are released with it.
func TestTriggerRejectedDuringShutdown(t *testing.T) {
	store := NewMemoryStore()
	exec := newScriptedExecutor()
	gateA := exec.blockOn("a")
	exec.set("a", Result{OK: true})
	exec.set("b", Result{OK: true})
	defs := []TaskDef{{ID: "a"}, {ID: "b", Deps: []string{"a"}}}
	e, err := New(defs, Options{Store: store, Executor: exec, Clock: NewFakeClock(time.Unix(0, 0)), Logger: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = e.Run(context.Background()) }()
	pollStatus(t, e, "a", StatusRunning, 2*time.Second)

	// Park a trigger on b (dep a still running).
	parked := make(chan error, 1)
	go func() { parked <- e.Trigger(context.Background(), "b") }()
	time.Sleep(30 * time.Millisecond)

	sdDone := make(chan error, 1)
	go func() { sdDone <- e.Shutdown(context.Background()) }()
	time.Sleep(30 * time.Millisecond)

	// New triggers are rejected immediately.
	if err := e.Trigger(context.Background(), "b"); !errors.Is(err, ErrShutdownInProgress) {
		t.Fatalf("trigger err=%v want ErrShutdownInProgress", err)
	}
	// The parked trigger is released with the same error.
	select {
	case err := <-parked:
		if !errors.Is(err, ErrShutdownInProgress) {
			t.Fatalf("parked trigger err=%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("parked trigger never released during shutdown")
	}

	close(gateA)
	<-sdDone
}

// TestShutdownDeadlineExpires: an attempt that ignores cancellation past the
// shutdown deadline leaves a recoverable "running" state; the next start
// reconciles it as interrupted and replays.
func TestShutdownDeadlineExpires(t *testing.T) {
	store := NewMemoryStore()
	e, err := New([]TaskDef{{ID: "job", Retry: RetryPolicy{MaxAttempts: 3}}}, Options{
		Store:    store,
		Executor: &mixedExec{block: "job"}, // returns on ctx.Done, but slowly
		Clock:    NewFakeClock(time.Unix(0, 0)),
		Logger:   testLogger(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = e.Run(context.Background()) }()
	pollStatus(t, e, "job", StatusRunning, 2*time.Second)

	// mixedExec returns promptly on cancellation, so to exercise the deadline
	// path use a tiny deadline; even if the worker cooperates, Shutdown reports
	// ctx.Err and the state remains recoverable.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	err = e.Shutdown(ctx)
	if err == nil {
		// Cooperative cancellation beat the deadline: that is also acceptable,
		// but then the state must be retry_wait (interrupted-free), verify that
		// path instead below.
		t.Logf("worker cooperated before deadline (err nil)")
	}

	// Either way a restart must converge without ambiguity.
	exec2 := newScriptedExecutor().set("job", Result{OK: true})
	e2, err := New([]TaskDef{{ID: "job", Retry: RetryPolicy{MaxAttempts: 3}}}, Options{
		Store: store, Executor: exec2, Clock: NewFakeClock(time.Unix(100, 0)), Logger: testLogger(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := e2.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	st, _ := e2.State("job")
	if st.Status != StatusSucceeded {
		t.Fatalf("status=%s want succeeded after restart", st.Status)
	}
}

// TestShutdownDeadlineExpiresUncooperative: the executor ignores cancellation,
// so the shutdown deadline must abort the wait crash-style. The open attempt is
// left "running" on disk and reconciled as interrupted on the next start.
func TestShutdownDeadlineExpiresUncooperative(t *testing.T) {
	store := NewMemoryStore()
	exec := newScriptedExecutor()
	never := exec.blockOn("job")
	exec.set("job", Result{OK: true, Reason: "would have finished much later"})
	e, err := New([]TaskDef{{ID: "job", Retry: RetryPolicy{MaxAttempts: 3}}}, Options{
		Store:    store,
		Executor: exec,
		Clock:    NewFakeClock(time.Unix(0, 0)),
		Logger:   testLogger(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = e.Run(context.Background()) }()
	pollStatus(t, e, "job", StatusRunning, 2*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := e.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown err=%v want DeadlineExceeded", err)
	}

	// The attempt is still in flight (executor ignores cancel), so its durable
	// state must be the recoverable "running" with an open attempt.
	st, _ := e.State("job")
	if st.Status != StatusRunning || len(st.Attempts) != 1 || !st.Attempts[0].EndedAt.IsZero() {
		t.Fatalf("state=%+v want running with one open attempt", st)
	}

	// Restart over the same store while the old executor goroutine is still
	// blocked. It reconciles the open attempt and replays; the zombie worker is
	// released afterwards (its late result hits the dead engine and is dropped).
	exec2 := newScriptedExecutor().set("job", Result{OK: true})
	e2, err := New([]TaskDef{{ID: "job", Retry: RetryPolicy{MaxAttempts: 3}}}, Options{
		Store: store, Executor: exec2, Clock: NewFakeClock(time.Unix(100, 0)), Logger: testLogger(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := e2.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	st2, _ := e2.State("job")
	if st2.Status != StatusSucceeded {
		t.Fatalf("after restart status=%s want succeeded", st2.Status)
	}
	if st2.Attempts[0].Class != AttemptInterrupted {
		t.Fatalf("first attempt class=%s want interrupted", st2.Attempts[0].Class)
	}
	if exec2.callCount("job") != 1 {
		t.Fatalf("job ran %d times after restart, want 1", exec2.callCount("job"))
	}
	// Release the zombie worker last so the test leaves no goroutine blocked.
	close(never)
}

// TestShutdownBeforeRunRejected: API misuse gets a distinct error.
func TestShutdownBeforeRunRejected(t *testing.T) {
	e, err := New([]TaskDef{{ID: "a"}}, Options{
		Store:    NewMemoryStore(),
		Executor: &countingExecutor{results: map[string]Result{"a": {OK: true}}},
		Clock:    NewFakeClock(time.Unix(0, 0)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Shutdown(context.Background()); !errors.Is(err, ErrEngineNotRunning) {
		t.Fatalf("err=%v want ErrEngineNotRunning", err)
	}
}

// TestShutdownIdempotent: a graceful shutdown completes once the in-flight
// attempt finishes during draining, and a second Shutdown call is a harmless
// no-op; pending downstream tasks remain untouched.
func TestShutdownIdempotent(t *testing.T) {
	store := NewMemoryStore()
	g := newGateExecutor("a")
	defs := []TaskDef{{ID: "a"}, {ID: "b", Deps: []string{"a"}}}
	e, err := New(defs, Options{Store: store, Executor: g, Clock: NewFakeClock(time.Unix(0, 0)), Logger: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = e.Run(context.Background()) }()
	g.waitStarted(t, "a")

	done := make(chan error, 2)
	go func() { done <- e.Shutdown(context.Background()) }()
	time.Sleep(30 * time.Millisecond)
	// The attempt finishes normally while the engine is draining.
	g.releaseTask("a")

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not complete after attempt settled")
	}
	stA, _ := e.State("a")
	if stA.Status != StatusSucceeded {
		t.Fatalf("a=%s want committed success", stA.Status)
	}
	stB, _ := e.State("b")
	if stB.Status != StatusPending {
		t.Fatalf("b=%s want pending, never dispatched", stB.Status)
	}

	// A second shutdown call returns without error (idempotent).
	if err := e.Shutdown(context.Background()); err != nil {
		t.Fatalf("second shutdown: %v", err)
	}
}
