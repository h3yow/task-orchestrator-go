package orchestrator

import (
	"context"
	"errors"
	"testing"
	"time"
)

// drainGateExecutor blocks task execution until its gate is closed, while
// still honoring context cancellation. Tests use it to hold an attempt in
// flight at the moment Shutdown is requested, then choose whether it finishes
// naturally or is force-cancelled.
type drainGateExecutor struct {
	gate   chan struct{}
	ok     bool
	reason string
}

func (g *drainGateExecutor) Execute(ctx context.Context, id string) Result {
	select {
	case <-g.gate:
		return Result{OK: g.ok, Reason: g.reason}
	case <-ctx.Done():
		return Result{OK: false, Retryable: true, Reason: "cancelled"}
	}
}

// TestGracefulShutdownWaitsForInFlight: with no deadline, Shutdown lets an
// in-flight attempt finish naturally, commits its result, and stops without
// dispatching anything new. Restart resumes the remaining DAG exactly once.
func TestGracefulShutdownWaitsForInFlight(t *testing.T) {
	store := NewMemoryStore()
	defs := []TaskDef{
		{ID: "a", Retry: RetryPolicy{MaxAttempts: 3}},
		{ID: "b", Deps: []string{"a"}},
	}
	exec := &drainGateExecutor{gate: make(chan struct{}), ok: true, reason: "finished during drain"}
	log := newCaptureLogger(t)
	e, err := New(defs, Options{Store: store, Executor: exec, Clock: NewFakeClock(time.Unix(0, 0)), Logger: log})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- e.Run(ctx) }()

	pollStatus(t, e, "a", StatusRunning, 2*time.Second)

	shutdownDone := make(chan struct{})
	go func() {
		_ = e.Shutdown(context.Background()) // unbounded grace period
		close(shutdownDone)
	}()

	// Shutdown must NOT return while the attempt is still running.
	select {
	case <-shutdownDone:
		t.Fatal("Shutdown returned while attempt still in flight")
	case <-time.After(50 * time.Millisecond):
	}
	stB, _ := e.State("b")
	if stB.Status != StatusPending || stB.Attempts != nil {
		t.Fatalf("b=%+v, must remain untouched pending during drain", stB)
	}

	// Let the in-flight attempt succeed; the drain then completes.
	close(exec.gate)
	select {
	case <-shutdownDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not return after in-flight attempt finished")
	}
	select {
	case err := <-runErr:
		if !errors.Is(err, ErrGracefulShutdown) {
			t.Fatalf("Run err=%v want ErrGracefulShutdown", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after shutdown")
	}

	stA, _ := e.State("a")
	if stA.Status != StatusSucceeded || len(stA.Attempts) != 1 ||
		stA.Attempts[0].Class != AttemptSucceeded || stA.Attempts[0].Reason != "finished during drain" {
		t.Fatalf("a=%+v, want natural success committed", stA)
	}
	stB, _ = e.State("b")
	if stB.Status != StatusPending || len(stB.Attempts) != 0 {
		t.Fatalf("b=%+v, must never have been dispatched", stB)
	}
	if !log.contains("drain") {
		t.Fatal("drain decisions not logged")
	}

	// Restart over the same store: a never re-runs, b runs once, whole DAG settles.
	exec2 := newScriptedExecutor().set("a", Result{OK: true}).set("b", Result{OK: true, Reason: "after restart"})
	e2, err := New(defs, Options{Store: store, Executor: exec2, Clock: NewFakeClock(time.Unix(1000, 0)), Logger: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := e2.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if exec2.callCount("a") != 0 {
		t.Fatalf("succeeded task re-executed: %d", exec2.callCount("a"))
	}
	if exec2.callCount("b") != 1 {
		t.Fatalf("b ran %d times after restart, want 1", exec2.callCount("b"))
	}
}

// TestGracefulShutdownForcesAfterDeadline: when the grace period expires the
// in-flight attempt's context is cancelled; it is closed as "interrupted" and
// parked in retry_wait (persisted immediately). A restart replays it once.
func TestGracefulShutdownForcesAfterDeadline(t *testing.T) {
	dir := t.TempDir()
	store := NewFileStore(dir + "/state.json")
	defs := []TaskDef{{ID: "job", Retry: RetryPolicy{MaxAttempts: 3}}}
	log := newCaptureLogger(t)
	e, err := New(defs, Options{Store: store, Executor: &drainGateExecutor{gate: make(chan struct{})}, Clock: NewFakeClock(time.Unix(0, 0)), Logger: log})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = e.Run(context.Background()) }()
	pollStatus(t, e, "job", StatusRunning, 2*time.Second)

	sctx, scancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer scancel()
	if err := e.Shutdown(sctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	st, _ := e.State("job")
	if st.Status != StatusRetryWait {
		t.Fatalf("status=%s want retry_wait", st.Status)
	}
	if len(st.Attempts) != 1 || st.Attempts[0].Class != AttemptInterrupted {
		t.Fatalf("attempts=%+v, want one interrupted attempt", st.Attempts)
	}
	if !st.NextRunAt.Equal(time.Unix(0, 0)) {
		t.Fatalf("NextRunAt=%s, interrupted-during-drain parks at the shutdown instant", st.NextRunAt)
	}

	// The reconciliation must be durable: a brand new engine reads it straight
	// from disk and sees NO running tasks to recover.
	exec2 := newScriptedExecutor().set("job", Result{OK: true, Reason: "after forced drain"})
	e2, err := New(defs, Options{Store: store, Executor: exec2, Clock: NewFakeClock(time.Unix(100, 0)), Logger: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := e2.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	st, _ = e2.State("job")
	if st.Status != StatusSucceeded || len(st.Attempts) != 2 {
		t.Fatalf("after restart status=%s attempts=%d, want succeeded with interrupted+success", st.Status, len(st.Attempts))
	}
	if exec2.callCount("job") != 1 {
		t.Fatalf("job replayed %d times, want exactly 1", exec2.callCount("job"))
	}
	if !log.contains("forced") || !log.contains("interrupted by forced drain") {
		t.Fatal("forced-drain decisions not logged")
	}
}

// TestGracefulShutdownForceExhaustsBudget: a forced drain on the last allowed
// attempt terminally fails the task with an explicit, persisted cause.
func TestGracefulShutdownForceExhaustsBudget(t *testing.T) {
	store := NewMemoryStore()
	defs := []TaskDef{{ID: "job", Retry: RetryPolicy{MaxAttempts: 1}}}
	e, err := New(defs, Options{
		Store:    store,
		Executor: &drainGateExecutor{gate: make(chan struct{})},
		Clock:    NewFakeClock(time.Unix(0, 0)),
		Logger:   testLogger(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = e.Run(context.Background()) }()
	pollStatus(t, e, "job", StatusRunning, 2*time.Second)

	sctx, scancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer scancel()
	if err := e.Shutdown(sctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	st, _ := e.State("job")
	if st.Status != StatusFailed || st.Attempts[len(st.Attempts)-1].Class != AttemptInterrupted {
		t.Fatalf("state=%+v, want terminal failed ending in interrupted attempt", st)
	}

	// Restart must keep it failed and never execute.
	exec2 := newScriptedExecutor().set("job", Result{OK: true})
	e2, err := New(defs, Options{Store: store, Executor: exec2, Clock: NewFakeClock(time.Unix(10, 0)), Logger: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := e2.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if exec2.callCount("job") != 0 {
		t.Fatalf("terminally failed task executed after restart: %d", exec2.callCount("job"))
	}
}

// TestShutdownBeforeRunIsNoOp: Shutdown on an engine whose Run was never called
// returns nil without touching persisted state.
func TestShutdownBeforeRunIsNoOp(t *testing.T) {
	store := NewMemoryStore()
	defs := []TaskDef{
		{ID: "a"},
		{ID: "b", Deps: []string{"a"}},
	}
	e, err := New(defs, Options{
		Store:    store,
		Executor: &countingExecutor{results: map[string]Result{"a": {OK: true}, "b": {OK: true}}},
		Clock:    NewFakeClock(time.Unix(0, 0)),
		Logger:   testLogger(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown before Run: %v", err)
	}
	snap := e.Snapshot()
	if snap.Version != 0 {
		t.Fatalf("version=%d, state must be untouched", snap.Version)
	}
}

// TestShutdownRacingStartup: Shutdown called right after "go Run()" must wait
// for the scheduler to come up and then drain (rather than returning as if Run
// had never started). The in-flight attempt is force-cancelled by the deadline
// and the restart converges.
func TestShutdownRacingStartup(t *testing.T) {
	store := NewMemoryStore()
	defs := []TaskDef{{ID: "a", Retry: RetryPolicy{MaxAttempts: 2}}}
	e, err := New(defs, Options{
		Store:    store,
		Executor: &drainGateExecutor{gate: make(chan struct{})},
		Clock:    NewFakeClock(time.Unix(0, 0)),
		Logger:   testLogger(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	runErr := make(chan error, 1)
	go func() { runErr <- e.Run(context.Background()) }()

	sctx, scancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer scancel()
	if err := e.Shutdown(sctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if err := <-runErr; !errors.Is(err, ErrGracefulShutdown) {
		t.Fatalf("Run err=%v want ErrGracefulShutdown", err)
	}
	st, _ := e.State("a")
	if st.Status != StatusRetryWait || st.Attempts[0].Class != AttemptInterrupted {
		t.Fatalf("state=%+v want retry_wait with interrupted attempt", st)
	}

	exec2 := newScriptedExecutor().set("a", Result{OK: true})
	e2, err := New(defs, Options{Store: store, Executor: exec2, Clock: NewFakeClock(time.Unix(10, 0)), Logger: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := e2.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if exec2.callCount("a") != 1 {
		t.Fatalf("a ran %d times after restart, want 1", exec2.callCount("a"))
	}
}

// TestTriggersRejectedDuringDrain: after Shutdown is requested, a new Trigger
// is refused and a Trigger already parked on pending dependencies is released
// with ErrEngineNotRunning; neither causes a dispatch.
func TestTriggersRejectedDuringDrain(t *testing.T) {
	store := NewMemoryStore()
	defs := []TaskDef{
		{ID: "a"},
		{ID: "b", Deps: []string{"a"}},
	}
	exec := newScriptedExecutor()
	gateA := exec.blockOn("a")
	exec.set("a", Result{OK: true}).set("b", Result{OK: true})
	e, err := New(defs, Options{Store: store, Executor: exec, Clock: NewFakeClock(time.Unix(0, 0)), Logger: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = e.Run(context.Background()) }()
	pollStatus(t, e, "a", StatusRunning, 2*time.Second)

	// Park a trigger for b while its dep a is still running.
	parked := make(chan error, 1)
	go func() { parked <- e.Trigger(context.Background(), "b") }()
	time.Sleep(50 * time.Millisecond)

	done := make(chan struct{})
	go func() { _ = e.Shutdown(context.Background()); close(done) }()
	time.Sleep(50 * time.Millisecond)

	if err := e.Trigger(context.Background(), "b"); !errors.Is(err, ErrEngineNotRunning) {
		t.Fatalf("new trigger err=%v want ErrEngineNotRunning", err)
	}
	select {
	case err := <-parked:
		if !errors.Is(err, ErrEngineNotRunning) {
			t.Fatalf("parked trigger released with err=%v want ErrEngineNotRunning", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("parked trigger never released during drain")
	}

	close(gateA) // let a finish so the drain completes
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("drain never completed")
	}
	if n := exec.callCount("b"); n != 0 {
		t.Fatalf("b dispatched %d times during drain", n)
	}
}
