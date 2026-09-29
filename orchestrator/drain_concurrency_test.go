package orchestrator

import (
	"context"
	"sync"
	"testing"
	"time"
)

// gateTask holds one dispatched attempt behind a gate while honoring context
// cancellation. Its started channel closes the first time the attempt begins.
type gateTask struct {
	startOnce sync.Once
	started   chan struct{}
	release   chan struct{}
}

// multiGateExecutor holds every dispatched task behind its own gate channel while
// honoring context cancellation, so tests can release several in-flight
// attempts "at the same instant" and control their ordering at the
// worker->scheduler boundary.
type multiGateExecutor struct {
	mu      sync.Mutex
	tasks   map[string]*gateTask
	results map[string]Result
	calls   map[string]int
}

func newMultiGateExecutor(ids []string) *multiGateExecutor {
	g := &multiGateExecutor{
		tasks:   map[string]*gateTask{},
		results: map[string]Result{},
		calls:   map[string]int{},
	}
	for _, id := range ids {
		g.tasks[id] = &gateTask{started: make(chan struct{}), release: make(chan struct{})}
		g.results[id] = Result{OK: true, Reason: "done"}
	}
	return g
}

func (g *multiGateExecutor) Execute(ctx context.Context, id string) Result {
	g.mu.Lock()
	g.calls[id]++
	task := g.tasks[id]
	res := g.results[id]
	g.mu.Unlock()

	task.startOnce.Do(func() { close(task.started) })
	select {
	case <-task.release:
		return res
	case <-ctx.Done():
		return Result{OK: false, Retryable: true, Reason: "cancelled"}
	}
}

// releaseOnce opens the gate at most once.
func (g *multiGateExecutor) releaseOnce(id string) {
	g.mu.Lock()
	task := g.tasks[id]
	g.mu.Unlock()
	select {
	case <-task.release:
	default:
		close(task.release)
	}
}

func (g *multiGateExecutor) waitStarted(t *testing.T, id string) {
	t.Helper()
	g.mu.Lock()
	task := g.tasks[id]
	g.mu.Unlock()
	select {
	case <-task.started:
	case <-time.After(2 * time.Second):
		t.Fatalf("task %q never started", id)
	}
}

func (g *multiGateExecutor) callCount(id string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.calls[id]
}

// waitStatus polls until the task reaches want or time runs out.
func waitStatus(e *Engine, id string, want Status, d time.Duration) (TaskState, bool) {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		st, _ := e.State(id)
		if st.Status == want {
			return st, true
		}
		time.Sleep(time.Millisecond)
	}
	st, _ := e.State(id)
	return st, false
}

// assertDrainInvariants asserts the post-drain invariants: no task is stranded
// in "running", every task sits in a well-defined home state, and the drain
// statistics match the snapshot exactly.
func assertDrainInvariants(t *testing.T, e *Engine, ids []string) {
	t.Helper()
	counts := map[Status]int{}
	for _, id := range ids {
		st, err := e.State(id)
		if err != nil {
			t.Fatalf("State(%q): %v", id, err)
		}
		counts[st.Status]++
		if st.Status == StatusRunning {
			t.Fatalf("task %q still running after drain returned", id)
		}
		switch st.Status {
		case StatusSucceeded, StatusFailed, StatusPending, StatusRetryWait:
		default:
			t.Fatalf("task %q in unexpected status %q", id, st.Status)
		}
	}
	stats := e.DrainStats()
	if stats.InFlight != 0 {
		t.Fatalf("DrainStats in-flight=%d, want 0 (snapshot counts=%v)", stats.InFlight, counts)
	}
	if stats.Running != counts[StatusRunning] {
		t.Fatalf("DrainStats running=%d but snapshot has %d running", stats.Running, counts[StatusRunning])
	}
	want := DrainStats{
		Succeeded: counts[StatusSucceeded],
		Failed:    counts[StatusFailed],
		Pending:   counts[StatusPending],
		RetryWait: counts[StatusRetryWait],
		Running:   counts[StatusRunning],
	}
	if stats.Succeeded != want.Succeeded || stats.Failed != want.Failed ||
		stats.Pending != want.Pending || stats.RetryWait != want.RetryWait {
		t.Fatalf("DrainStats %+v do not match snapshot counts %v", stats, counts)
	}
}

// TestDrainConcurrentTasksAllFinishNaturally: several in-flight tasks finish at
// almost the same moment around shutdown. The drain must not complete while any
// finished attempt's result is still pending reconciliation: after Shutdown
// returns every task must have a persisted terminal/waiting state, never
// "running". A deterministic hook parks one worker AFTER releasing its slot but
// BEFORE delivering its result — the exact window in which the old scheduler
// falsely concluded the drain was over.
func TestDrainConcurrentTasksAllFinishNaturally(t *testing.T) {
	ids := []string{"a", "b", "c"}
	store := NewMemoryStore()
	defs := []TaskDef{
		{ID: "a", Retry: RetryPolicy{MaxAttempts: 2}},
		{ID: "b", Retry: RetryPolicy{MaxAttempts: 2}},
		{ID: "c", Retry: RetryPolicy{MaxAttempts: 2}},
	}
	exec := newMultiGateExecutor(ids)
	log := newCaptureLogger(t)
	e, err := New(defs, Options{
		Store: store, Executor: exec,
		Clock: NewFakeClock(time.Unix(0, 0)), MaxConcurrent: 3, Logger: log,
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = e.Run(context.Background()) }()
	for _, id := range ids {
		exec.waitStarted(t, id)
	}

	// Exactly one worker parks in the dangerous window; the others race through.
	var parkedMu sync.Mutex
	parked := make(chan struct{})
	releaseParked := make(chan struct{})
	var parkedID string
	var parkedUsed int32
	e.testHookBeforeDeliver = func(id string, seq int) {
		parkedMu.Lock()
		first := parkedUsed == 0
		if first {
			parkedUsed = 1
			parkedID = id
		}
		parkedMu.Unlock()
		if !first {
			return
		}
		close(parked)
		<-releaseParked
	}

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- e.Shutdown(context.Background()) }()

	// Release all attempts almost simultaneously while one result is stuck
	// unreconciled in the worker.
	for _, id := range ids {
		exec.releaseOnce(id)
	}
	select {
	case <-parked:
	case <-time.After(2 * time.Second):
		t.Fatal("worker never reached the pre-delivery park point")
	}

	// The parked task must still visibly be "running", and Shutdown must not
	// return early even though its worker has already released the slot.
	if st, _ := e.State(parkedID); st.Status != StatusRunning {
		t.Fatalf("parked task %q status=%s, must still be running while its result is unreconciled",
			parkedID, st.Status)
	}
	select {
	case err := <-shutdownDone:
		t.Fatalf("Shutdown returned early with err=%v while task %q result is not reconciled", err, parkedID)
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseParked)
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not return after all attempts reconciled")
	}

	assertDrainInvariants(t, e, ids)
	for _, id := range ids {
		st, _ := e.State(id)
		if st.Status != StatusSucceeded || len(st.Attempts) != 1 ||
			st.Attempts[0].Class != AttemptSucceeded {
			t.Fatalf("task %q state=%+v, want a single committed success", id, st)
		}
	}
	if !log.contains("drain") {
		t.Fatal("drain decisions not logged")
	}

	// Restart: nothing re-executes, everything stays succeeded.
	exec2 := newScriptedExecutor().
		set("a", Result{OK: true}).set("b", Result{OK: true}).set("c", Result{OK: true})
	e2, err := New(defs, Options{
		Store: store, Executor: exec2,
		Clock: NewFakeClock(time.Unix(1000, 0)), Logger: testLogger(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := e2.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if exec2.callCount(id) != 0 {
			t.Fatalf("succeeded task %q re-executed %d times on restart", id, exec2.callCount(id))
		}
	}
}

// TestDrainConcurrentTasksForceCancelled: several tasks still in flight when
// the grace period expires. The drain ends only after every forced attempt has
// been closed as interrupted and persisted; restart replays each exactly once.
func TestDrainConcurrentTasksForceCancelled(t *testing.T) {
	ids := []string{"job1", "job2", "job3"}
	store := NewMemoryStore()
	defs := []TaskDef{
		{ID: "job1", Retry: RetryPolicy{MaxAttempts: 3}},
		{ID: "job2", Retry: RetryPolicy{MaxAttempts: 3}},
		{ID: "job3", Retry: RetryPolicy{MaxAttempts: 1}}, // last budget: must land failed
	}
	exec := newMultiGateExecutor(ids)
	log := newCaptureLogger(t)
	e, err := New(defs, Options{
		Store: store, Executor: exec,
		Clock: NewFakeClock(time.Unix(0, 0)), MaxConcurrent: 3, Logger: log,
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = e.Run(context.Background()) }()
	for _, id := range ids {
		exec.waitStarted(t, id)
	}

	sctx, scancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer scancel()
	if err := e.Shutdown(sctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	assertDrainInvariants(t, e, ids)
	for _, id := range ids {
		st, _ := e.State(id)
		last := st.Attempts[len(st.Attempts)-1]
		if last.Class != AttemptInterrupted {
			t.Fatalf("task %q last attempt class=%s, want interrupted", id, last.Class)
		}
	}
	if st, _ := e.State("job1"); st.Status != StatusRetryWait {
		t.Fatalf("job1=%s want retry_wait", st.Status)
	}
	if st, _ := e.State("job2"); st.Status != StatusRetryWait {
		t.Fatalf("job2=%s want retry_wait", st.Status)
	}
	if st, _ := e.State("job3"); st.Status != StatusFailed {
		t.Fatalf("job3=%s want terminal failed (budget exhausted)", st.Status)
	}
	if !log.contains("forced") {
		t.Fatal("forced-drain decisions not logged")
	}

	// Restart replays each interrupted task exactly once; job3 never runs again.
	exec2 := newScriptedExecutor().
		set("job1", Result{OK: true}).set("job2", Result{OK: true}).set("job3", Result{OK: true})
	e2, err := New(defs, Options{
		Store: store, Executor: exec2,
		Clock: NewFakeClock(time.Unix(100, 0)), Logger: testLogger(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := e2.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if exec2.callCount("job1") != 1 || exec2.callCount("job2") != 1 {
		t.Fatalf("replay counts job1=%d job2=%d, want 1/1", exec2.callCount("job1"), exec2.callCount("job2"))
	}
	if exec2.callCount("job3") != 0 {
		t.Fatalf("terminally failed job3 re-executed %d times", exec2.callCount("job3"))
	}
}

// TestDrainMixedNaturalAndForcedFinish: with multiple concurrent in-flight
// tasks, some release naturally inside the grace period while others survive to
// the deadline and are force-cancelled. Both reconciliation kinds must be fully
// persisted before Shutdown returns, and restart must neither re-run committed
// successes nor lose the interrupted tasks.
func TestDrainMixedNaturalAndForcedFinish(t *testing.T) {
	ids := []string{"fast", "slow1", "slow2"}
	store := NewMemoryStore()
	defs := []TaskDef{
		{ID: "fast", Retry: RetryPolicy{MaxAttempts: 2}},
		{ID: "slow1", Retry: RetryPolicy{MaxAttempts: 2}},
		{ID: "slow2", Retry: RetryPolicy{MaxAttempts: 2}},
	}
	exec := newMultiGateExecutor(ids)
	e, err := New(defs, Options{
		Store: store, Executor: exec,
		Clock: NewFakeClock(time.Unix(0, 0)), MaxConcurrent: 3, Logger: testLogger(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = e.Run(context.Background()) }()
	for _, id := range ids {
		exec.waitStarted(t, id)
	}

	shutdownDone := make(chan error, 1)
	go func() {
		sctx, scancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer scancel()
		shutdownDone <- e.Shutdown(sctx)
	}()

	// One task finishes naturally during the grace period; the rest stay blocked.
	exec.releaseOnce("fast")
	if st, ok := waitStatus(e, "fast", StatusSucceeded, 2*time.Second); !ok {
		t.Fatalf("fast=%+v, want natural success committed during drain", st)
	}

	if err := <-shutdownDone; err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	assertDrainInvariants(t, e, ids)
	for _, id := range []string{"slow1", "slow2"} {
		st, _ := e.State(id)
		if st.Status != StatusRetryWait || st.Attempts[len(st.Attempts)-1].Class != AttemptInterrupted {
			t.Fatalf("%s=%+v, want retry_wait ending in interrupted attempt", id, st)
		}
	}

	exec2 := newScriptedExecutor().
		set("fast", Result{OK: true}).set("slow1", Result{OK: true}).set("slow2", Result{OK: true})
	e2, err := New(defs, Options{
		Store: store, Executor: exec2,
		Clock: NewFakeClock(time.Unix(500, 0)), Logger: testLogger(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := e2.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if exec2.callCount("fast") != 0 {
		t.Fatalf("naturally succeeded fast re-ran %d times", exec2.callCount("fast"))
	}
	if exec2.callCount("slow1") != 1 || exec2.callCount("slow2") != 1 {
		t.Fatalf("replay counts slow1=%d slow2=%d, want 1/1",
			exec2.callCount("slow1"), exec2.callCount("slow2"))
	}
}

// TestDrainSingleInFlightNaturalAndForced covers the single in-flight task for
// both grace-period outcomes.
func TestDrainSingleInFlightNaturalAndForced(t *testing.T) {
	t.Run("natural", func(t *testing.T) {
		ids := []string{"only"}
		store := NewMemoryStore()
		defs := []TaskDef{{ID: "only", Retry: RetryPolicy{MaxAttempts: 2}}}
		exec := newMultiGateExecutor(ids)
		e, err := New(defs, Options{
			Store: store, Executor: exec,
			Clock: NewFakeClock(time.Unix(0, 0)), Logger: testLogger(t),
		})
		if err != nil {
			t.Fatal(err)
		}
		go func() { _ = e.Run(context.Background()) }()
		exec.waitStarted(t, "only")
		done := make(chan error, 1)
		go func() { done <- e.Shutdown(context.Background()) }()
		select {
		case err := <-done:
			t.Fatalf("drain finished while the only attempt is in flight: %v", err)
		case <-time.After(50 * time.Millisecond):
		}
		exec.releaseOnce("only")
		if err := <-done; err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
		assertDrainInvariants(t, e, ids)
		if st, _ := e.State("only"); st.Status != StatusSucceeded {
			t.Fatalf("only=%s want succeeded", st.Status)
		}
	})

	t.Run("forced", func(t *testing.T) {
		ids := []string{"only"}
		store := NewMemoryStore()
		defs := []TaskDef{{ID: "only", Retry: RetryPolicy{MaxAttempts: 2}}}
		exec := newMultiGateExecutor(ids)
		e, err := New(defs, Options{
			Store: store, Executor: exec,
			Clock: NewFakeClock(time.Unix(0, 0)), Logger: testLogger(t),
		})
		if err != nil {
			t.Fatal(err)
		}
		go func() { _ = e.Run(context.Background()) }()
		exec.waitStarted(t, "only")
		sctx, scancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer scancel()
		if err := e.Shutdown(sctx); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
		assertDrainInvariants(t, e, ids)
		if st, _ := e.State("only"); st.Status != StatusRetryWait ||
			st.Attempts[len(st.Attempts)-1].Class != AttemptInterrupted {
			t.Fatalf("state=%+v, want interrupted retry_wait", st)
		}
	})
}

// TestDrainCompletesImmediatelyWhenNothingInFlight: with zero attempts in
// flight (the only task is parked in retry_wait, another is pending behind it),
// Shutdown must complete immediately and leave the persisted waiting states
// untouched for restart.
func TestDrainCompletesImmediatelyWhenNothingInFlight(t *testing.T) {
	store := NewMemoryStore()
	defs := []TaskDef{
		{ID: "a", Retry: RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Hour}},
		{ID: "b", Deps: []string{"a"}, Retry: RetryPolicy{MaxAttempts: 1}},
	}
	exec := newScriptedExecutor().set("a",
		Result{OK: false, Retryable: true, Reason: "first failure"},
		Result{OK: true, Reason: "second succeeds"},
	).set("b", Result{OK: true})
	clock := NewFakeClock(time.Unix(0, 0))
	e, err := New(defs, Options{
		Store: store, Executor: exec, Clock: clock, Logger: testLogger(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = e.Run(context.Background()) }()

	stA := pollStatus(t, e, "a", StatusRetryWait, 2*time.Second)
	deadline := stA.NextRunAt
	if st, _ := e.State("b"); st.Status != StatusPending {
		t.Fatalf("b=%s, must remain pending behind a", st.Status)
	}
	if stats := e.DrainStats(); stats.InFlight != 0 {
		t.Fatalf("in-flight=%d, want 0 before shutdown", stats.InFlight)
	}

	// No task is running: an unbounded-grace Shutdown returns immediately.
	done := make(chan error, 1)
	go func() { done <- e.Shutdown(context.Background()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not complete immediately with nothing in flight")
	}
	assertDrainInvariants(t, e, []string{"a", "b"})

	// retry_wait keeps the ORIGINAL absolute deadline; b stays pending.
	if st, _ := e.State("a"); st.Status != StatusRetryWait || !st.NextRunAt.Equal(deadline) {
		t.Fatalf("a=%+v, want retry_wait with original NextRunAt %s", st, deadline)
	}
	if st, _ := e.State("b"); st.Status != StatusPending {
		t.Fatalf("b=%s, must remain pending", st.Status)
	}

	// Restart before the deadline, then advance past it: a retries once and
	// succeeds, b follows — no result lost, no double execution.
	e2, err := New(defs, Options{
		Store: store, Executor: exec,
		Clock: NewFakeClock(clock.Now()), Logger: testLogger(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	runDone := make(chan error, 1)
	go func() { runDone <- e2.Run(ctx2) }()
	e2.clock.(*FakeClock).Advance(time.Hour + time.Second)
	if err := e2.Wait(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	// Join the scheduler so its final decision log cannot land after the test.
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("restart Run: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("restart Run did not return after settling")
	}
	if exec.callCount("a") != 2 {
		t.Fatalf("a executed %d times, want 2", exec.callCount("a"))
	}
	if exec.callCount("b") != 1 {
		t.Fatalf("b executed %d times, want 1", exec.callCount("b"))
	}
}
