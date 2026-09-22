package orchestrator

import (
	"context"
	"sync"
	"testing"
	"time"
)

// gateExecutor blocks each task until release, recording running tasks so the
// test can assert independent tasks overlap and dependent ones never do.
type gateExecutor struct {
	mu      sync.Mutex
	running map[string]bool
	started map[string]chan struct{}
	release map[string]chan struct{}
	results map[string]Result
	order   []string
}

func newGateExecutor(ids ...string) *gateExecutor {
	g := &gateExecutor{
		running: map[string]bool{},
		started: map[string]chan struct{}{},
		release: map[string]chan struct{}{},
		results: map[string]Result{},
	}
	for _, id := range ids {
		g.started[id] = make(chan struct{})
		g.release[id] = make(chan struct{})
		g.results[id] = Result{OK: true}
	}
	return g
}

func (g *gateExecutor) Execute(ctx context.Context, id string) Result {
	g.mu.Lock()
	g.running[id] = true
	g.order = append(g.order, id)
	startCh := g.started[id]
	rel := g.release[id]
	r := g.results[id]
	g.mu.Unlock()
	close(startCh)
	select {
	case <-rel:
	case <-ctx.Done():
	}
	g.mu.Lock()
	g.running[id] = false
	g.mu.Unlock()
	return r
}

func (g *gateExecutor) waitStarted(t *testing.T, id string) {
	t.Helper()
	select {
	case <-g.started[id]:
	case <-time.After(2 * time.Second):
		t.Fatalf("task %s never started", id)
	}
}

func (g *gateExecutor) releaseTask(id string) { close(g.release[id]) }

func (g *gateExecutor) isRunning(id string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.running[id]
}

// TestIndependentTasksRunConcurrently: b and c both depend only on a; they
// must overlap in execution; d runs only after both finish.
func TestIndependentTasksRunConcurrently(t *testing.T) {
	g := newGateExecutor("a", "b", "c", "d")
	defs := []TaskDef{
		{ID: "a"},
		{ID: "b", Deps: []string{"a"}},
		{ID: "c", Deps: []string{"a"}},
		{ID: "d", Deps: []string{"b", "c"}},
	}
	e, err := New(defs, Options{Store: NewMemoryStore(), Executor: g, Clock: NewFakeClock(time.Unix(0, 0)), Logger: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = e.Run(context.Background()) }()

	g.waitStarted(t, "a")
	if g.isRunning("b") || g.isRunning("c") {
		t.Fatal("b/c started before a finished")
	}
	g.releaseTask("a")

	g.waitStarted(t, "b")
	g.waitStarted(t, "c")
	// b and c must overlap.
	if !g.isRunning("b") || !g.isRunning("c") {
		t.Fatal("b and c did not run concurrently")
	}
	stD, _ := e.State("d")
	if stD.Status != StatusPending {
		t.Fatalf("d started early: %s", stD.Status)
	}
	g.releaseTask("b")
	g.releaseTask("c")

	g.waitStarted(t, "d")
	g.releaseTask("d")
	pollTerminal(t, e, 2*time.Second)
	for _, id := range []string{"a", "b", "c", "d"} {
		st, _ := e.State(id)
		if st.Status != StatusSucceeded {
			t.Fatalf("%s=%s", id, st.Status)
		}
	}
}

// TestUpstreamFailurePropagates: failing a dependency deterministically fails
// all dependents with a distinguishable deps_failed attempt class, and tasks
// that never ran must not consume executor attempts.
func TestUpstreamFailurePropagates(t *testing.T) {
	exec := &countingExecutor{results: map[string]Result{
		"a": {OK: false, Retryable: false, Reason: "hard fail"},
		"b": {OK: true},
		"c": {OK: true},
		"d": {OK: true},
	}}
	defs := []TaskDef{
		{ID: "a", Retry: RetryPolicy{MaxAttempts: 1}},
		{ID: "b", Deps: []string{"a"}},
		{ID: "c", Deps: []string{"b"}},
		{ID: "d"}, // independent must still succeed
	}
	log := newCaptureLogger(t)
	e, err := New(defs, Options{Store: NewMemoryStore(), Executor: exec, Clock: NewFakeClock(time.Unix(0, 0)), Logger: log})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"b", "c"} {
		st, _ := e.State(id)
		if st.Status != StatusFailed {
			t.Fatalf("%s=%s want failed", id, st.Status)
		}
		if st.Attempts[0].Class != AttemptSkippedDeps {
			t.Fatalf("%s attempt class=%s want deps_failed", id, st.Attempts[0].Class)
		}
		if exec.callCount(id) != 0 {
			t.Fatalf("%s executed %d times despite upstream failure", id, exec.callCount(id))
		}
	}
	stD, _ := e.State("d")
	if stD.Status != StatusSucceeded {
		t.Fatalf("independent task d=%s", stD.Status)
	}
	if !log.contains("upstream failure") {
		t.Fatal("upstream failure decision not logged")
	}
}

// TestMaxConcurrentRespected verifies the concurrency bound with independent
// tasks; it relies only on goroutine scheduling (no semantic wall-clock wait).
func TestMaxConcurrentRespected(t *testing.T) {
	g := newGateExecutor("t1", "t2", "t3", "t4")
	defs := []TaskDef{
		{ID: "t1"}, {ID: "t2"}, {ID: "t3"}, {ID: "t4"},
	}
	e, err := New(defs, Options{
		Store: NewMemoryStore(), Executor: g,
		Clock: NewFakeClock(time.Unix(0, 0)), MaxConcurrent: 2, Logger: testLogger(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = e.Run(context.Background()) }()

	g.waitStarted(t, "t1")
	g.waitStarted(t, "t2")
	// Give the scheduler a moment; t3 must not be running.
	time.Sleep(20 * time.Millisecond)
	if g.isRunning("t3") || g.isRunning("t4") {
		t.Fatal("concurrency limit exceeded")
	}
	g.releaseTask("t1")
	g.waitStarted(t, "t3")
	if g.isRunning("t4") {
		t.Fatal("t4 started while 2 slots still occupied")
	}
	g.releaseTask("t2")
	g.releaseTask("t3")
	g.waitStarted(t, "t4")
	g.releaseTask("t4")
	pollTerminal(t, e, 2*time.Second)
}
