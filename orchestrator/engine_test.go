package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// testEnv bundles the pieces most tests need.
type testEnv struct {
	store *MemoryStore
	clock *FakeClockHandle
	log   Logger
}

func newTestEnv(t *testing.T) testEnv {
	t.Helper()
	return testEnv{
		store: NewMemoryStore(),
		clock: NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)),
		log:   newTestLogger(t),
	}
}

func (e testEnv) opts() Options {
	return Options{Store: e.store, Clock: e.clock.Clock(), Logger: e.log}
}

// blockingTask can hold an attempt in flight until released.
type blockingTask struct {
	mu        sync.Mutex
	started   chan struct{}
	release   chan struct{}
	count     int
	fail      int // fail on attempts <= fail
	permanent bool
}

func newBlockingTask() *blockingTask {
	return &blockingTask{started: make(chan struct{}, 8), release: make(chan struct{})}
}

func (b *blockingTask) run(ctx context.Context) error {
	b.mu.Lock()
	b.count++
	n := b.count
	b.mu.Unlock()
	b.started <- struct{}{}
	select {
	case <-b.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	if n <= b.fail {
		if b.permanent {
			return NonRetryable(fmt.Errorf("boom on attempt %d", n))
		}
		return fmt.Errorf("transient boom on attempt %d", n)
	}
	return nil
}

func waitStarted(t *testing.T, b *blockingTask, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-b.started:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for attempt start %d/%d", i+1, n)
		}
	}
}

func TestRunDAGHappyPath(t *testing.T) {
	env := newTestEnv(t)
	var orderMu sync.Mutex
	var order []string
	ran := func(id string) TaskFunc {
		return func(ctx context.Context) error {
			orderMu.Lock()
			order = append(order, id)
			orderMu.Unlock()
			return nil
		}
	}
	defs := []TaskDef{
		{ID: "a", Run: ran("a")},
		{ID: "b", Deps: []string{"a"}, Run: ran("b")},
		{ID: "c", Deps: []string{"a"}, Run: ran("c")},
		{ID: "d", Deps: []string{"b", "c"}, Run: ran("d")},
	}
	plan, err := BuildPlan("run-happy", defs)
	if err != nil {
		t.Fatal(err)
	}
	eng, err := NewEngine(plan, env.opts())
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Run(context.Background()); err != nil {
		t.Fatalf("Run error: %v", err)
	}
	for _, id := range plan.IDs() {
		st, _ := eng.State(id)
		if st.Status != StatusSucceeded {
			t.Fatalf("task %s status %s", id, st.Status)
		}
	}
	pos := map[string]int{}
	for i, id := range order {
		pos[id] = i
	}
	if !(pos["a"] < pos["b"] && pos["b"] < pos["d"] && pos["c"] < pos["d"]) {
		t.Fatalf("dependency order violated: %v", order)
	}
	t.Logf("completed in a dependency-respecting order: %v", order)
}

func TestIndependentTasksRunConcurrently(t *testing.T) {
	env := newTestEnv(t)
	var mu sync.Mutex
	cur := 0
	max := 0
	mk := func() TaskFunc {
		return func(ctx context.Context) error {
			mu.Lock()
			cur++
			if cur > max {
				max = cur
			}
			mu.Unlock()
			time.Sleep(20 * time.Millisecond) // only for overlap, not for scheduling
			mu.Lock()
			cur--
			mu.Unlock()
			return nil
		}
	}
	defs := []TaskDef{{ID: "a", Run: mk()}, {ID: "b", Run: mk()}, {ID: "c", Run: mk()}}
	plan, _ := BuildPlan("run-concurrent", defs)
	eng, err := NewEngine(plan, env.opts())
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if max < 2 {
		t.Fatalf("expected independent tasks to overlap, max concurrency was %d", max)
	}
	t.Logf("observed peak concurrency %d for 3 independent tasks", max)
}

func TestRetryThenSuccessRecordsEveryAttempt(t *testing.T) {
	env := newTestEnv(t)
	task := newBlockingTask()
	task.fail = 2 // fail attempt 1 and 2, succeed on 3
	defs := []TaskDef{{
		ID:    "a",
		Run:   task.run,
		Retry: RetryPolicy{MaxAttempts: 3, Backoff: 10 * time.Millisecond},
	}}
	plan, _ := BuildPlan("run-retry", defs)
	eng, err := NewEngine(plan, env.opts())
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- eng.Run(context.Background()) }()

	waitStarted(t, task, 1) // attempt 1 in flight
	close(task.release)     // release every attempt (close is sticky)

	// Attempts 1 and 2 fail transiently. Each retry waits out the 10ms
	// backoff on the fake clock; advancing time fires the backoff timer.
	if !env.clock.WaitPending(1, time.Second) {
		t.Fatal("backoff timer never armed after attempt 1")
	}
	env.clock.Advance(10 * time.Millisecond) // fires backoff -> attempt 2
	waitStarted(t, task, 1)                  // attempt 2 in flight and fails
	if task.fail > 1 {
		if !env.clock.WaitPending(1, time.Second) {
			t.Fatal("backoff timer never armed after attempt 2")
		}
		env.clock.Advance(10 * time.Millisecond) // -> attempt 3
		waitStarted(t, task, 1)                  // attempt 3 succeeds
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected final success, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not finish")
	}

	st, _ := eng.State("a")
	if st.Status != StatusSucceeded {
		t.Fatalf("status=%s", st.Status)
	}
	if st.Attempt != 3 {
		t.Fatalf("attempt=%d, want 3", st.Attempt)
	}
	if len(st.Records) != 3 {
		t.Fatalf("records=%d, want 3", len(st.Records))
	}
	for i, r := range st.Records[:2] {
		if r.Err == "" {
			t.Fatalf("record %d should capture the failure cause", i+1)
		}
	}
	if st.Records[2].Err != "" {
		t.Fatalf("final record must be the successful attempt, got %q", st.Records[2].Err)
	}
	t.Logf("retry sequence explained: %d attempts; reasons: record1=%q record2=%q final=success",
		len(st.Records), st.Records[0].Err, st.Records[1].Err)
}

func TestNonRetryableErrorFailsImmediately(t *testing.T) {
	env := newTestEnv(t)
	task := newBlockingTask()
	task.fail = 10
	task.permanent = true
	defs := []TaskDef{{
		ID:    "a",
		Run:   task.run,
		Retry: RetryPolicy{MaxAttempts: 5, Backoff: 0},
	}}
	plan, _ := BuildPlan("run-permanent", defs)
	eng, err := NewEngine(plan, env.opts())
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- eng.Run(context.Background()) }()
	waitStarted(t, task, 1)
	close(task.release)

	var runErr *RunError
	select {
	case err := <-done:
		if !errors.As(err, &runErr) {
			t.Fatalf("want *RunError, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not finish")
	}
	st, _ := eng.State("a")
	if st.Status != StatusFailed || st.LastReason != ReasonNonRetryable {
		t.Fatalf("status=%s reason=%s", st.Status, st.LastReason)
	}
	if st.Attempt != 1 {
		t.Fatalf("non-retryable failure must not be retried, attempts=%d", st.Attempt)
	}
	t.Logf("permanent error distinguished: status=%s reason=%s err=%q",
		st.Status, st.LastReason, st.Records[0].Err)
}

func TestRetriesExhaustedFailsDeterministically(t *testing.T) {
	env := newTestEnv(t)
	task := newBlockingTask()
	task.fail = 10 // always fails transiently
	defs := []TaskDef{{
		ID:    "a",
		Run:   task.run,
		Retry: RetryPolicy{MaxAttempts: 2, Backoff: 0},
	}}
	plan, _ := BuildPlan("run-exhaust", defs)
	eng, err := NewEngine(plan, env.opts())
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- eng.Run(context.Background()) }()
	waitStarted(t, task, 1)
	close(task.release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not finish")
	}
	st, _ := eng.State("a")
	if st.Status != StatusFailed || st.LastReason != ReasonRetriesExhausted {
		t.Fatalf("status=%s reason=%s", st.Status, st.LastReason)
	}
	if st.Attempt != 2 {
		t.Fatalf("attempts=%d want 2", st.Attempt)
	}
	t.Logf("exhaustion terminal: reason=%s attempts=%d", st.LastReason, st.Attempt)
}

func TestFailedTaskSkipsDependents(t *testing.T) {
	env := newTestEnv(t)
	task := newBlockingTask()
	task.fail = 10
	task.permanent = true
	defs := []TaskDef{
		{ID: "a", Run: task.run},
		{ID: "b", Deps: []string{"a"}, Run: noopRun},
		{ID: "c", Deps: []string{"b"}, Run: noopRun},
		{ID: "d", Run: noopRun}, // independent: must still succeed
	}
	plan, _ := BuildPlan("run-skip", defs)
	eng, err := NewEngine(plan, env.opts())
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- eng.Run(context.Background()) }()
	waitStarted(t, task, 1)
	close(task.release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not finish")
	}
	for _, id := range []string{"b", "c"} {
		st, _ := eng.State(id)
		if st.Status != StatusSkipped || st.LastReason != ReasonDependencyFailed {
			t.Fatalf("task %s: status=%s reason=%s", id, st.Status, st.LastReason)
		}
		if st.Attempted() {
			t.Fatalf("skipped task %s must never have executed", id)
		}
	}
	if st, _ := eng.State("d"); st.Status != StatusSucceeded {
		t.Fatalf("independent task d must succeed, got %s", st.Status)
	}
	t.Logf("failure propagated: a=failed, b=skipped, c=skipped, d=succeeded")
}

func TestTimeoutControlledByFakeClock(t *testing.T) {
	env := newTestEnv(t)
	task := newBlockingTask()
	defs := []TaskDef{{
		ID:      "a",
		Run:     task.run,
		Timeout: 5 * time.Second,
		Retry:   RetryPolicy{MaxAttempts: 1},
	}}
	plan, _ := BuildPlan("run-timeout", defs)
	eng, err := NewEngine(plan, env.opts())
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- eng.Run(context.Background()) }()
	waitStarted(t, task, 1)
	// No real sleeping: move fake time past the deadline.
	env.clock.Advance(5 * time.Second)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not finish after fake timeout")
	}
	st, _ := eng.State("a")
	if st.Status != StatusFailed || st.LastReason != ReasonTimeout {
		t.Fatalf("status=%s reason=%s", st.Status, st.LastReason)
	}
	if !st.Records[0].Timeout {
		t.Fatal("attempt record must be marked as timeout")
	}
	t.Logf("timeout settled deterministically at fake t=%v, reason=%s",
		env.clock.Now(), st.LastReason)
}

func TestTimeoutRetryableThenSucceed(t *testing.T) {
	env := newTestEnv(t)
	task := newBlockingTask()
	defs := []TaskDef{{
		ID:      "a",
		Run:     task.run,
		Timeout: 5 * time.Second,
		Retry:   RetryPolicy{MaxAttempts: 2, Backoff: 0, TimeoutIsRetryable: true},
	}}
	plan, _ := BuildPlan("run-timeout-retry", defs)
	eng, err := NewEngine(plan, env.opts())
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- eng.Run(context.Background()) }()
	waitStarted(t, task, 1)
	env.clock.Advance(5 * time.Second) // attempt 1 times out
	waitStarted(t, task, 1)            // attempt 2 starts
	close(task.release)                // attempt 2 succeeds
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not finish")
	}
	st, _ := eng.State("a")
	if st.Status != StatusSucceeded || st.Attempt != 2 {
		t.Fatalf("status=%s attempts=%d", st.Status, st.Attempt)
	}
	if st.Records[0].Reason != ReasonTimeout {
		t.Fatalf("first record reason=%s", st.Records[0].Reason)
	}
	t.Logf("retryable timeout recovered: record1=timeout, attempt2=succeeded")
}
