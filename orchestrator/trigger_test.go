package orchestrator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestTriggerTaskRejectsDuplicateAndConcurrentTrigger(t *testing.T) {
	env := newTestEnv(t)
	task := newBlockingTask()
	defs := []TaskDef{{ID: "a", Run: task.run}}
	plan, _ := BuildPlan("run-trigger", defs)
	eng, err := NewEngine(plan, env.opts())
	if err != nil {
		t.Fatal(err)
	}

	// First manual trigger starts the attempt.
	if err := eng.TriggerTask(context.Background(), "a"); err != nil {
		t.Fatalf("first trigger: %v", err)
	}
	waitStarted(t, task, 1)

	// Concurrent duplicate trigger must be rejected, not run a second time.
	err = eng.TriggerTask(context.Background(), "a")
	if !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("want ErrAlreadyRunning, got %v", err)
	}
	t.Logf("concurrent trigger rejected distinctly: %v", err)

	close(task.release)
	if err := eng.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Triggering a succeeded task is rejected too.
	err = eng.TriggerTask(context.Background(), "a")
	if !errors.Is(err, ErrAlreadySucceeded) {
		t.Fatalf("want ErrAlreadySucceeded, got %v", err)
	}
	if got := taskAttemptCount(task); got != 1 {
		t.Fatalf("task function must take effect exactly once, invocations=%d", got)
	}
	t.Logf("post-success trigger rejected: %v; invocations=1", err)
}

func TestTriggerNotReadyUntilDepsSucceed(t *testing.T) {
	env := newTestEnv(t)
	blocker := newBlockingTask()
	defs := []TaskDef{
		{ID: "a", Run: blocker.run},
		{ID: "b", Deps: []string{"a"}, Run: noopRun},
	}
	plan, _ := BuildPlan("run-trigger-notready", defs)
	eng, err := NewEngine(plan, env.opts())
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.TriggerTask(context.Background(), "b"); !errors.Is(err, ErrNotReady) {
		t.Fatalf("want ErrNotReady before deps done, got %v", err)
	}
	if err := eng.TriggerTask(context.Background(), "missing"); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("want ErrTaskNotFound, got %v", err)
	}
	t.Logf("not-ready and missing triggers rejected with distinct causes")
}

func TestConcurrentSchedulingLaunchesEachTaskOnce(t *testing.T) {
	env := newTestEnv(t)
	// Fan-out: one parent unlocks many children simultaneously; each child
	// must observe exactly one effective execution even though scheduling
	// notifications may race.
	const n = 10
	var mu sync.Mutex
	counts := make(map[string]int)
	defs := []TaskDef{{ID: "root", Run: noopRun}}
	for i := 0; i < n; i++ {
		id := string(rune('a' + i))
		defs = append(defs, TaskDef{
			ID:   id,
			Deps: []string{"root"},
			Run: func(ctx context.Context) error {
				mu.Lock()
				counts[id]++
				mu.Unlock()
				return nil
			},
		})
	}
	plan, _ := BuildPlan("run-fanout", defs)
	eng, err := NewEngine(plan, env.opts())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := eng.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	for i := 0; i < n; i++ {
		id := string(rune('a' + i))
		if counts[id] != 1 {
			t.Fatalf("task %s effective executions=%d, want exactly 1", id, counts[id])
		}
	}
	t.Logf("all %d parallel dependents executed exactly once", n)
}

func taskAttemptCount(b *blockingTask) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.count
}
