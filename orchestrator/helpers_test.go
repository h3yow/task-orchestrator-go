package orchestrator

import (
	"context"
	"sync"
)

// countingExecutor returns per-task scripted results and records how many
// times each task actually ran, so tests can assert "at most one effect".
type countingExecutor struct {
	mu      sync.Mutex
	calls   map[string]int
	results map[string]Result
	// onCall, if set, is invoked while holding no lock before returning the
	// result (tests use it to block or observe scheduling).
	onCall func(taskID string, attempt int)
}

func (c *countingExecutor) Execute(ctx context.Context, taskID string) Result {
	c.mu.Lock()
	if c.calls == nil {
		c.calls = map[string]int{}
	}
	c.calls[taskID]++
	n := c.calls[taskID]
	r := c.results[taskID]
	hook := c.onCall
	c.mu.Unlock()
	if hook != nil {
		hook(taskID, n)
	}
	return r
}

func (c *countingExecutor) callCount(taskID string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls[taskID]
}

// scriptedExecutor returns a different result per attempt number (1-based).
type scriptedExecutor struct {
	mu      sync.Mutex
	calls   map[string]int
	scripts map[string][]Result
	block   map[string]chan struct{} // optional: block until closed, per task
}

func newScriptedExecutor() *scriptedExecutor {
	return &scriptedExecutor{calls: map[string]int{}, scripts: map[string][]Result{}, block: map[string]chan struct{}{}}
}

func (s *scriptedExecutor) set(taskID string, results ...Result) *scriptedExecutor {
	s.scripts[taskID] = results
	return s
}

func (s *scriptedExecutor) blockOn(taskID string) chan struct{} {
	ch := make(chan struct{})
	s.block[taskID] = ch
	return ch
}

func (s *scriptedExecutor) Execute(ctx context.Context, taskID string) Result {
	s.mu.Lock()
	s.calls[taskID]++
	n := s.calls[taskID]
	script := s.scripts[taskID]
	wait := s.block[taskID]
	s.mu.Unlock()

	if wait != nil {
		select {
		case <-wait:
		case <-ctx.Done():
			return Result{OK: false, Retryable: true, Reason: "cancelled while blocked"}
		}
	}
	if len(script) == 0 {
		return Result{OK: true}
	}
	if n-1 < len(script) {
		return script[n-1]
	}
	return script[len(script)-1]
}

func (s *scriptedExecutor) callCount(taskID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[taskID]
}

// alwaysBlockExecutor blocks inside Execute until ctx is cancelled (used to
// deterministically trigger the engine-side attempt timeout).
type alwaysBlockExecutor struct {
	mu    sync.Mutex
	calls int
}

func (x *alwaysBlockExecutor) Execute(ctx context.Context, taskID string) Result {
	x.mu.Lock()
	x.calls++
	x.mu.Unlock()
	<-ctx.Done()
	return Result{OK: false, Retryable: true, Reason: "blocked until cancel"}
}

func (x *alwaysBlockExecutor) count() int {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.calls
}
