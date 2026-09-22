package orchestrator

import (
	"sort"
	"sync"
	"time"
)

// Clock abstracts time so retry/backoff and timeout logic never has to wait
// real wall-clock time in tests.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

// SystemClock uses wall-clock time and real timers.
type SystemClock struct{}

func NewSystemClock() Clock { return SystemClock{} }

func (SystemClock) Now() time.Time { return time.Now() }
func (SystemClock) After(d time.Duration) <-chan time.Time {
	if d <= 0 {
		ch := make(chan time.Time, 1)
		ch <- time.Now()
		return ch
	}
	return time.After(d)
}

// fakeTimer is one pending FakeClock timer.
type fakeTimer struct {
	fireAt time.Time
	ch     chan time.Time
	fired  bool
}

// FakeClock is a manually driven clock for deterministic tests: timers only
// fire when Advance is called. It is safe for concurrent use.
type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{now: start}
}

func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *FakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	if d <= 0 {
		ch <- c.now
		return ch
	}
	// Drop already-fired timers so they can't be mistaken for pending ones.
	kept := c.timers[:0]
	for _, t := range c.timers {
		if !t.fired {
			kept = append(kept, t)
		}
	}
	c.timers = kept
	t := &fakeTimer{fireAt: c.now.Add(d), ch: ch}
	c.timers = append(c.timers, t)
	return ch
}

// Advance moves the clock forward by d and fires every timer that becomes due,
// in chronological order. Timers registered with zero/negative delay are
// already delivered immediately by After.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	due := make([]*fakeTimer, 0)
	kept := make([]*fakeTimer, 0, len(c.timers))
	for _, t := range c.timers {
		if !t.fired && !t.fireAt.After(c.now) {
			due = append(due, t)
		} else {
			kept = append(kept, t)
		}
	}
	c.timers = kept
	c.mu.Unlock()

	// Fire outside the lock; channels are buffered so this never blocks.
	sort.Slice(due, func(i, j int) bool { return due[i].fireAt.Before(due[j].fireAt) })
	for _, t := range due {
		t.fired = true
		t.ch <- t.fireAt
	}
}

// PendingTimerCount reports timers not yet fired. Tests use it as a
// deterministic synchronization point ("the timeout timer is armed") without
// any real sleeping.
func (c *FakeClock) PendingTimerCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, t := range c.timers {
		if !t.fired {
			n++
		}
	}
	return n
}

// WaitForTimers blocks until at least n pending timers are registered.
func (c *FakeClock) WaitForTimers(n int) bool {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.PendingTimerCount() >= n {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}
