package orchestrator

import (
	"container/heap"
	"sync"
	"time"
)

// Clock abstracts time so tests never wait on real wall-clock time.
type Clock interface {
	Now() time.Time
	NewTimer(d time.Duration) Timer
}

// Timer is the abstract counterpart of time.Timer.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
	Reset(d time.Duration) bool
}

// ---------------------------------------------------------------- real clock

type realClock struct{}

// NewRealClock returns a Clock backed by wall-clock time.
func NewRealClock() Clock { return realClock{} }

type realTimer struct{ t *time.Timer }

func (realClock) Now() time.Time                   { return time.Now() }
func (c realClock) NewTimer(d time.Duration) Timer { return &realTimer{t: time.NewTimer(d)} }
func (t *realTimer) C() <-chan time.Time           { return t.t.C }
func (t *realTimer) Stop() bool                    { return t.t.Stop() }
func (t *realTimer) Reset(d time.Duration) bool    { return t.t.Reset(d) }

// ---------------------------------------------------------------- fake clock

// fakeClock is a manually controlled clock. Timers never fire on their own;
// they fire when the test advances the fake time past their deadline. This
// makes timeout/backoff behaviour fully deterministic with zero real waits.
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers timerHeap
	seq    int64
}

type fakeTimer struct {
	ch      chan time.Time
	clock   *fakeClock
	fireAt  time.Time
	seq     int64
	index   int // heap index, -1 when not scheduled
	stopped bool
}

func (t *fakeTimer) C() <-chan time.Time { return t.ch }

func (t *fakeTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	if t.index < 0 || t.stopped {
		t.stopped = true
		return false
	}
	heap.Remove(&t.clock.timers, t.index)
	t.index = -1
	t.stopped = true
	return true
}

func (t *fakeTimer) Reset(d time.Duration) bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	active := t.index >= 0 && !t.stopped
	if t.index >= 0 {
		heap.Remove(&t.clock.timers, t.index)
	}
	// Best-effort drain of a previously delivered value.
	select {
	case <-t.ch:
	default:
	}
	t.stopped = false
	t.fireAt = t.clock.now.Add(d)
	t.clock.seq++
	t.seq = t.clock.seq
	heap.Push(&t.clock.timers, t)
	return active
}

// NewFakeClock returns a controllable clock starting at start.
func NewFakeClock(start time.Time) *FakeClockHandle {
	fc := &fakeClock{now: start}
	fc.timers = timerHeap{}
	heap.Init(&fc.timers)
	return &FakeClockHandle{fc: fc}
}

// FakeClockHandle is the test-side handle for a fakeClock.
type FakeClockHandle struct{ fc *fakeClock }

// Clock exposes the clock to inject into an Engine.
func (h *FakeClockHandle) Clock() Clock { return h.fc }

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeClock) NewTimer(d time.Duration) Timer {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seq++
	t := &fakeTimer{
		ch:     make(chan time.Time, 1),
		clock:  f,
		fireAt: f.now.Add(d),
		seq:    f.seq,
		index:  -1,
	}
	heap.Push(&f.timers, t)
	return t
}

// Set moves the clock to an absolute instant, firing every timer whose
// deadline is at or before that instant.
func (h *FakeClockHandle) Set(t time.Time) { h.advance(t) }

// Advance moves the clock forward by d, firing due timers (zero d still
// fires timers scheduled at the current instant).
func (h *FakeClockHandle) Advance(d time.Duration) { h.advance(h.fc.now.Add(d)) }

// Now reports the fake current time.
func (h *FakeClockHandle) Now() time.Time {
	h.fc.mu.Lock()
	defer h.fc.mu.Unlock()
	return h.fc.now
}

// PeekFireTimes returns scheduled timer deadlines in chronological order.
func (h *FakeClockHandle) PeekFireTimes() []time.Time {
	h.fc.mu.Lock()
	defer h.fc.mu.Unlock()
	out := make([]time.Time, 0, h.fc.timers.Len())
	for _, t := range h.fc.timers {
		out = append(out, t.fireAt)
	}
	return out
}

// WaitPending blocks until at least n timers are armed or the deadline
// expires. It only waits for goroutine scheduling, never for the clock.
func (h *FakeClockHandle) WaitPending(n int, deadline time.Duration) bool {
	deadlineCh := time.NewTimer(deadline)
	defer deadlineCh.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if h.Pending() >= n {
			return true
		}
		select {
		case <-tick.C:
		case <-deadlineCh.C:
			return false
		}
	}
}

// Pending reports the number of armed timers.
func (h *FakeClockHandle) Pending() int {
	h.fc.mu.Lock()
	defer h.fc.mu.Unlock()
	return h.fc.timers.Len()
}

// AdvanceNext advances to the earliest pending deadline and fires every
// timer due at that instant. It returns false when no timer is pending.
func (h *FakeClockHandle) AdvanceNext() bool {
	h.fc.mu.Lock()
	if h.fc.timers.Len() == 0 {
		h.fc.mu.Unlock()
		return false
	}
	target := h.fc.timers[0].fireAt
	h.fc.mu.Unlock()
	h.advance(target)
	return true
}

// advance fires all timers with deadline <= target while moving time forward.
// Time never moves backwards.
func (h *FakeClockHandle) advance(target time.Time) {
	h.fc.mu.Lock()
	if target.Before(h.fc.now) {
		h.fc.mu.Unlock()
		return
	}
	h.fc.now = target
	var due []*fakeTimer
	for h.fc.timers.Len() > 0 && !h.fc.timers[0].fireAt.After(target) {
		t := heap.Pop(&h.fc.timers).(*fakeTimer)
		t.index = -1
		due = append(due, t)
	}
	h.fc.mu.Unlock()
	for _, t := range due {
		select {
		case t.ch <- target:
		default: // value already pending; match time.Timer coalescing
		}
	}
}

// ---------------------------------------------------------------- timer heap

type timerHeap []*fakeTimer

func (h timerHeap) Len() int { return len(h) }
func (h timerHeap) Less(i, j int) bool {
	if h[i].fireAt.Equal(h[j].fireAt) {
		return h[i].seq < h[j].seq
	}
	return h[i].fireAt.Before(h[j].fireAt)
}
func (h timerHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *timerHeap) Push(x any) {
	t := x.(*fakeTimer)
	t.index = len(*h)
	*h = append(*h, t)
}
func (h *timerHeap) Pop() any {
	old := *h
	n := len(old)
	t := old[n-1]
	old[n-1] = nil
	t.index = -1
	*h = old[:n-1]
	return t
}
