package orchestrator

import (
	"testing"
	"time"
)

func TestFakeClockTimerFiresOnAdvance(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	h := NewFakeClock(t0)
	c := h.Clock()
	tm := c.NewTimer(5 * time.Second)
	if tm == nil {
		t.Fatal("nil timer")
	}
	select {
	case <-tm.C():
		t.Fatal("timer must not fire before time advances")
	default:
	}
	h.Advance(4 * time.Second)
	select {
	case <-tm.C():
		t.Fatal("timer must not fire before deadline")
	default:
	}
	h.Advance(1 * time.Second)
	got := <-tm.C()
	if !got.Equal(t0.Add(5 * time.Second)) {
		t.Fatalf("fired at %v, want %v", got, t0.Add(5*time.Second))
	}
	t.Logf("timer fired deterministically at fake time %v without real waiting", got)
}

func TestFakeClockStop(t *testing.T) {
	h := NewFakeClock(time.Time{})
	tm := h.Clock().NewTimer(time.Second)
	if !tm.Stop() {
		t.Fatal("Stop should return true for active timer")
	}
	h.Advance(time.Minute)
	select {
	case <-tm.C():
		t.Fatal("stopped timer must not fire")
	default:
	}
}

func TestFakeClockResetAndOrdering(t *testing.T) {
	h := NewFakeClock(time.Time{})
	t1 := h.Clock().NewTimer(3 * time.Second)
	t2 := h.Clock().NewTimer(1 * time.Second)
	if !h.AdvanceNext() {
		t.Fatal("expected pending timer")
	}
	select {
	case <-t2.C():
	default:
		t.Fatal("earliest timer should fire first")
	}
	select {
	case <-t1.C():
		t.Fatal("later timer must not fire yet")
	default:
	}
	if !t1.Reset(2 * time.Second) {
		t.Fatal("Reset should report an active timer")
	}
}
