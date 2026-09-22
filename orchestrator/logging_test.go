package orchestrator

import (
	"fmt"
	"strings"
	"sync"
	"testing"
)

type logf func(format string, args ...any)

func (f logf) Printf(format string, args ...any) { f(format, args...) }

func testLogger(t *testing.T) Logger {
	return logf(func(format string, args ...any) {
		t.Helper()
		t.Logf(format, args...)
	})
}

// captureLogger records every decision line and optionally forwards to t.
type captureLogger struct {
	mu sync.Mutex
	b  strings.Builder
	t  *testing.T
}

func newCaptureLogger(t *testing.T) *captureLogger { return &captureLogger{t: t} }

func (c *captureLogger) Printf(format string, args ...any) {
	c.mu.Lock()
	c.b.WriteString(strings.TrimRight(fmt.Sprintf(format, args...), "\n"))
	c.b.WriteByte('\n')
	c.mu.Unlock()
	if c.t != nil {
		c.t.Helper()
		c.t.Logf(format, args...)
	}
}

func (c *captureLogger) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.b.String()
}

func (c *captureLogger) contains(sub string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return strings.Contains(c.b.String(), sub)
}
