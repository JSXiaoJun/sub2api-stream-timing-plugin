package timing

import (
	"bytes"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tidwall/gjson"
)

// A separate admission limit bounds replay memory after the hint slot is freed.
// The global byte cap also applies across live config generations.
const maxReplayBytes = 64 << 20

type replayCapture struct {
	mu                  sync.Mutex
	body                io.ReadCloser
	buf                 []byte
	expected            int64
	complete, abandoned bool
}

func (c *replayCapture) Read(p []byte) (int, error) {
	n, err := c.body.Read(p)
	c.mu.Lock()
	if !c.abandoned && !c.complete {
		if int64(len(c.buf)+n) > c.expected || (err != nil && err != io.EOF) {
			c.abandoned = true
			c.buf = nil
		} else {
			c.buf = append(c.buf, p[:n]...)
			c.complete = int64(len(c.buf)) == c.expected
		}
	}
	c.mu.Unlock()
	return n, err
}
func (c *replayCapture) Close() error { return c.body.Close() }
func (c *replayCapture) snapshot() ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Immutable once complete. No second multi-megabyte allocation per attempt.
	return c.buf, c.complete && !c.abandoned
}
func reserve(g *atomic.Int64, n, limit int64) bool {
	for {
		old := g.Load()
		if n > limit-old {
			return false
		}
		if g.CompareAndSwap(old, old+n) {
			return true
		}
	}
}
func (s *Server) captureReplay(req *http.Request, cfg Config) (*replayCapture, func()) {
	if cfg.RetryMaxAttempts == 0 {
		return nil, func() {}
	}
	if req.ContentLength < 0 || req.ContentLength > int64(cfg.RetryBodyMaxBytes) || (req.ContentLength == 0 && req.Body != nil && req.Body != http.NoBody) || (req.Body == nil && req.ContentLength > 0) {
		s.retryBodySkip.Add(1)
		return nil, func() {}
	}
	if !reserve(&s.retryCaptures, 1, int64(cfg.RetryMaxConcurrent)) {
		s.retryBusy.Add(1)
		return nil, func() {}
	}
	if !reserve(&s.retryBytes, req.ContentLength, maxReplayBytes) {
		s.retryCaptures.Add(-1)
		s.retryBusy.Add(1)
		return nil, func() {}
	}
	n := req.ContentLength
	c := &replayCapture{body: req.Body, expected: n, buf: make([]byte, 0, int(n)), complete: n == 0}
	if req.Body != nil && req.Body != http.NoBody {
		req.Body = c
	}
	release := sync.OnceFunc(func() {
		c.mu.Lock()
		c.abandoned = true
		c.buf = nil
		c.mu.Unlock()
		s.retryBytes.Add(-n)
		s.retryCaptures.Add(-1)
	})
	return c, release
}

// Unlike io.NopCloser, Close drops the slice reference after HTTP upload.
// Read and Close may run concurrently in net/http.
type replayReader struct {
	mu sync.Mutex
	r  *bytes.Reader
}

func newReplayReader(b []byte) *replayReader { return &replayReader{r: bytes.NewReader(b)} }
func (r *replayReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.r.Read(p)
}
func (r *replayReader) Close() error { r.mu.Lock(); r.r.Reset(nil); r.mu.Unlock(); return nil }

type retryFailure struct {
	event      []byte
	retryAfter string
}

func (*retryFailure) Error() string { return "timing: retryable upstream rejection before output" }
func overloadCode(root gjson.Result) bool {
	for _, path := range []string{"code", "type", "error.code", "error.type", "response.error.code", "response.error.type"} {
		switch root.Get(path).String() {
		case "server_is_overloaded", "server_overloaded", "overloaded_error":
			return true
		}
	}
	return false
}

// Some upstream gateways remove code/status but keep this provider message.
// Match the complete normalized sentence, only inside an error object (or a
// top-level error event). Explicit unrelated codes/types take precedence.
func overloadMessage(root gjson.Result) bool {
	if v := root.Get("type"); v.Exists() && v.Type != gjson.Null {
		if v.Type != gjson.String {
			return false
		}
		switch strings.ToLower(strings.TrimSpace(v.String())) {
		case "", "error", "response.failed", "server_error", "upstream_error":
		default:
			return false
		}
	}
	for _, path := range []string{"code", "error.code", "response.error.code"} {
		v := root.Get(path)
		if v.Exists() && v.Type != gjson.Null && (v.Type != gjson.String || strings.TrimSpace(v.String()) != "") {
			return false
		}
	}
	for _, path := range []string{"error.type", "response.error.type"} {
		v := root.Get(path)
		if !v.Exists() || v.Type == gjson.Null {
			continue
		}
		if v.Type != gjson.String {
			return false
		}
		switch strings.ToLower(strings.TrimSpace(v.String())) {
		case "", "server_error", "upstream_error":
		default:
			return false
		}
	}
	paths := []string{"error.message", "response.error.message"}
	if root.Get("type").String() == "error" {
		paths = append(paths, "message")
	}
	for _, path := range paths {
		v := root.Get(path)
		if v.Type != gjson.String {
			continue
		}
		message := strings.ToLower(strings.Join(strings.Fields(v.String()), " "))
		if message == "our servers are currently overloaded. please try again later." {
			return true
		}
	}
	return false
}
func nonRetryableCode(root gjson.Result) bool {
	for _, path := range []string{"code", "type", "error.code", "error.type", "response.error.code", "response.error.type"} {
		switch root.Get(path).String() {
		case "insufficient_quota", "quota_exceeded", "billing_hard_limit_reached", "invalid_api_key", "authentication_error", "permission_error":
			return true
		}
	}
	return false
}
func retryHTTP(status int, b []byte) bool {
	root := gjson.ParseBytes(b)
	if nonRetryableCode(root) || retryHasState(root) {
		return false
	}
	if status == 503 {
		return true
	}
	return (status == 429 || status == 500 || status == 502) && (overloadCode(root) || overloadMessage(root))
}
func retrySSE(b []byte) *retryFailure {
	payload := sseData(b)
	if !gjson.Valid(payload) {
		return nil
	}
	root := gjson.Parse(payload)
	typ := root.Get("type").String()
	if typ == "" {
		for _, line := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(line, "event:") {
				typ = strings.TrimSpace(line[6:])
			}
		}
	}
	if (typ != "error" && typ != "response.failed") || retryHasState(root) || nonRetryableCode(root) {
		return nil
	}
	retry := overloadCode(root) || overloadMessage(root)
	for _, path := range []string{"status", "status_code", "error.status", "error.status_code", "response.error.status", "response.error.status_code"} {
		v := root.Get(path)
		if v.Type == gjson.Number && (v.Int() == 401 || v.Int() == 403) {
			return nil
		}
		if v.Type == gjson.Number && v.Int() == 503 {
			retry = true
		}
	}
	if !retry {
		return nil
	}
	after := ""
	for _, path := range []string{"retry_after", "error.retry_after", "response.error.retry_after"} {
		if v := root.Get(path); v.Exists() {
			after = v.String()
			break
		}
	}
	return &retryFailure{event: bytes.Clone(b), retryAfter: after}
}

// Reject even empty usage objects and partial tool/output state conservatively:
// a failure may be the only event summarizing work already performed upstream.
func retryHasState(root gjson.Result) bool {
	for _, prefix := range []string{"", "response.", "data.", "data.response."} {
		for _, key := range []string{"usage", "tool_usage", "item", "call_id", "tool_calls", "function_call", "arguments", "delta", "text", "output_text", "content", "output"} {
			v := root.Get(prefix + key)
			if !v.Exists() || v.Type == gjson.Null {
				continue
			}
			if (key == "output" || key == "content" || key == "tool_calls") && v.IsArray() && len(v.Array()) == 0 {
				continue
			}
			if v.Type == gjson.String && v.String() == "" {
				continue
			}
			return true
		}
	}
	return false
}
func retryWait(cfg Config, attempt int, after string, now time.Time) time.Duration {
	base := time.Duration(cfg.RetryBackoffMS) * time.Millisecond * (1 << attempt)
	wait := base + time.Duration(rand.Int64N(int64(base/4)+1))
	if n, e := strconv.ParseInt(strings.TrimSpace(after), 10, 64); e == nil && n > 0 {
		if n > 60 {
			return time.Hour
		}
		wait = max(wait, time.Duration(n)*time.Second)
	} else if t, e := http.ParseTime(after); e == nil {
		wait = max(wait, t.Sub(now))
	}
	return wait
}
