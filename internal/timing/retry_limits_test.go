package timing

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	pluginv1 "github.com/Wei-Shaw/sub2api/pkg/pluginapi/v1"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func retryConfig(t *testing.T, c pluginv1.TransportPluginClient, extra map[string]any) {
	t.Helper()
	cfg := map[string]any{"mode": "hint", "hint_after_ms": 20, "retry_backoff_ms": 1, "retry_total_wait_ms": 200}
	for k, v := range extra {
		cfg[k] = v
	}
	b, _ := json.Marshal(cfg)
	apply(t, c, string(b))
}
func successEvents() string {
	return event(map[string]any{"type": "response.output_text.delta", "delta": "real"}) + event(map[string]any{"type": "response.completed", "response": map[string]any{"usage": map[string]any{"output_tokens": 1}}})
}
func overloadEvent() string {
	return event(map[string]any{"type": "response.failed", "response": map[string]any{"error": map[string]any{"code": "server_is_overloaded", "status_code": 503}}})
}
func assertRetryReleased(t *testing.T, s *Server) {
	t.Helper()
	until := time.Now().Add(time.Second)
	for time.Now().Before(until) {
		if s.active.Load() == 0 && s.retryCaptures.Load() == 0 && s.retryBytes.Load() == 0 && s.retryActive.Load() == 0 && s.waitingHeaders.Load() == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("leak active=%d captures=%d bytes=%d retries=%d headers=%d", s.active.Load(), s.retryCaptures.Load(), s.retryBytes.Load(), s.retryActive.Load(), s.waitingHeaders.Load())
}
func TestRPCRetrySSERecovery(t *testing.T) {
	for _, enc := range []string{"identity", "gzip"} {
		t.Run(enc, func(t *testing.T) {
			var calls atomic.Int64
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				n := calls.Add(1)
				if n == 1 {
					time.Sleep(60 * time.Millisecond)
				}
				// Mislabeled SSE is common upstream. Each attempt independently decodes gzip.
				w.Header().Set("Content-Type", "application/json")
				body := successEvents()
				if n == 1 {
					body = overloadEvent()
				}
				if enc == "gzip" {
					w.Header().Set("Content-Encoding", "gzip")
					z := gzip.NewWriter(w)
					io.WriteString(z, body)
					z.Close()
				} else {
					io.WriteString(w, body)
				}
			}))
			defer up.Close()
			s := New()
			c := rpcClient(t, s)
			retryConfig(t, c, nil)
			r, _, _ := timedRequest(t, c, up.URL+"/v1/responses")
			if calls.Load() != 2 || string(r.body) != timeoutHint+successEvents() || r.pe != nil {
				t.Fatalf("calls=%d error=%v body=%s", calls.Load(), r.pe, r.body)
			}
			if s.retryAttempts.Load() != 1 || s.retryContinued.Load() != 1 {
				t.Fatal("retry status not updated")
			}
			assertRetryReleased(t, s)
		})
	}
}
func TestRPCRetryStopsAtUpstreamEvent(t *testing.T) {
	for name, pre := range map[string]string{
		"metadata":  event(map[string]any{"type": "response.created", "response": map[string]any{"id": "real-id"}}),
		"text":      event(map[string]any{"type": "response.output_text.delta", "delta": "real"}),
		"tool":      event(map[string]any{"type": "response.output_item.added", "item": map[string]any{"type": "function_call", "call_id": "call-real", "arguments": ""}}),
		"heartbeat": ": ping\n\n",
		"oversized": event(map[string]any{"type": "response.created", "padding": strings.Repeat("x", 5000)}),
	} {
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int64
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				calls.Add(1)
				time.Sleep(60 * time.Millisecond)
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, pre+overloadEvent())
			}))
			defer up.Close()
			s := New()
			c := rpcClient(t, s)
			retryConfig(t, c, map[string]any{"max_event_bytes": 4096})
			r, _, _ := timedRequest(t, c, up.URL+"/v1/responses")
			if calls.Load() != 1 || string(r.body) != timeoutHint+pre+overloadEvent() {
				t.Fatalf("replayed or changed committed output: calls=%d body=%s", calls.Load(), r.body)
			}
			assertRetryReleased(t, s)
		})
	}
}
func TestRPCRetryPreservesFailures(t *testing.T) {
	for _, tc := range []struct {
		name     string
		status   int
		body     string
		early    bool
		attempts int
	}{
		{"early503", 503, "busy", true, 1},
		{"late503", 503, "busy", false, 3},
		{"auth", 401, "bad auth", false, 1},
		{"forbidden", 403, "denied", false, 1},
		{"quota", 429, "quota", false, 1},
		{"sse_quota", 200, event(map[string]any{"type": "response.failed", "response": map[string]any{"error": map[string]any{"code": "insufficient_quota", "status": 503}}}), false, 1},
		{"sse_usage", 200, event(map[string]any{"type": "response.failed", "response": map[string]any{"usage": map[string]any{"input_tokens": 5}, "error": map[string]any{"code": "server_is_overloaded"}}}), false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int64
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				n := calls.Add(1)
				if !tc.early && n == 1 {
					time.Sleep(60 * time.Millisecond)
				}
				if tc.status == 200 {
					w.Header().Set("Content-Type", "text/event-stream")
				}
				w.WriteHeader(tc.status)
				io.WriteString(w, tc.body)
			}))
			defer up.Close()
			s := New()
			c := rpcClient(t, s)
			retryConfig(t, c, nil)
			r, _, _ := timedRequest(t, c, up.URL+"/v1/responses")
			if calls.Load() != int64(tc.attempts) || r.pe != nil {
				t.Fatalf("calls=%d want=%d error=%v", calls.Load(), tc.attempts, r.pe)
			}
			if tc.early {
				if r.start.StatusCode != 503 || string(r.body) != tc.body {
					t.Fatal("host failover path changed")
				}
			} else if !strings.Contains(string(r.body), "response.failed") {
				t.Fatal("failure lost", string(r.body))
			}
			if tc.attempts == 3 && s.retryExhausted.Load() != 1 {
				t.Fatal("exhaustion counter")
			}
			assertRetryReleased(t, s)
		})
	}
}
func TestRPCRetryBudget(t *testing.T) {
	for _, kind := range []string{"headers", "body", "retry_after", "long_success"} {
		t.Run(kind, func(t *testing.T) {
			var calls atomic.Int64
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				n := calls.Add(1)
				if n == 1 {
					time.Sleep(60 * time.Millisecond)
					if kind == "retry_after" {
						w.Header().Set("Retry-After", "7")
					}
					w.WriteHeader(503)
					io.WriteString(w, "busy")
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				if kind == "long_success" {
					io.WriteString(w, event(map[string]any{"type": "response.output_text.delta", "delta": "real"}))
					w.(http.Flusher).Flush()
					time.Sleep(140 * time.Millisecond)
					io.WriteString(w, event(map[string]any{"type": "response.completed", "response": map[string]any{"usage": map[string]any{"output_tokens": 1}}}))
					return
				}
				if kind == "body" {
					w.(http.Flusher).Flush()
				}
				<-r.Context().Done()
			}))
			defer up.Close()
			s := New()
			c := rpcClient(t, s)
			retryConfig(t, c, map[string]any{"retry_total_wait_ms": 80})
			started := time.Now()
			r, _, _ := timedRequest(t, c, up.URL+"/v1/responses")
			if time.Since(started) > time.Second {
				t.Fatal("retry deadline not enforced")
			}
			if kind == "long_success" {
				if string(r.body) != timeoutHint+successEvents() {
					t.Fatal("deadline interrupted resumed output", string(r.body))
				}
			} else {
				if !strings.Contains(string(r.body), "response.failed") || s.retryBudget.Load() != 1 {
					t.Fatal("failure/budget lost", string(r.body))
				}
			}
			expected := int64(2)
			if kind == "retry_after" {
				expected = 1
			}
			if calls.Load() != expected {
				t.Fatal("attempts", calls.Load())
			}
			assertRetryReleased(t, s)
		})
	}
}
func TestRPCRetryCancellation(t *testing.T) {
	var calls atomic.Int64
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		calls.Add(1)
		time.Sleep(60 * time.Millisecond)
		w.WriteHeader(503)
		io.WriteString(w, "busy")
	}))
	defer up.Close()
	s := New()
	c := rpcClient(t, s)
	retryConfig(t, c, map[string]any{"retry_backoff_ms": 1000, "retry_total_wait_ms": 3000})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, e := c.Forward(ctx)
	if e != nil {
		t.Fatal(e)
	}
	body := []byte("fixture")
	stream.Send(&pluginv1.ForwardRequest{Frame: &pluginv1.ForwardRequest_Start{Start: &pluginv1.ForwardRequestStart{Method: "POST", Url: up.URL + "/v1/responses", HasBody: true, ContentLength: int64(len(body)), AccountId: 42, Platform: "openai", AccountType: "oauth"}}})
	stream.Send(&pluginv1.ForwardRequest{Frame: &pluginv1.ForwardRequest_BodyChunk{BodyChunk: body}})
	stream.Send(&pluginv1.ForwardRequest{Frame: &pluginv1.ForwardRequest_BodyEnd{BodyEnd: true}})
	stream.CloseSend()
	until := time.Now().Add(time.Second)
	for s.retryActive.Load() == 0 && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if s.retryActive.Load() != 1 {
		t.Fatal("did not enter backoff")
	}
	cancel()
	assertRetryReleased(t, s)
	if calls.Load() != 1 {
		t.Fatal("canceled request replayed")
	}
}
func TestReplayCaptureBounds(t *testing.T) {
	s := New()
	cfg := Defaults()
	for _, length := range []int64{-1, 0, int64(cfg.RetryBodyMaxBytes) + 1} {
		req, _ := http.NewRequest("POST", "http://fixture", strings.NewReader("abc"))
		req.ContentLength = length
		c, release := s.captureReplay(req, cfg)
		release()
		if c != nil {
			t.Fatal("unsafe replay length admitted", length)
		}
	}
	req, _ := http.NewRequest("POST", "http://fixture", strings.NewReader("abc"))
	c, release := s.captureReplay(req, cfg)
	if c == nil {
		t.Fatal("capture denied")
	}
	if _, ok := c.snapshot(); ok {
		t.Fatal("incomplete upload replayable")
	}
	got, _ := io.ReadAll(req.Body)
	snap, ok := c.snapshot()
	if !ok || !bytes.Equal(got, snap) {
		t.Fatal("capture mismatch")
	}
	release()
	release()
	assertRetryReleased(t, s)
	s.retryCaptures.Store(int64(cfg.RetryMaxConcurrent))
	req, _ = http.NewRequest("POST", "http://fixture", strings.NewReader("abc"))
	c, release = s.captureReplay(req, cfg)
	release()
	if c != nil {
		t.Fatal("capture cap exceeded")
	}
	s.retryCaptures.Store(0)
	s.retryBytes.Store(maxReplayBytes)
	c, release = s.captureReplay(req, cfg)
	release()
	if c != nil {
		t.Fatal("byte cap exceeded")
	}
	s.retryBytes.Store(0)
	assertRetryReleased(t, s)
}
func TestRetryConfigMigrationAndBounds(t *testing.T) {
	c, e := ParseConfig([]byte("{\"mode\":\"hint\"}"))
	if e != nil || c.RetryMaxAttempts != 2 || c.RetryMaxConcurrent != 32 {
		t.Fatal("old config migration", c, e)
	}
	for _, bad := range []string{"{\"retry_max_attempts\":4}", "{\"retry_total_wait_ms\":0}", "{\"retry_body_max_bytes\":0}", "{\"retry_max_concurrent\":129}", "{\"retry_backoff_ms\":0}"} {
		if _, e := ParseConfig([]byte(bad)); e == nil {
			t.Fatal("accepted invalid config", bad)
		}
	}
}
