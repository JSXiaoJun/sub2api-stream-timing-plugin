package timing

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const messageOnlyFailure = "{\"response\":{\"created_at\":0,\"error\":{\"code\":\"\",\"message\":\"Our servers are currently overloaded. Please try again later.\"},\"id\":\"\",\"model\":\"\",\"object\":\"\",\"output\":null,\"status\":\"failed\"},\"type\":\"response.failed\"}"

func TestRPCMessageOnlyOverload(t *testing.T) {
	for _, kind := range []string{"recovery", "exhausted", "before_hint", "after_metadata", "after_text"} {
		t.Run(kind, func(t *testing.T) {
			var calls atomic.Int64
			raw := "event: response.failed\ndata: " + messageOnlyFailure + "\n\n"
			prefix := ""
			if kind == "after_metadata" {
				prefix = event(map[string]any{"type": "response.created", "response": map[string]any{"id": "fixture-id"}})
			}
			if kind == "after_text" {
				prefix = event(map[string]any{"type": "response.output_text.delta", "delta": "already emitted"})
			}
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				if string(body) != "{\"model\":\"fixture\",\"input\":\"ping\",\"stream\":true}" {
					t.Error("replay changed request body")
				}
				n := calls.Add(1)
				if n == 1 && kind != "before_hint" {
					time.Sleep(90 * time.Millisecond)
				}
				w.Header().Set("Content-Type", "application/json") // Production also mislabels SSE.
				if kind == "recovery" && n > 1 {
					io.WriteString(w, successEvents())
					return
				}
				io.WriteString(w, prefix+raw)
			}))
			defer up.Close()
			s := New()
			c := rpcClient(t, s)
			retryConfig(t, c, map[string]any{"hint_after_ms": 30})
			r, _, _ := timedRequest(t, c, up.URL+"/v1/responses")
			wantCalls := int64(1)
			want := timeoutHint + prefix + raw
			if kind == "before_hint" {
				want = raw
			}
			if kind == "recovery" {
				wantCalls = 2
				want = timeoutHint + successEvents()
			}
			if kind == "exhausted" {
				wantCalls = 3
			}
			if calls.Load() != wantCalls || string(r.body) != want || r.pe != nil {
				t.Fatalf("calls=%d want=%d body=%s error=%v", calls.Load(), wantCalls, r.body, r.pe)
			}
			if kind == "recovery" && s.retryAttempts.Load() != 1 {
				t.Fatal("missing retry count")
			}
			if kind == "exhausted" && s.retryExhausted.Load() != 1 {
				t.Fatal("missing exhaustion count")
			}
			assertRetryReleased(t, s)
			t.Logf("calls=%d retry_attempts=%d failure_forwarded=%v", calls.Load(), s.retryAttempts.Load(), strings.Contains(string(r.body), "response.failed"))
		})
	}
}

func TestMessageOnlyOverloadClassifier(t *testing.T) {
	for _, typ := range []string{"invalid_request_error", "authentication_error", "permission_error", "response.completed"} {
		body := []byte("{\"type\":\"" + typ + "\",\"error\":{\"message\":\"Our servers are currently overloaded. Please try again later.\"}}")
		for _, status := range []int{429, 500, 502} {
			if retryHTTP(status, body) {
				t.Errorf("HTTP%d explicit type %s retried via message", status, typ)
			}
		}
	}
	for _, tc := range []struct {
		name, body string
		want       bool
	}{
		{"exact_user_error", messageOnlyFailure, true},
		{"whitespace_case", strings.Replace(messageOnlyFailure, "Our servers are currently overloaded. Please try again later.", "  OUR SERVERS ARE CURRENTLY OVERLOADED.   Please try again later.  ", 1), true},
		{"error_envelope", "{\"type\":\"error\",\"error\":{\"message\":\"Our servers are currently overloaded. Please try again later.\"}}", true},
		{"generic_busy", "{\"type\":\"response.failed\",\"response\":{\"error\":{\"message\":\"service busy\"}}}", false},
		{"negated", strings.Replace(messageOnlyFailure, "Our servers are currently overloaded.", "Our servers are not currently overloaded.", 1), false},
		{"quoted", strings.Replace(messageOnlyFailure, "Our servers are currently overloaded.", "The documentation says: Our servers are currently overloaded.", 1), false},
		{"quota", strings.Replace(messageOnlyFailure, "\"code\":\"\"", "\"code\":\"insufficient_quota\"", 1), false},
		{"auth", strings.Replace(messageOnlyFailure, "\"code\":\"\"", "\"code\":\"invalid_api_key\"", 1), false},
		{"explicit_other_code", strings.Replace(messageOnlyFailure, "\"code\":\"\"", "\"code\":\"invalid_request_error\"", 1), false},
		{"status401", strings.Replace(messageOnlyFailure, "\"code\":\"\"", "\"status_code\":401,\"code\":\"\"", 1), false},
		{"usage", strings.Replace(messageOnlyFailure, "\"output\":null", "\"usage\":{\"total_tokens\":7},\"output\":null", 1), false},
		{"text_delta", "{\"type\":\"response.output_text.delta\",\"delta\":\"Our servers are currently overloaded. Please try again later.\"}", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := retrySSE([]byte("data: "+tc.body+"\n\n")) != nil
			if got != tc.want {
				t.Fatalf("retryable=%v want=%v", got, tc.want)
			}
		})
	}
	for _, status := range []int{401, 403, 429, 500, 502, 503} {
		want := status != 401 && status != 403
		if got := retryHTTP(status, []byte(messageOnlyFailure)); got != want {
			t.Errorf("HTTP%d retryable=%v want=%v", status, got, want)
		}
	}
}
