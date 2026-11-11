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

// A late 503 must not escape if a replay produces a valid response.
func TestRPCRetryLate503Recovery(t *testing.T) {
	var calls atomic.Int64
	success := event(map[string]any{"type": "response.output_text.delta", "delta": "real"}) + event(map[string]any{"type": "response.completed", "response": map[string]any{"usage": map[string]any{"output_tokens": 1}}})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), "fixture") {
			t.Error("request body not replayed")
		}
		if calls.Add(1) == 1 {
			time.Sleep(120 * time.Millisecond)
			w.WriteHeader(503)
			io.WriteString(w, "{\"error\":{\"code\":\"server_is_overloaded\",\"message\":\"busy\"}}")
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, success)
	}))
	defer up.Close()
	c := rpcClient(t, New())
	apply(t, c, "{\"mode\":\"hint\",\"hint_after_ms\":40}")
	r, _, _ := timedRequest(t, c, up.URL+"/v1/responses")
	if calls.Load() != 2 || string(r.body) != timeoutHint+success || r.pe != nil {
		t.Fatalf("want hint then recovery with 2 calls; calls=%d body=%s error=%v", calls.Load(), r.body, r.pe)
	}
}
