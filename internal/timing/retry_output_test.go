package timing

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestRPCRetryFailureWithOutputIsNotReplayed(t *testing.T) {
	var calls atomic.Int64
	body := event(map[string]any{"type": "response.failed", "response": map[string]any{"output": []any{map[string]any{"type": "function_call", "call_id": "real", "arguments": "{}"}}, "error": map[string]any{"code": "server_is_overloaded"}}})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		calls.Add(1)
		time.Sleep(60 * time.Millisecond)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, body)
	}))
	defer up.Close()
	s := New()
	c := rpcClient(t, s)
	retryConfig(t, c, nil)
	r, _, _ := timedRequest(t, c, up.URL+"/v1/responses")
	if calls.Load() != 1 || string(r.body) != timeoutHint+body {
		t.Fatal("failed event with existing tool/output replayed", calls.Load(), string(r.body))
	}
}
