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

func TestRPCRetryStalledErrorPreservesLatestStatus(t *testing.T) {
	for _, initial := range []bool{true, false} {
		t.Run(map[bool]string{true: "first_error_body", false: "retry_error_body"}[initial], func(t *testing.T) {
			var calls atomic.Int64
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				n := calls.Add(1)
				if n == 1 {
					time.Sleep(60 * time.Millisecond)
					w.WriteHeader(503)
					if !initial {
						io.WriteString(w, "busy")
						return
					}
				} else {
					w.WriteHeader(401)
				}
				w.(http.Flusher).Flush()
				<-r.Context().Done()
			}))
			defer up.Close()
			s := New()
			c := rpcClient(t, s)
			retryConfig(t, c, map[string]any{"retry_total_wait_ms": 80})
			r, _, _ := timedRequest(t, c, up.URL+"/v1/responses")
			want := "\"status_code\":401"
			if initial {
				want = "\"status_code\":503"
			}
			if !strings.Contains(string(r.body), want) || s.retryBudget.Load() != 1 {
				t.Fatal("latest rejection status lost", string(r.body))
			}
			assertRetryReleased(t, s)
		})
	}
}
func TestRetryStateGuard(t *testing.T) {
	for _, body := range []string{
		"{\"type\":\"response.failed\",\"response\":{\"usage\":{\"total_tokens\":9},\"error\":{\"code\":\"server_is_overloaded\"}}}",
		"{\"type\":\"response.failed\",\"response\":{\"usage\":{\"input_tokens\":0,\"output_tokens\":0},\"error\":{\"code\":\"server_is_overloaded\"}}}",
		"{\"type\":\"error\",\"status_code\":401,\"error\":{\"code\":\"server_is_overloaded\"}}",
	} {
		if retrySSE([]byte("data: "+body+"\n\n")) != nil {
			t.Fatal("state/401 replayable", body)
		}
	}
}
