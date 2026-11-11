package timing

import (
	"bytes"
	"context"
	"encoding/json"
	pluginv1 "github.com/Wei-Shaw/sub2api/pkg/pluginapi/v1"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRPCMislabelledSSEIsTransformed(t *testing.T) {
	for _, contentType := range []string{"application/json", ""} {
		t.Run(contentType, func(t *testing.T) {
			body := setup("message") + part("message") + event(map[string]any{"type": "response.output_text.delta", "sequence_number": 3, "delta": "real"})
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				if contentType != "" {
					w.Header().Set("Content-Type", contentType)
				} else {
					w.Header()["Content-Type"] = nil
				}
				io.WriteString(w, body)
			}))
			defer up.Close()
			c := rpcClient(t, New())
			apply(t, c, `{"mode":"hint","hint_after_ms":0}`)
			r := request(t, c, up.URL+"/v1/responses", context.Background())
			if len(events(t, string(r.body))) != 5 {
				t.Fatal("mislabelled SSE was skipped", string(r.body))
			}
		})
	}
}

func timedRequest(t *testing.T, c pluginv1.TransportPluginClient, url string) (reply, time.Duration, bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	stream, err := c.Forward(ctx)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	body := `{"model":"fixture","input":"ping","stream":true}`
	if err = stream.Send(&pluginv1.ForwardRequest{Frame: &pluginv1.ForwardRequest_Start{Start: &pluginv1.ForwardRequestStart{Method: "POST", Url: url, HasBody: true, ContentLength: int64(len(body)), AccountId: 42, Platform: "openai", AccountType: "oauth", Headers: map[string]*pluginv1.HeaderValues{"Content-Type": {Values: []string{"application/json"}}}}}}); err != nil {
		t.Fatal(err)
	}
	stream.Send(&pluginv1.ForwardRequest{Frame: &pluginv1.ForwardRequest_BodyChunk{BodyChunk: []byte(body)}})
	stream.Send(&pluginv1.ForwardRequest{Frame: &pluginv1.ForwardRequest_BodyEnd{BodyEnd: true}})
	stream.CloseSend()
	var r reply
	var hintAt time.Duration
	live := false
	for {
		f, e := stream.Recv()
		if e == io.EOF {
			break
		}
		if e != nil {
			t.Fatal(e)
		}
		if f.GetStart() != nil {
			r.start = f.GetStart()
			r.elapsed = time.Since(start)
		}
		r.body = append(r.body, f.GetBodyChunk()...)
		if hintAt == 0 && bytes.Contains(r.body, []byte(`"type":"sub2api.timing"`)) {
			hintAt = time.Since(start)
			h, e := c.Health(ctx, &pluginv1.HealthRequest{})
			if e != nil {
				t.Fatal(e)
			}
			var status map[string]any
			json.Unmarshal([]byte(h.StatusJson), &status)
			live = status["empty_deltas_sent"].(float64) > 0
		}
		if f.GetError() != nil {
			r.pe = f.GetError()
		}
	}
	r.bodyElapsed = time.Since(start)
	return r, hintAt, live
}

func TestRPCTimedHintThresholdAndFastPassThrough(t *testing.T) {
	for _, tc := range []struct {
		name              string
		headers, preamble bool
		delay             time.Duration
		hint              bool
	}{
		{"fast", true, true, 20 * time.Millisecond, false},
		{"slow_headers", false, false, 240 * time.Millisecond, true},
		{"slow_body", true, false, 240 * time.Millisecond, true},
		{"metadata_only", true, true, 240 * time.Millisecond, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pre := setup("message") + part("message")
			tail := event(map[string]any{"type": "response.output_text.delta", "sequence_number": 3, "delta": "real"}) + event(map[string]any{"type": "response.completed", "sequence_number": 4, "response": map[string]any{"usage": map[string]any{"input_tokens": 10, "output_tokens": 2}}})
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "text/event-stream")
				if tc.preamble {
					io.WriteString(w, pre)
				}
				if tc.headers {
					w.(http.Flusher).Flush()
				}
				select {
				case <-time.After(tc.delay):
				case <-r.Context().Done():
					return
				}
				if !tc.preamble {
					io.WriteString(w, pre)
				}
				io.WriteString(w, tail)
			}))
			defer up.Close()
			c := rpcClient(t, New())
			apply(t, c, `{"mode":"hint","hint_after_ms":100}`)
			r, at, live := timedRequest(t, c, up.URL+"/v1/responses")
			if r.pe != nil || r.start == nil || r.start.StatusCode != 200 {
				t.Fatal(r.pe, r.start)
			}
			if tc.hint {
				if at < 80*time.Millisecond || at > 200*time.Millisecond || !live {
					t.Fatal("timeout hint missing/late/not live", at, live)
				}
				es := events(t, string(r.body))
				if len(es) != 6 {
					t.Fatal("unexpected event count", len(es))
				}
				if strings.Count(string(r.body), `"type":"sub2api.timing"`) != 1 {
					t.Fatal("duplicate hint")
				}
				for _, e := range es {
					if e["type"] == "response.output_text.delta" && e["sequence_number"] != float64(3) {
						t.Fatal("real sequence changed", e)
					}
				}
			} else if at != 0 || string(r.body) != pre+tail {
				t.Fatal("fast response changed", at, string(r.body))
			}
		})
	}
}

func TestRPCTimedLateHTTPErrorStaysFailure(t *testing.T) {
	for _, delay := range []time.Duration{0, 220 * time.Millisecond} {
		t.Run(delay.String(), func(t *testing.T) {
			body := `{"type":"error","error":{"code":"server_is_overloaded","type":"server_error","message":"fixture overloaded"}}`
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				select {
				case <-time.After(delay):
				case <-r.Context().Done():
					return
				}
				w.Header().Set("Retry-After", "7")
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(503)
				io.WriteString(w, body)
			}))
			defer up.Close()
			c := rpcClient(t, New())
			apply(t, c, `{"mode":"hint","hint_after_ms":80,"retry_max_attempts":0}`)
			r, at, _ := timedRequest(t, c, up.URL+"/v1/responses")
			if delay == 0 {
				if at != 0 || r.start.StatusCode != 503 || string(r.body) != body {
					t.Fatal("fast HTTP error changed", r.start, at)
				}
			} else {
				if at == 0 || r.start.StatusCode != 200 {
					t.Fatal("timeout not committed")
				}
				es := events(t, string(r.body))
				last := es[len(es)-1]
				if last["type"] != "response.failed" {
					t.Fatal("late error became success", last)
				}
				errorBody := last["response"].(map[string]any)["error"].(map[string]any)
				if errorBody["code"] != "server_is_overloaded" || errorBody["type"] != "server_error" || errorBody["status_code"] != float64(503) || errorBody["retry_after"] != "7" {
					t.Fatal("late error details lost", errorBody)
				}
			}
		})
	}
}
