package timing

import (
	"bytes"
	"compress/gzip"
	"context"
	pluginv1 "github.com/Wei-Shaw/sub2api/pkg/pluginapi/v1"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestTimedEmptyCompletionAndTruncatedEvent(t *testing.T) {
	for _, usage := range []string{"null", `{"input_tokens":0,"output_tokens":0}`} {
		for _, large := range []bool{false, true} {
			p := outputProbe{limit: 4096}
			var out bytes.Buffer
			emit := func(b []byte) error { _, e := out.Write(b); return e }
			pre := "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"usage\":" + usage + "}}\n\n"
			if large {
				pre += ":" + strings.Repeat("x", 8192) + "\n\n"
			}
			if err := p.feed([]byte(pre), true, emit); err != nil {
				t.Fatal(err)
			}
			terminal := "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"output\":[],\"usage\":" + usage + "}}\n\n"
			if err := p.feed([]byte(terminal), true, emit); err != ErrEmptyCompletion {
				t.Errorf("empty success accepted usage=%s large=%v: %v", usage, large, err)
			}
			if strings.Contains(out.String(), "response.completed") {
				t.Error("empty completion emitted")
			}
		}
	}
	p := outputProbe{limit: 4096}
	var out bytes.Buffer
	emit := func(b []byte) error { _, e := out.Write(b); return e }
	p.feed([]byte("event: response.output_text.delta\ndata: {\"type\":"), true, emit)
	if err := p.finish(true, emit); err == nil {
		t.Error("truncated stream succeeded")
	}
	if out.Len() != 0 {
		t.Error("partial event would corrupt appended failure", out.String())
	}
}

func TestRPCTimedFragmentsFormatsAndToolPreamble(t *testing.T) {
	delta := event(map[string]any{"type": "response.output_text.delta", "sequence_number": 3, "delta": "real"})
	tool := event(map[string]any{"type": "response.output_item.added", "sequence_number": 1, "item": map[string]any{"type": "function_call", "arguments": ""}})
	for _, kind := range []string{"half_event", "short_prefix", "wrong_mime", "gzip", "empty_tool", "named_event", "unknown_field"} {
		t.Run(kind, func(t *testing.T) {
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "text/event-stream")
				if kind == "wrong_mime" {
					w.Header().Set("Content-Type", "application/json")
				}
				var prefix, tail string
				switch kind {
				case "half_event":
					prefix, tail = delta[:30], delta[30:]
				case "short_prefix":
					prefix, tail = "da", delta
				case "empty_tool":
					prefix, tail = tool, delta
				case "named_event":
					prefix, tail = "event: response.created\ndata: {\"response\":{\"output\":[]}}\n\n", delta
				case "unknown_field":
					prefix, tail = "x-transport: local\n\n", delta
				default:
					tail = delta
				}
				if kind == "short_prefix" {
					tail = "ta: {\"type\":\"response.output_text.delta\",\"sequence_number\":3,\"delta\":\"real\"}\n\n"
				}
				if kind == "gzip" {
					w.Header().Set("Content-Encoding", "gzip")
				}
				io.WriteString(w, prefix)
				w.(http.Flusher).Flush()
				time.Sleep(160 * time.Millisecond)
				if kind == "gzip" {
					z := gzip.NewWriter(w)
					io.WriteString(z, tail)
					z.Close()
				} else {
					io.WriteString(w, tail)
				}
			}))
			defer up.Close()
			c := rpcClient(t, New())
			apply(t, c, `{"mode":"hint","hint_after_ms":60}`)
			r, at, live := timedRequest(t, c, up.URL+"/v1/responses")
			if at < 40*time.Millisecond || at > 140*time.Millisecond || !live {
				t.Fatal("timer missed", at, live)
			}
			es := events(t, string(r.body))
			hints, real := 0, 0
			for _, e := range es {
				if e["type"] == "sub2api.timing" {
					hints++
				}
				if e["type"] == "response.output_text.delta" && e["delta"] == "real" && e["sequence_number"] == float64(3) {
					real++
				}
			}
			if hints != 1 || real != 1 || r.pe != nil {
				t.Fatal("framing or output changed", string(r.body), r.pe)
			}
			if kind != "gzip" && strings.ReplaceAll(string(r.body), timeoutHint, "") != func() string {
				if kind == "empty_tool" {
					return tool + delta
				}
				if kind == "unknown_field" {
					return "x-transport: local\n\n" + delta
				}
				if kind == "named_event" {
					return "event: response.created\ndata: {\"response\":{\"output\":[]}}\n\n" + delta
				}
				if kind == "short_prefix" {
					return "data: {\"type\":\"response.output_text.delta\",\"sequence_number\":3,\"delta\":\"real\"}\n\n"
				}
				return delta
			}() {
				t.Fatal("real bytes changed")
			}
		})
	}
}

func TestRPCTimedNonSSEAndEncoding(t *testing.T) {
	for _, late := range []bool{false, true} {
		for _, encoding := range []string{"", "br"} {
			name := encoding
			if late {
				name += "late"
			}
			t.Run(name, func(t *testing.T) {
				up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					io.Copy(io.Discard, r.Body)
					if late {
						time.Sleep(140 * time.Millisecond)
					}
					w.Header().Set("Content-Type", "application/json")
					if encoding != "" {
						w.Header().Set("Content-Encoding", encoding)
					}
					io.WriteString(w, `{"result":"fixture"}`)
				}))
				defer up.Close()
				c := rpcClient(t, New())
				apply(t, c, `{"mode":"hint","hint_after_ms":50}`)
				r, at, _ := timedRequest(t, c, up.URL+"/v1/responses")
				if late {
					es := events(t, string(r.body))
					if at == 0 || es[len(es)-1]["type"] != "response.failed" {
						t.Fatal("late non-SSE succeeded", string(r.body))
					}
				} else if at != 0 || string(r.body) != `{"result":"fixture"}` {
					t.Fatal("fast response changed", string(r.body))
				}
			})
		}
	}
}

func TestRPCTimedCancellationSaturationAndHotConfig(t *testing.T) {
	for _, headers := range []bool{false, true} {
		t.Run(map[bool]string{true: "body", false: "headers"}[headers], func(t *testing.T) {
			arrived := make(chan struct{}, 1)
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				if headers {
					w.Header().Set("Content-Type", "text/event-stream")
					w.(http.Flusher).Flush()
				}
				arrived <- struct{}{}
				<-r.Context().Done()
			}))
			defer up.Close()
			s := New()
			c := rpcClient(t, s)
			apply(t, c, `{"mode":"hint","hint_after_ms":50,"max_concurrent_transforms":1}`)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			st, e := c.Forward(ctx)
			if e != nil {
				t.Fatal(e)
			}
			st.Send(&pluginv1.ForwardRequest{Frame: &pluginv1.ForwardRequest_Start{Start: &pluginv1.ForwardRequestStart{Method: "POST", Url: up.URL + "/v1/responses", AccountId: 42, Platform: "openai", AccountType: "oauth"}}})
			st.Send(&pluginv1.ForwardRequest{Frame: &pluginv1.ForwardRequest_BodyEnd{BodyEnd: true}})
			st.CloseSend()
			<-arrived
			fastBody := event(map[string]any{"type": "response.output_text.delta", "delta": "real"})
			fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, fastBody)
			}))
			defer fast.Close()
			pass := request(t, c, fast.URL+"/v1/responses", context.Background())
			if string(pass.body) != fastBody || s.busy.Load() != 1 {
				t.Fatal("saturation did not pass through")
			}
			apply(t, c, `{"mode":"off"}`)
			var body strings.Builder
			for !strings.Contains(body.String(), "sub2api.timing") {
				f, e := st.Recv()
				if e != nil {
					t.Fatal(e)
				}
				body.Write(f.GetBodyChunk())
			}
			cancel()
			deadline := time.Now().Add(time.Second)
			for time.Now().Before(deadline) && (s.transforms.Load() != 0 || s.waitingHeaders.Load() != 0 || s.waitingOutput.Load() != 0) {
				time.Sleep(5 * time.Millisecond)
			}
			if s.transforms.Load() != 0 || s.waitingHeaders.Load() != 0 || s.waitingOutput.Load() != 0 {
				t.Fatal("cancellation leaked resources")
			}
		})
	}
}

func TestRPCTimedSlowUploadAndSingleStart(t *testing.T) {
	got := make(chan string, 1)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got <- string(b)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, event(map[string]any{"type": "response.output_text.delta", "delta": "real"}))
	}))
	defer up.Close()
	c := rpcClient(t, New())
	apply(t, c, `{"mode":"hint","hint_after_ms":50}`)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	st, e := c.Forward(ctx)
	if e != nil {
		t.Fatal(e)
	}
	st.Send(&pluginv1.ForwardRequest{Frame: &pluginv1.ForwardRequest_Start{Start: &pluginv1.ForwardRequestStart{Method: "POST", Url: up.URL + "/v1/responses", HasBody: true, ContentLength: 7, AccountId: 42, Platform: "openai", AccountType: "oauth"}}})
	st.Send(&pluginv1.ForwardRequest{Frame: &pluginv1.ForwardRequest_BodyChunk{BodyChunk: []byte("first")}})
	starts := 0
	var body strings.Builder
	for !strings.Contains(body.String(), "sub2api.timing") {
		f, e := st.Recv()
		if e != nil {
			t.Fatal(e)
		}
		if f.GetStart() != nil {
			starts++
		}
		body.Write(f.GetBodyChunk())
	}
	st.Send(&pluginv1.ForwardRequest{Frame: &pluginv1.ForwardRequest_BodyChunk{BodyChunk: []byte("ok")}})
	st.Send(&pluginv1.ForwardRequest{Frame: &pluginv1.ForwardRequest_BodyEnd{BodyEnd: true}})
	st.CloseSend()
	for {
		f, e := st.Recv()
		if e == io.EOF {
			break
		}
		if e != nil {
			t.Fatal(e)
		}
		if f.GetStart() != nil {
			starts++
		}
		if f.GetError() != nil {
			t.Fatal(f.GetError())
		}
		body.Write(f.GetBodyChunk())
	}
	if starts != 1 || <-got != "firstok" || !strings.Contains(body.String(), "real") {
		t.Fatal("upload or response corrupted", starts, body.String())
	}
}

func TestTimedHintConfigBounds(t *testing.T) {
	for _, raw := range []string{`{"hint_after_ms":-1}`, `{"hint_after_ms":60001}`, `{"hint_after_ms":null}`, `{"hint_after_ms":1.5}`} {
		if _, err := ParseConfig([]byte(raw)); err == nil {
			t.Fatal("invalid accepted", raw)
		}
	}
	for _, raw := range []string{`{}`, `{"mode":"hint"}`} {
		c, e := ParseConfig([]byte(raw))
		if e != nil || c.HintAfterMS != 1000 {
			t.Fatal("default changed", c, e)
		}
	}
}
