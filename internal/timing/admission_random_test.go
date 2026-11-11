package timing

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	pluginv1 "github.com/Wei-Shaw/sub2api/pkg/pluginapi/v1"
)

type timingRoundTripFunc func(*http.Request) (*http.Response, error)

func (f timingRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// Models only the transport's downstream backpressure. The probe, timed
// forwarding, framing, and admission cleanup are real production code.
type timingBackpressureStream struct {
	pluginv1.TransportPlugin_ForwardServer
	ctx        context.Context
	secondSend chan struct{}
	bodies     int
}

func (s *timingBackpressureStream) Context() context.Context { return s.ctx }
func (s *timingBackpressureStream) Send(f *pluginv1.ForwardResponse) error {
	if len(f.GetBodyChunk()) > 0 {
		s.bodies++
		if s.bodies == 2 {
			close(s.secondSend)
			<-s.ctx.Done()
			return s.ctx.Err()
		}
	}
	return nil
}

func TestHintSlotReleasedBeforeFollowingOutputBackpressure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := &timingBackpressureStream{ctx: ctx, secondSend: make(chan struct{})}
	body := event(map[string]any{"type": "response.output_text.delta", "delta": "real"}) + strings.Repeat("x", 1024)
	client := &http.Client{Transport: timingRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
	})}
	req, _ := http.NewRequestWithContext(ctx, "POST", "http://fixture/v1/responses", nil)
	s := New()
	defer s.Close()
	if !s.admit(1) {
		t.Fatal("admission failed")
	}
	release := sync.OnceFunc(func() { s.transforms.Add(-1) })
	done := make(chan error, 1)
	cfg := Defaults()
	cfg.Mode = "hint"
	cfg.HintAfterMS = 1000
	go func() { defer release(); done <- s.forwardTimed(stream, client, req, cfg, time.Now(), release) }()
	select {
	case <-stream.secondSend:
	case <-time.After(time.Second):
		t.Fatal("following output did not reach blocked send")
	}
	if s.transforms.Load() != 0 {
		t.Error("first effective event was sent but following output retained admission")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("blocked send did not cancel")
	}
	awaitTimingState(t, "backpressure cleanup released twice", func() bool { return s.transforms.Load() == 0 })
}

func awaitTimingState(t *testing.T, description string, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !predicate() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !predicate() {
		t.Fatal(description)
	}
}

func openTimingStream(t *testing.T, ctx context.Context, c pluginv1.TransportPluginClient, url string) pluginv1.TransportPlugin_ForwardClient {
	t.Helper()
	stream, err := c.Forward(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range []*pluginv1.ForwardRequest{
		{Frame: &pluginv1.ForwardRequest_Start{Start: &pluginv1.ForwardRequestStart{Method: "POST", Url: url, AccountId: 42, Platform: "openai", AccountType: "oauth"}}},
		{Frame: &pluginv1.ForwardRequest_BodyEnd{BodyEnd: true}},
	} {
		if err := stream.Send(f); err != nil {
			t.Fatal(err)
		}
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	return stream
}

// Catches the old full-response defer: a long stream must not prevent the
// next request from receiving its own hint once the first wait is resolved.
func TestRPCHintSlotReleasedBeforeStreamEnds(t *testing.T) {
	for _, kind := range []string{"slow_headers", "slow_body", "fast_output"} {
		t.Run(kind, func(t *testing.T) {
			finish := make(chan struct{})
			firstOutput := event(map[string]any{"type": "response.output_text.delta", "delta": "first-real", "sequence_number": 3})
			tail := event(map[string]any{"type": "response.output_text.delta", "delta": "tail-real", "sequence_number": 4}) + event(map[string]any{"type": "response.completed", "response": map[string]any{"usage": map[string]any{"input_tokens": 10, "output_tokens": 2}}})
			first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				if kind == "fast_output" {
					io.WriteString(w, firstOutput)
				}
				if kind != "slow_headers" {
					w.(http.Flusher).Flush()
				}
				select {
				case <-finish:
				case <-r.Context().Done():
					return
				}
				if kind != "fast_output" {
					io.WriteString(w, firstOutput)
				}
				io.WriteString(w, tail)
			}))
			defer first.Close()
			second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				select {
				case <-time.After(160 * time.Millisecond):
				case <-r.Context().Done():
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, firstOutput+tail)
			}))
			defer second.Close()
			s := New()
			c := rpcClient(t, s)
			apply(t, c, `{"mode":"hint","hint_after_ms":40,"max_concurrent_transforms":1}`)
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			stream := openTimingStream(t, ctx, c, first.URL+"/v1/responses")
			var received strings.Builder
			marker := "sub2api.timing"
			if kind == "fast_output" {
				marker = "first-real"
			}
			for !strings.Contains(received.String(), marker) {
				frame, err := stream.Recv()
				if err != nil {
					t.Fatal(err)
				}
				if frame.GetError() != nil {
					t.Fatal(frame.GetError())
				}
				received.Write(frame.GetBodyChunk())
			}
			awaitTimingState(t, "waiting slot retained after hint/output while stream remains open", func() bool { return s.transforms.Load() == 0 })
			if s.active.Load() != 1 {
				t.Fatal("first stream stopped instead of releasing only its wait slot")
			}
			result, at, _ := timedRequest(t, c, second.URL+"/v1/responses")
			if result.pe != nil || at == 0 || s.busy.Load() != 0 {
				t.Fatal("second request skipped hint", result.pe, at, s.busy.Load())
			}
			close(finish)
			for {
				frame, err := stream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				if frame.GetError() != nil {
					t.Fatal(frame.GetError())
				}
				received.Write(frame.GetBodyChunk())
			}
			want := firstOutput + tail
			if kind != "fast_output" {
				want = timeoutHint + want
			}
			if received.String() != want {
				t.Fatal("continuing stream changed", received.String())
			}
			awaitTimingState(t, "slot leaked or released twice", func() bool { return s.transforms.Load() == 0 && s.active.Load() == 0 })
		})
	}
}

func TestHintWaitConfigCompatibilityAndBounds(t *testing.T) {
	for _, raw := range []string{
		`{"mode":"hint","hint_after_ms":1500}`,
		`{"mode":"hint","hint_after_ms":0}`,
		`{"hint_wait_mode":"random","hint_min_ms":1,"hint_max_ms":60000}`,
		`{"hint_wait_mode":"random","hint_min_ms":800,"hint_max_ms":800}`,
	} {
		c, err := ParseConfig([]byte(raw))
		if err != nil {
			t.Fatalf("valid config rejected: %s: %v", raw, err)
		}
		b, _ := json.Marshal(c)
		var out map[string]any
		json.Unmarshal(b, &out)
		var in map[string]any
		json.Unmarshal([]byte(raw), &in)
		if in["hint_wait_mode"] == nil && out["hint_wait_mode"] != "fixed" {
			t.Fatal("old config no longer selects fixed waiting")
		}
		if v, ok := in["hint_after_ms"]; ok && out["hint_after_ms"] != v {
			t.Fatal("old fixed value changed")
		}
	}
	for _, raw := range []string{
		`{"hint_wait_mode":"other"}`, `{"hint_wait_mode":null}`,
		`{"hint_wait_mode":"random","hint_min_ms":0}`, `{"hint_min_ms":-1}`,
		`{"hint_max_ms":60001}`, `{"hint_min_ms":2001,"hint_max_ms":2000}`,
		`{"hint_min_ms":1.5}`, `{"hint_max_ms":null}`,
	} {
		if _, err := ParseConfig([]byte(raw)); err == nil {
			t.Fatal("invalid range accepted", raw)
		}
	}
}

func TestRPCLegacyHintReleasesSlot(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, setup("message")+part("message"))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer up.Close()
	s := New()
	c := rpcClient(t, s)
	apply(t, c, `{"mode":"hint","hint_after_ms":0,"max_concurrent_transforms":1}`)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream := openTimingStream(t, ctx, c, up.URL+"/v1/responses")
	var body strings.Builder
	for !strings.Contains(body.String(), `"delta":""`) {
		f, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		body.Write(f.GetBodyChunk())
	}
	awaitTimingState(t, "legacy hint retained admission", func() bool { return s.transforms.Load() == 0 })
	if s.active.Load() != 1 {
		t.Fatal("legacy stream stopped")
	}
	cancel()
	awaitTimingState(t, "legacy cleanup released twice", func() bool { return s.transforms.Load() == 0 && s.active.Load() == 0 })
}

// Exercises selection through real RPC requests. Each admitted request should
// draw once; old fixed=0 must not disable the explicit random mode.
func TestRPCRandomThresholdOncePerRequest(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(100 * time.Millisecond):
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, event(map[string]any{"type": "response.output_text.delta", "delta": "real"}))
	}))
	defer up.Close()
	s := New()
	c := rpcClient(t, s)
	apply(t, c, `{"mode":"hint","hint_after_ms":0,"hint_wait_mode":"random","hint_min_ms":20,"hint_max_ms":50}`)
	unique := map[int]bool{}
	for i := 1; i <= 16; i++ {
		result, at, _ := timedRequest(t, c, up.URL+"/v1/responses")
		if result.pe != nil || at < 15*time.Millisecond || strings.Count(string(result.body), `"type":"sub2api.timing"`) != 1 {
			t.Fatal("random hint missing/early/duplicated", result.pe, at)
		}
		health, err := c.Health(context.Background(), &pluginv1.HealthRequest{})
		if err != nil {
			t.Fatal(err)
		}
		var status map[string]any
		json.Unmarshal([]byte(health.StatusJson), &status)
		chosen, ok := status["last_hint_threshold_ms"].(float64)
		if !ok || chosen < 20 || chosen > 50 {
			t.Fatal("sample outside configured range", status)
		}
		if status["hint_threshold_samples"] != float64(i) {
			t.Fatal("threshold sampled multiple times or missing", status)
		}
		unique[int(chosen)] = true
	}
	if len(unique) < 2 {
		t.Fatal("range did not vary across requests")
	}
	apply(t, c, `{"mode":"hint","hint_wait_mode":"random","hint_min_ms":35,"hint_max_ms":35}`)
	_, at, _ := timedRequest(t, c, up.URL+"/v1/responses")
	if at < 30*time.Millisecond {
		t.Fatal("equal-bound range fired early", at)
	}
	health, _ := c.Health(context.Background(), &pluginv1.HealthRequest{})
	var status map[string]any
	json.Unmarshal([]byte(health.StatusJson), &status)
	if status["last_hint_threshold_ms"] != float64(35) {
		t.Fatal("equal-bound range changed", status)
	}
	apply(t, c, `{"mode":"hint","hint_wait_mode":"random","hint_min_ms":200,"hint_max_ms":250}`)
	fastResult, at, _ := timedRequest(t, c, up.URL+"/v1/responses")
	if at != 0 || string(fastResult.body) != event(map[string]any{"type": "response.output_text.delta", "delta": "real"}) {
		t.Fatal("random waiting changed fast response", at, string(fastResult.body))
	}
}

func TestRPCRandomThresholdSurvivesHotUpdate(t *testing.T) {
	arrived := make(chan struct{}, 1)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		arrived <- struct{}{}
		<-r.Context().Done()
	}))
	defer up.Close()
	s := New()
	c := rpcClient(t, s)
	apply(t, c, `{"mode":"hint","hint_wait_mode":"random","hint_min_ms":250,"hint_max_ms":250,"max_concurrent_transforms":1}`)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	started := time.Now()
	stream := openTimingStream(t, ctx, c, up.URL+"/v1/responses")
	<-arrived
	apply(t, c, `{"mode":"hint","hint_wait_mode":"random","hint_min_ms":10,"hint_max_ms":10,"max_concurrent_transforms":1}`)
	var body strings.Builder
	for !strings.Contains(body.String(), "sub2api.timing") {
		f, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		body.Write(f.GetBodyChunk())
	}
	if time.Since(started) < 220*time.Millisecond {
		t.Fatal("inflight random threshold was replaced")
	}
	awaitTimingState(t, "hot update retained hint slot", func() bool { return s.transforms.Load() == 0 })
	health, err := c.Health(ctx, &pluginv1.HealthRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var status map[string]any
	json.Unmarshal([]byte(health.StatusJson), &status)
	if status["hint_threshold_samples"] != float64(1) || status["last_hint_threshold_ms"] != float64(250) || status["hint_min_ms"] != float64(10) {
		t.Fatal("inflight selection or new configuration changed", status)
	}
	cancel()
	awaitTimingState(t, "hot update cleanup leaked or released twice", func() bool { return s.transforms.Load() == 0 && s.active.Load() == 0 })
}
