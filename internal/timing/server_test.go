package timing

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	pluginv1 "github.com/Wei-Shaw/sub2api/pkg/pluginapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func rpcClient(t *testing.T, s *Server) pluginv1.TransportPluginClient {
	t.Helper()
	l := bufconn.Listen(1024 * 1024)
	g := grpc.NewServer()
	pluginv1.RegisterTransportPluginServer(g, s)
	go g.Serve(l)
	conn, err := grpc.NewClient("passthrough:///fixture", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return l.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(); g.Stop(); l.Close(); s.Close() })
	return pluginv1.NewTransportPluginClient(conn)
}

type reply struct {
	start       *pluginv1.ForwardResponseStart
	body        []byte
	pe          *pluginv1.ForwardResponseError
	elapsed     time.Duration
	bodyElapsed time.Duration
}

func request(t *testing.T, c pluginv1.TransportPluginClient, url string, ctx context.Context, lengthOverride ...int64) reply {
	t.Helper()
	stream, err := c.Forward(ctx)
	if err != nil {
		t.Fatal(err)
	}
	body := `{"model":"fixture","input":"ping","stream":true}`
	length := int64(len(body))
	if len(lengthOverride) > 0 {
		length = lengthOverride[0]
	}
	start := time.Now()
	if err = stream.Send(&pluginv1.ForwardRequest{Frame: &pluginv1.ForwardRequest_Start{Start: &pluginv1.ForwardRequestStart{Method: "POST", Url: url, Headers: map[string]*pluginv1.HeaderValues{"Content-Type": {Values: []string{"application/json"}}, "Accept-Encoding": {Values: []string{"identity"}}}, HasBody: true, ContentLength: length, AccountId: 42, Platform: "openai", AccountType: "oauth"}}}); err != nil {
		t.Fatal(err)
	}
	if err = stream.Send(&pluginv1.ForwardRequest{Frame: &pluginv1.ForwardRequest_BodyChunk{BodyChunk: []byte(body)}}); err != nil {
		t.Fatal(err)
	}
	stream.Send(&pluginv1.ForwardRequest{Frame: &pluginv1.ForwardRequest_BodyEnd{BodyEnd: true}})
	stream.CloseSend()
	var out reply
	for {
		f, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if f.GetStart() != nil {
			out.start = f.GetStart()
			out.elapsed = time.Since(start)
		}
		out.body = append(out.body, f.GetBodyChunk()...)
		if f.GetError() != nil {
			out.pe = f.GetError()
		}
	}
	out.bodyElapsed = time.Since(start)
	return out
}

func TestRPCUnknownUploadLengthPreservesBody(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.Write(raw)
	}))
	defer up.Close()
	c := rpcClient(t, New())
	r := request(t, c, up.URL+"/v1/responses", context.Background(), 0)
	if r.pe != nil || string(r.body) != `{"model":"fixture","input":"ping","stream":true}` {
		t.Fatal("unknown request length lost upload", r.pe, string(r.body))
	}
}
func apply(t *testing.T, c pluginv1.TransportPluginClient, raw string) {
	t.Helper()
	r, err := c.ApplyConfig(context.Background(), &pluginv1.ApplyConfigRequest{ConfigJson: []byte(raw)})
	if err != nil || !r.Applied {
		t.Fatal(err, r)
	}
}

func TestRPCModesHeadersAndErrors(t *testing.T) {
	for _, encoding := range []string{"identity", "gzip"} {
		t.Run(encoding, func(t *testing.T) {
			body := setup("message") + part("message") + event(map[string]any{"type": "response.output_text.delta", "sequence_number": 3, "delta": "real"})
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got, _ := io.ReadAll(r.Body)
				if string(got) != `{"model":"fixture","input":"ping","stream":true}` {
					t.Error("request changed")
				}
				w.Header().Set("Content-Type", "text/event-stream")
				w.Header().Set("X-Request-ID", "real-request")
				if encoding == "gzip" {
					w.Header().Set("Content-Encoding", "gzip")
					z := gzip.NewWriter(w)
					z.Write([]byte(body))
					z.Close()
				} else {
					io.WriteString(w, body)
				}
			}))
			defer up.Close()
			s := New()
			c := rpcClient(t, s)
			apply(t, c, `{"mode":"hint","hint_after_ms":0}`)
			r := request(t, c, up.URL+"/v1/responses", context.Background())
			if r.pe != nil || r.start == nil {
				t.Fatal(r.pe)
			}
			if len(events(t, string(r.body))) != 5 || r.start.ContentLength != -1 || r.start.Headers["Content-Encoding"] != nil || r.start.Headers["Content-Length"] != nil || r.start.Headers["X-Request-Id"].Values[0] != "real-request" {
				t.Fatal("SSE hint or headers incorrect", r.start)
			}
			apply(t, c, `{"mode":"off"}`)
			r = request(t, c, up.URL+"/v1/responses", context.Background())
			raw := r.body
			if encoding == "gzip" {
				z, err := gzip.NewReader(bytes.NewReader(raw))
				if err != nil {
					t.Fatal(err)
				}
				raw, _ = io.ReadAll(z)
				z.Close()
				if r.start.Headers["Content-Encoding"] == nil {
					t.Fatal("off lost encoding")
				}
			}
			if string(raw) != body {
				t.Fatal("rollback modified bytes")
			}
			apply(t, c, `{"mode":"delay","delay_ms":80}`)
			r = request(t, c, up.URL+"/v1/responses", context.Background())
			if r.bodyElapsed < 70*time.Millisecond {
				t.Fatal("delay not applied", r.bodyElapsed)
			}
		})
	}
	for _, status := range []int{200, 429, 503} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			body := `{"error":{"code":"fixture_error"}}`
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				io.WriteString(w, body)
			}))
			defer up.Close()
			c := rpcClient(t, New())
			apply(t, c, `{"mode":"delay","delay_ms":1000}`)
			r := request(t, c, up.URL+"/v1/responses", context.Background())
			if string(r.body) != body || r.start.StatusCode != int32(status) || r.elapsed > 500*time.Millisecond {
				t.Fatal("JSON/error delayed or changed", r.elapsed)
			}
		})
	}
}

func TestLocalStatusAndConfiguration(t *testing.T) {
	s := New()
	defer s.Close()
	for _, raw := range []string{`null`, `{"mode":"hint","unknown":1}`, `{"mode":null}`, `{"mode":"delay","delay_ms":30001}`, `{"max_event_bytes":1024}`, `{} {}`} {
		if _, err := ParseConfig([]byte(raw)); err == nil {
			t.Fatal("accepted invalid config", raw)
		}
	}
	h, err := s.Health(context.Background(), &pluginv1.HealthRequest{})
	if err != nil || !h.Healthy {
		t.Fatal(err)
	}
	var v map[string]any
	if err = json.Unmarshal([]byte(h.StatusJson), &v); err != nil || v["mode"] != "off" {
		t.Fatal(err, v)
	}
	d, err := s.TestConfig(context.Background(), &pluginv1.TestConfigRequest{ConfigJson: []byte(`{"mode":"hint","hint_after_ms":0}`)})
	if err != nil || !d.Success {
		t.Fatal(err, d)
	}
	h, _ = s.Health(context.Background(), &pluginv1.HealthRequest{})
	json.Unmarshal([]byte(h.StatusJson), &v)
	if v["mode"] != "off" {
		t.Fatal("diagnostic changed configuration")
	}
}

func TestDelayAndRPCCancellationReleaseResources(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := waitDelay(ctx, 1000); err != context.Canceled {
		t.Fatal(err)
	}
	// No per-response timing test uses the live provider; all requests are local fixtures.
	s := New()
	c := rpcClient(t, s)
	apply(t, c, `{"mode":"hint","hint_after_ms":0}`)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, setup("message")+part("message"))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer up.Close()
	ctx, cancel = context.WithCancel(context.Background())
	stream, err := c.Forward(ctx)
	if err != nil {
		t.Fatal(err)
	}
	stream.Send(&pluginv1.ForwardRequest{Frame: &pluginv1.ForwardRequest_Start{Start: &pluginv1.ForwardRequestStart{Method: "POST", Url: up.URL + "/v1/responses", HasBody: true, ContentLength: 2, AccountId: 42, Platform: "openai", AccountType: "oauth"}}})
	stream.Send(&pluginv1.ForwardRequest{Frame: &pluginv1.ForwardRequest_BodyChunk{BodyChunk: []byte(`{}`)}})
	stream.Send(&pluginv1.ForwardRequest{Frame: &pluginv1.ForwardRequest_BodyEnd{BodyEnd: true}})
	for {
		f, e := stream.Recv()
		if e != nil {
			t.Fatal(e)
		}
		if strings.Contains(string(f.GetBodyChunk()), `"delta":""`) {
			break
		}
	}
	cancel()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		h, _ := s.Health(context.Background(), &pluginv1.HealthRequest{})
		var v map[string]any
		json.Unmarshal([]byte(h.StatusJson), &v)
		if v["active_requests"] == float64(0) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("canceled request retained resources")
}

func TestRPCBusyPassesThroughAndHotUpdatePreservesInflight(t *testing.T) {
	release := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	body := setup("message") + part("message") + event(map[string]any{"type": "response.output_text.delta", "sequence_number": 3, "delta": "real"})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		if r.URL.Query().Get("hold") == "1" {
			io.WriteString(w, setup("message"))
			w.(http.Flusher).Flush()
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
			io.WriteString(w, strings.TrimPrefix(body, setup("message")))
			return
		}
		io.WriteString(w, body)
	}))
	defer up.Close()
	defer cancel()
	s := New()
	c := rpcClient(t, s)
	apply(t, c, `{"mode":"hint","hint_after_ms":0,"max_concurrent_transforms":1}`)
	stream, err := c.Forward(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = stream.Send(&pluginv1.ForwardRequest{Frame: &pluginv1.ForwardRequest_Start{Start: &pluginv1.ForwardRequestStart{Method: "POST", Url: up.URL + "/v1/responses?hold=1", AccountId: 42, Platform: "openai", AccountType: "oauth"}}}); err != nil {
		t.Fatal(err)
	}
	stream.Send(&pluginv1.ForwardRequest{Frame: &pluginv1.ForwardRequest_BodyEnd{BodyEnd: true}})
	stream.CloseSend()
	first, err := stream.Recv()
	if err != nil || first.GetStart() == nil {
		t.Fatal("missing held response headers", err)
	}
	busy := request(t, c, up.URL+"/v1/responses", ctx)
	if busy.pe != nil || string(busy.body) != body {
		t.Fatal("busy transform did not pass through", busy.pe)
	}
	apply(t, c, `{"mode":"off"}`)
	updated := request(t, c, up.URL+"/v1/responses", ctx)
	if updated.pe != nil || string(updated.body) != body {
		t.Fatal("updated configuration not applied", updated.pe)
	}
	close(release)
	var held bytes.Buffer
	for {
		f, e := stream.Recv()
		if e == io.EOF {
			break
		}
		if e != nil {
			t.Fatal(e)
		}
		if f.GetError() != nil {
			t.Fatal(f.GetError())
		}
		held.Write(f.GetBodyChunk())
	}
	es := events(t, held.String())
	if len(es) != 5 || es[3]["delta"] != "" || es[4]["delta"] != "real" {
		t.Fatal("inflight configuration changed", es)
	}
	h, err := c.Health(ctx, &pluginv1.HealthRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var status map[string]any
	if err = json.Unmarshal([]byte(h.StatusJson), &status); err != nil {
		t.Fatal(err)
	}
	if status["mode"] != "off" || status["transform_busy"] != float64(1) || status["empty_deltas_sent"] != float64(1) {
		t.Fatal("wrong status after update", status)
	}
}
