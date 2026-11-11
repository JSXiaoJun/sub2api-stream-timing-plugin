package timing

import (
	"context"
	pluginv1 "github.com/Wei-Shaw/sub2api/pkg/pluginapi/v1"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRPCRetryBypassLeavesHTTPFailure(t *testing.T) {
	for _, kind := range []string{"large", "unknown", "capacity"} {
		t.Run(kind, func(t *testing.T) {
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
			retryConfig(t, c, map[string]any{"retry_body_max_bytes": 65536})
			body := "fixture"
			if kind == "large" {
				body = strings.Repeat("x", 65537)
			}
			length := int64(len(body))
			if kind == "unknown" {
				length = -1
			}
			if kind == "capacity" {
				s.retryCaptures.Store(32)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			stream, e := c.Forward(ctx)
			if e != nil {
				t.Fatal(e)
			}
			stream.Send(&pluginv1.ForwardRequest{Frame: &pluginv1.ForwardRequest_Start{Start: &pluginv1.ForwardRequestStart{Method: "POST", Url: up.URL + "/v1/responses", HasBody: true, ContentLength: length, AccountId: 42, Platform: "openai", AccountType: "oauth"}}})
			stream.Send(&pluginv1.ForwardRequest{Frame: &pluginv1.ForwardRequest_BodyChunk{BodyChunk: []byte(body)}})
			stream.Send(&pluginv1.ForwardRequest{Frame: &pluginv1.ForwardRequest_BodyEnd{BodyEnd: true}})
			stream.CloseSend()
			var got strings.Builder
			status := int32(0)
			for {
				f, e := stream.Recv()
				if e == io.EOF {
					break
				}
				if e != nil {
					t.Fatal(e)
				}
				if f.GetStart() != nil {
					status = f.GetStart().StatusCode
				}
				got.Write(f.GetBodyChunk())
			}
			if status != 503 || got.String() != "busy" || calls.Load() != 1 || s.hints.Load() != 0 {
				t.Fatal("bypass changed host rejection", status, got.String(), calls.Load())
			}
			if kind == "capacity" {
				s.retryCaptures.Store(0)
			}
			assertRetryReleased(t, s)
		})
	}
}
func TestRetryBodyAndCredentialIdentity(t *testing.T) {
	var calls atomic.Int64
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		if string(b) != "fixture exact body" || r.Header.Get("Authorization") != "Bearer fixture-credential" || r.Method != "POST" || r.URL.RawQuery != "fixture=1" {
			t.Error("replay changed request")
		}
		if calls.Add(1) == 1 {
			time.Sleep(60 * time.Millisecond)
			w.WriteHeader(503)
			io.WriteString(w, "busy")
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, successEvents())
	}))
	defer up.Close()
	s := New()
	c := rpcClient(t, s)
	retryConfig(t, c, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stream, e := c.Forward(ctx)
	if e != nil {
		t.Fatal(e)
	}
	body := "fixture exact body"
	stream.Send(&pluginv1.ForwardRequest{Frame: &pluginv1.ForwardRequest_Start{Start: &pluginv1.ForwardRequestStart{Method: "POST", Url: up.URL + "/v1/responses?fixture=1", HasBody: true, ContentLength: int64(len(body)), AccountId: 42, Platform: "openai", AccountType: "oauth", Headers: map[string]*pluginv1.HeaderValues{"Authorization": {Values: []string{"Bearer fixture-credential"}}}}}})
	stream.Send(&pluginv1.ForwardRequest{Frame: &pluginv1.ForwardRequest_BodyChunk{BodyChunk: []byte(body)}})
	stream.Send(&pluginv1.ForwardRequest{Frame: &pluginv1.ForwardRequest_BodyEnd{BodyEnd: true}})
	stream.CloseSend()
	var got strings.Builder
	for {
		f, e := stream.Recv()
		if e == io.EOF {
			break
		}
		if e != nil {
			t.Fatal(e)
		}
		got.Write(f.GetBodyChunk())
	}
	if calls.Load() != 2 || got.String() != timeoutHint+successEvents() {
		t.Fatal("replay failed", calls.Load(), got.String())
	}
	assertRetryReleased(t, s)
}
