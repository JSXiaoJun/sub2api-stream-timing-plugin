package timing

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	pluginv1 "github.com/Wei-Shaw/sub2api/pkg/pluginapi/v1"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type generation struct {
	cfg     Config
	pool    *transportPool
	active  int
	retired bool
}
type Server struct {
	retryCaptures, retryBytes, retryActive                                                                atomic.Int64
	retryAttempts, retryContinued, retryExhausted, retryBudget, retryBodySkip, retryBusy, retryIncomplete atomic.Uint64

	pluginv1.UnimplementedTransportPluginServer
	mu                                                                                              sync.Mutex
	gen                                                                                             *generation
	active, transforms                                                                              atomic.Int64
	forwarded, hints, delayed, busy, encodedSkip, malformed, oversized, ioErrors, canceled          atomic.Uint64
	waitingHeaders, waitingOutput                                                                   atomic.Int64
	lastHintThreshold                                                                               atomic.Int64
	hintThresholdSamples                                                                            atomic.Uint64
	responsesSSE, responsesOther, typeMismatch, non200, ineligible, fast, timedHints, emptyFailures atomic.Uint64
}

func New() *Server { c := Defaults(); return &Server{gen: &generation{cfg: c, pool: newPool(c)}} }
func (s *Server) acquire() *generation {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gen.active++
	s.active.Add(1)
	return s.gen
}
func (s *Server) release(g *generation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g.active--
	s.active.Add(-1)
	if g.retired && g.active == 0 {
		g.pool.close()
	}
}
func (s *Server) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gen.retired = true
	if s.gen.active == 0 {
		s.gen.pool.close()
	}
}
func (s *Server) GetInfo(context.Context, *pluginv1.GetInfoRequest) (*pluginv1.GetInfoResponse, error) {
	return &pluginv1.GetInfoResponse{PluginId: PluginID, PluginVersion: Version, ProtocolVersion: 1, TransportApiVersion: 1, Capabilities: []string{Capability, APIKeyCapability}}, nil
}
func (s *Server) ValidateConfig(_ context.Context, r *pluginv1.ValidateConfigRequest) (*pluginv1.ValidateConfigResponse, error) {
	c, err := ParseConfig(r.ConfigJson)
	if err != nil {
		return &pluginv1.ValidateConfigResponse{Message: err.Error()}, nil
	}
	b, _ := json.Marshal(c)
	return &pluginv1.ValidateConfigResponse{Valid: true, NormalizedConfigJson: b}, nil
}
func (s *Server) ApplyConfig(_ context.Context, r *pluginv1.ApplyConfigRequest) (*pluginv1.ApplyConfigResponse, error) {
	c, err := ParseConfig(r.ConfigJson)
	if err != nil {
		return &pluginv1.ApplyConfigResponse{Message: err.Error()}, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.gen.cfg != c {
		old := s.gen
		s.gen = &generation{cfg: c, pool: newPool(c)}
		old.retired = true
		if old.active == 0 {
			old.pool.close()
		}
	}
	return &pluginv1.ApplyConfigResponse{Applied: true, Message: "配置已应用；进行中的请求使用原配置完成"}, nil
}
func (s *Server) Health(context.Context, *pluginv1.HealthRequest) (*pluginv1.HealthResponse, error) {
	s.mu.Lock()
	g := s.gen
	s.mu.Unlock()
	b, _ := json.Marshal(map[string]any{"retry_max_attempts": g.cfg.RetryMaxAttempts, "retry_body_max_bytes": g.cfg.RetryBodyMaxBytes, "retry_max_concurrent": g.cfg.RetryMaxConcurrent, "retry_backoff_ms": g.cfg.RetryBackoffMS, "retry_total_wait_ms": g.cfg.RetryTotalWaitMS,
		"active_retry_captures": s.retryCaptures.Load(), "retry_capture_bytes": s.retryBytes.Load(), "active_retries": s.retryActive.Load(), "retry_attempts": s.retryAttempts.Load(), "retry_streams_resumed": s.retryContinued.Load(), "retry_exhausted": s.retryExhausted.Load(), "retry_budget_exceeded": s.retryBudget.Load(), "retry_body_bypassed": s.retryBodySkip.Load(), "retry_capacity_bypassed": s.retryBusy.Load(), "retry_incomplete_upload": s.retryIncomplete.Load(), "plugin_version": Version, "mode": g.cfg.Mode, "delay_ms": g.cfg.DelayMS, "hint_after_ms": g.cfg.HintAfterMS, "hint_wait_mode": g.cfg.HintWaitMode, "hint_min_ms": g.cfg.HintMinMS, "hint_max_ms": g.cfg.HintMaxMS, "last_hint_threshold_ms": s.lastHintThreshold.Load(), "hint_threshold_samples": s.hintThresholdSamples.Load(), "max_concurrent_transforms": g.cfg.MaxConcurrentTransforms, "forwarded": s.forwarded.Load(), "empty_deltas_sent": s.hints.Load(), "timed_hints_sent": s.timedHints.Load(), "fast_responses": s.fast.Load(), "waiting_upstream_headers": s.waitingHeaders.Load(), "waiting_first_output": s.waitingOutput.Load(), "responses_sse": s.responsesSSE.Load(), "responses_non_sse": s.responsesOther.Load(), "content_type_mismatch": s.typeMismatch.Load(), "http_non_200": s.non200.Load(), "ineligible_requests": s.ineligible.Load(), "empty_completion_failures": s.emptyFailures.Load(), "delayed": s.delayed.Load(), "transform_busy": s.busy.Load(), "unsupported_encoding": s.encodedSkip.Load(), "malformed_events": s.malformed.Load(), "oversized_events": s.oversized.Load(), "active_requests": s.active.Load(), "active_transforms": s.transforms.Load(), "transport_pools": g.pool.count(), "io_errors": s.ioErrors.Load(), "caller_canceled": s.canceled.Load(), "ttft_effect": "semantic_only_for_hint", "native_websocket": false})
	return &pluginv1.HealthResponse{Healthy: true, Message: "运行中；状态查询不访问上游", StatusJson: string(b)}, nil
}
func (s *Server) TestConfig(ctx context.Context, r *pluginv1.TestConfigRequest) (*pluginv1.TestConfigResponse, error) {
	if _, err := ParseConfig(r.ConfigJson); err != nil {
		return &pluginv1.TestConfigResponse{Message: err.Error()}, nil
	}
	h, _ := s.Health(ctx, &pluginv1.HealthRequest{})
	return &pluginv1.TestConfigResponse{Success: true, Message: "本地配置校验通过；未发送上游请求", StatusJson: h.StatusJson}, nil
}
func sendError(stream pluginv1.TransportPlugin_ForwardServer, code string, sent bool) error {
	return stream.Send(&pluginv1.ForwardResponse{Frame: &pluginv1.ForwardResponse_Error{Error: &pluginv1.ForwardResponseError{Code: code, Message: code, RequestSent: sent}}})
}
func (s *Server) error(stream pluginv1.TransportPlugin_ForwardServer, code string, sent bool) error {
	if stream.Context().Err() != nil {
		s.canceled.Add(1)
		return stream.Context().Err()
	}
	s.ioErrors.Add(1)
	return sendError(stream, code, sent)
}
func eligible(st *pluginv1.ForwardRequestStart, u *url.URL) bool {
	return st.Method == "POST" && strings.HasSuffix(strings.TrimRight(u.Path, "/"), "/responses")
}
func (s *Server) Forward(stream pluginv1.TransportPlugin_ForwardServer) error {
	started := time.Now()
	g := s.acquire()
	defer s.release(g)
	f, err := stream.Recv()
	if err != nil {
		return err
	}
	st := f.GetStart()
	if st == nil || st.Platform != "openai" || (st.AccountType != "oauth" && st.AccountType != "apikey") || st.AccountId <= 0 || st.ContentLength < -1 {
		return sendError(stream, "TIMING_INVALID_REQUEST", false)
	}
	u, err := url.Parse(st.Url)
	if err != nil || u.Host == "" || u.User != nil || u.Fragment != "" || (u.Scheme != "http" && u.Scheme != "https") || strings.ContainsAny(st.Host, "\r\n") || (!st.HasBody && st.ContentLength > 0) {
		return sendError(stream, "TIMING_INVALID_REQUEST", false)
	}
	client, release, err := g.pool.acquire(st.ProxyUrl, st.AccountId)
	if err != nil {
		return sendError(stream, "TIMING_INVALID_PROXY", false)
	}
	defer release()
	var body io.Reader
	var pipe *io.PipeReader
	uploadLength := st.ContentLength
	if st.HasBody && uploadLength == 0 {
		uploadLength = -1
	}
	if st.HasBody {
		r, w := io.Pipe()
		pipe = r
		body = r
		defer r.Close()
		go upload(stream, w, uploadLength)
	} else {
		go drain(stream)
	}
	req, err := http.NewRequestWithContext(stream.Context(), st.Method, st.Url, body)
	if err != nil {
		return sendError(stream, "TIMING_INVALID_REQUEST", false)
	}
	req.Host = st.Host
	req.ContentLength = uploadLength
	req.GetBody = nil
	for key, v := range st.Headers {
		if v != nil {
			req.Header[key] = append([]string(nil), v.Values...)
		}
	}
	s.forwarded.Add(1)
	runConfig := g.cfg
	if eligible(st, u) && runConfig.timedHint() {
		if s.admit(runConfig.MaxConcurrentTransforms) {
			releaseSlot := sync.OnceFunc(func() { s.transforms.Add(-1) })
			defer releaseSlot()
			return s.forwardTimed(stream, client, req, runConfig, started, releaseSlot)
		}
		s.busy.Add(1)
		runConfig.Mode = "off"
	}
	if !eligible(st, u) {
		s.ineligible.Add(1)
	}
	resp, err := s.do(client, req)
	if pipe != nil {
		defer pipe.Close()
	}
	if err != nil {
		return s.error(stream, "TIMING_UPSTREAM_IO_ERROR", true)
	}
	defer resp.Body.Close()
	media, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if resp.StatusCode != http.StatusOK {
		s.non200.Add(1)
	}
	isStream := eligible(st, u) && resp.StatusCode == http.StatusOK
	encoding := strings.ToLower(strings.TrimSpace(resp.Header.Get("Content-Encoding")))
	transform := isStream && runConfig.Mode != "off"
	releaseSlot := func() {}
	if transform && encoding != "" && encoding != "identity" && encoding != "gzip" {
		transform = false
		s.encodedSkip.Add(1)
	}
	if transform {
		if !s.admit(runConfig.MaxConcurrentTransforms) {
			transform = false
			s.busy.Add(1)
		} else {
			releaseSlot = sync.OnceFunc(func() { s.transforms.Add(-1) })
			defer releaseSlot()
		}
	}
	reader := io.Reader(resp.Body)
	decoded := false
	if transform && encoding == "gzip" {
		z, err := gzip.NewReader(resp.Body)
		if err != nil {
			return s.error(stream, "TIMING_RESPONSE_ENCODING_ERROR", true)
		}
		defer z.Close()
		reader = z
		decoded = true
	}
	if transform {
		var wire byte
		reader, wire, err = sniffReader(reader)
		if err != nil {
			return s.error(stream, "TIMING_RESPONSE_IO_ERROR", true)
		}
		if wire == 'u' && strings.EqualFold(media, "text/event-stream") {
			wire = 's'
		}
		if wire == 's' {
			s.responsesSSE.Add(1)
			if !strings.EqualFold(media, "text/event-stream") {
				s.typeMismatch.Add(1)
			}
		} else {
			transform = false
			s.responsesOther.Add(1)
		}
	}
	if !transform {
		releaseSlot()
	}
	headers := resp.Header.Clone()
	length := resp.ContentLength
	if transform || decoded {
		headers.Del("Content-Length")
		headers.Del("Content-Encoding")
		headers.Del("ETag")
		headers.Del("Content-MD5")
		length = -1
	}
	if transform {
		headers.Set("Content-Type", "text/event-stream")
	}
	h := map[string]*pluginv1.HeaderValues{}
	for k, v := range headers {
		h[k] = &pluginv1.HeaderValues{Values: append([]string(nil), v...)}
	}
	if err = stream.Send(&pluginv1.ForwardResponse{Frame: &pluginv1.ForwardResponse_Start{Start: &pluginv1.ForwardResponseStart{StatusCode: int32(resp.StatusCode), Status: resp.Status, Protocol: resp.Proto, ProtocolMajor: int32(resp.ProtoMajor), ProtocolMinor: int32(resp.ProtoMinor), Headers: h, ContentLength: length}}}); err != nil {
		return err
	}
	var total int64
	emit := func(b []byte) error {
		for len(b) > 0 {
			n := len(b)
			if n > 32768 {
				n = 32768
			}
			if err := stream.Send(&pluginv1.ForwardResponse{Frame: &pluginv1.ForwardResponse_BodyChunk{BodyChunk: b[:n]}}); err != nil {
				return err
			}
			total += int64(n)
			b = b[n:]
		}
		return nil
	}
	if transform {
		stats, transformErr := transformWithRelease(stream.Context(), reader, emit, runConfig, releaseSlot)
		s.hints.Add(uint64(stats.Hints))
		s.delayed.Add(uint64(stats.Delays))
		s.malformed.Add(uint64(stats.MalformedEvents))
		s.oversized.Add(uint64(stats.OversizedEvents))
		err = transformErr
	} else {
		err = copyStream(stream.Context(), reader, emit)
	}
	if err != nil {
		if errors.Is(err, ErrEmptyCompletion) {
			s.emptyFailures.Add(1)
			return sendError(stream, "TIMING_EMPTY_COMPLETION", true)
		}
		return s.error(stream, "TIMING_RESPONSE_IO_ERROR", true)
	}
	return stream.Send(&pluginv1.ForwardResponse{Frame: &pluginv1.ForwardResponse_End{End: &pluginv1.ForwardResponseEnd{BytesReceived: total, DurationMs: time.Since(started).Milliseconds()}}})
}

func (s *Server) do(client *http.Client, req *http.Request) (*http.Response, error) {
	s.waitingHeaders.Add(1)
	defer s.waitingHeaders.Add(-1)
	return client.Do(req)
}
func (s *Server) admit(max int) bool {
	for {
		n := s.transforms.Load()
		if n >= int64(max) {
			return false
		}
		if s.transforms.CompareAndSwap(n, n+1) {
			return true
		}
	}
}
func upload(stream pluginv1.TransportPlugin_ForwardServer, w *io.PipeWriter, length int64) {
	defer w.Close()
	var total int64
	for {
		f, err := stream.Recv()
		if err != nil {
			w.CloseWithError(errors.New("interrupted upload"))
			return
		}
		switch v := f.Frame.(type) {
		case *pluginv1.ForwardRequest_BodyChunk:
			total += int64(len(v.BodyChunk))
			if len(v.BodyChunk) > 256*1024 || (length >= 0 && total > length) {
				w.CloseWithError(errors.New("invalid upload length"))
				return
			}
			if _, err := w.Write(v.BodyChunk); err != nil {
				return
			}
		case *pluginv1.ForwardRequest_BodyEnd:
			if !v.BodyEnd || (length >= 0 && total != length) {
				w.CloseWithError(errors.New("invalid upload end"))
			}
			return
		default:
			w.CloseWithError(errors.New("invalid upload frame"))
			return
		}
	}
}
func drain(stream pluginv1.TransportPlugin_ForwardServer) {
	for {
		f, e := stream.Recv()
		if e != nil || f.GetBodyEnd() {
			return
		}
	}
}
