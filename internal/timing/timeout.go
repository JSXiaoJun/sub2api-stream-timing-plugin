package timing

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	pluginv1 "github.com/Wei-Shaw/sub2api/pkg/pluginapi/v1"
	"github.com/tidwall/gjson"
	"io"
	"net/http"
	"strings"
	"time"
)

const timeoutHint = "event: sub2api.timing\ndata: {\"type\":\"sub2api.timing\",\"delta\":\"\"}\n\n"

type httpResult struct {
	response *http.Response
	err      error
}
type bodyResult struct {
	data []byte
	err  error
}

func responseStart(resp *http.Response, sse, decoded bool) *pluginv1.ForwardResponse {
	if resp == nil {
		resp = &http.Response{StatusCode: 200, Status: "200 OK", Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1, Header: make(http.Header), ContentLength: -1}
	}
	h := resp.Header.Clone()
	length := resp.ContentLength
	if sse {
		h.Set("Content-Type", "text/event-stream")
		h.Set("Cache-Control", "no-cache")
		h.Set("X-Accel-Buffering", "no")
	}
	if sse || decoded {
		h.Del("Content-Length")
		h.Del("Content-Encoding")
		h.Del("ETag")
		h.Del("Content-MD5")
		length = -1
	}
	headers := make(map[string]*pluginv1.HeaderValues, len(h))
	for k, v := range h {
		headers[k] = &pluginv1.HeaderValues{Values: v}
	}
	return &pluginv1.ForwardResponse{Frame: &pluginv1.ForwardResponse_Start{Start: &pluginv1.ForwardResponseStart{StatusCode: int32(resp.StatusCode), Status: resp.Status, Protocol: resp.Proto, ProtocolMajor: int32(resp.ProtoMajor), ProtocolMinor: int32(resp.ProtoMinor), Headers: headers, ContentLength: length}}}
}

// forwardTimed is the only stream sender. Reader goroutines deliver bounded
// chunks with backpressure; context cancellation closes the upstream request.
func (s *Server) forwardTimed(stream pluginv1.TransportPlugin_ForwardServer, client *http.Client, req *http.Request, cfg Config, started time.Time, releaseSlot func()) error {
	ctx, cancel := context.WithCancel(req.Context())
	defer cancel()
	req = req.WithContext(ctx)
	capture, releaseCapture := s.captureReplay(req, cfg)
	defer releaseCapture()
	canHint := cfg.RetryMaxAttempts == 0 || capture != nil
	retrying, forwardedUpstream := false, false
	attempts := 0
	var retryDeadline time.Time
	var deadlineTimer *time.Timer
	var lastFailure *retryFailure
	defer func() {
		if deadlineTimer != nil {
			deadlineTimer.Stop()
		}
	}()
	defer func() {
		if stream.Context().Err() != nil {
			s.canceled.Add(1)
		}
	}()
	waiting := true
	s.waitingOutput.Add(1)
	stopWaiting := func() {
		if waiting {
			waiting = false
			s.waitingOutput.Add(-1)
			releaseSlot()
		}
	}
	defer stopWaiting()
	thresholdMS := cfg.hintThresholdMS()
	s.lastHintThreshold.Store(int64(thresholdMS))
	s.hintThresholdSamples.Add(1)
	timer := time.NewTimer(time.Duration(thresholdMS) * time.Millisecond)
	defer timer.Stop()
	timerC := timer.C
	if !canHint {
		timer.Stop()
		timerC = nil
	}
	stopTimer := func() { timer.Stop(); timerC = nil; stopWaiting() }
	if !canHint {
		stopWaiting()
	}
	activeContext := ctx
	var resp *http.Response
	probe := outputProbe{limit: cfg.MaxEventBytes}
	startedResponse, hinted := false, false
	var total int64
	startResponse := func(asSSE, decoded bool) error {
		if startedResponse {
			return nil
		}
		if err := stream.Send(responseStart(resp, asSSE, decoded)); err != nil {
			return err
		}
		startedResponse = true
		return nil
	}
	emit := func(b []byte) error {
		for len(b) > 0 {
			n := min(len(b), 32768)
			if err := stream.Send(&pluginv1.ForwardResponse{Frame: &pluginv1.ForwardResponse_BodyChunk{BodyChunk: b[:n]}}); err != nil {
				return err
			}
			total += int64(n)
			b = b[n:]
		}
		return nil
	}
	// Release at the successful resolving event, before probe.feed forwards any
	// following bytes that might block on a slow downstream in the same chunk.
	emitProbed := func(b []byte) error {
		if activeContext.Err() != nil {
			return activeContext.Err()
		}
		if hinted && !forwardedUpstream && capture != nil && !probe.overflow {
			if f := retrySSE(b); f != nil {
				if f.retryAfter == "" && resp != nil {
					f.retryAfter = resp.Header.Get("Retry-After")
				}
				return f
			}
		}
		if len(b) > 0 && !forwardedUpstream {
			forwardedUpstream = true
			if deadlineTimer != nil {
				deadlineTimer.Stop()
			}
			releaseCapture()
			if retrying {
				s.retryContinued.Add(1)
			}
		}
		if err := emit(b); err != nil {
			return err
		}
		if probe.stopHint && timerC != nil {
			if probe.raw && !probe.terminal {
				s.fast.Add(1)
			}
			stopTimer()
		}
		return nil
	}
	end := func() error {
		return stream.Send(&pluginv1.ForwardResponse{Frame: &pluginv1.ForwardResponse_End{End: &pluginv1.ForwardResponseEnd{BytesReceived: total, DurationMs: time.Since(started).Milliseconds()}}})
	}
	fire := func() error {
		if hinted {
			return nil
		}
		if err := startResponse(true, false); err != nil {
			return err
		}
		// Publish before Send so a concurrent Health query after receipt sees it.
		s.hints.Add(1)
		s.timedHints.Add(1)
		if err := emit([]byte(timeoutHint)); err != nil {
			s.hints.Add(^uint64(0))
			s.timedHints.Add(^uint64(0))
			return err
		}
		hinted = true
		stopTimer()
		return nil
	}
	fail := func(code, message string) error {
		if activeContext.Err() != nil {
			return activeContext.Err()
		}
		s.ioErrors.Add(1)
		if !hinted || probe.raw || probe.overflow {
			return sendError(stream, code, true)
		}
		if err := emit(failedEvent(resp, code, message, nil)); err != nil {
			return err
		}
		return end()
	}
	runAttempt := func(attemptReq *http.Request) error {
		attemptCtx, attemptCancel := context.WithCancel(ctx)
		defer attemptCancel()
		activeContext = attemptCtx
		req := attemptReq.WithContext(attemptCtx)
		ctx := attemptCtx
		resp = nil
		probe = outputProbe{limit: cfg.MaxEventBytes}
		if retrying {
			deadlineTimer = time.AfterFunc(time.Until(retryDeadline), attemptCancel)
			defer deadlineTimer.Stop()
		}
		headers := make(chan httpResult)
		go func() {
			r, e := s.do(client, req)
			select {
			case headers <- httpResult{r, e}:
			case <-ctx.Done():
				if r != nil {
					r.Body.Close()
				}
			}
		}()
		for resp == nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case result := <-headers:
				if result.err != nil {
					if result.response != nil {
						result.response.Body.Close()
					}
					return fail("TIMING_UPSTREAM_IO_ERROR", "Upstream request failed")
				}
				resp = result.response
			case <-timerC:
				if err := fire(); err != nil {
					return err
				}
			}
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			s.non200.Add(1)
			stopTimer()
			if !hinted {
				if err := startResponse(false, false); err != nil {
					return err
				}
				if err := copyStream(ctx, resp.Body, emit); err != nil {
					return fail("TIMING_RESPONSE_IO_ERROR", "Upstream response read failed")
				}
				return end()
			}
			if capture != nil && !forwardedUpstream {
				lastFailure = &retryFailure{event: failedEvent(resp, fmt.Sprintf("upstream_http_%d", resp.StatusCode), http.StatusText(resp.StatusCode), nil)}
				if retryDeadline.IsZero() {
					retryDeadline = time.Now().Add(time.Duration(cfg.RetryTotalWaitMS) * time.Millisecond)
					deadlineTimer = time.AfterFunc(time.Until(retryDeadline), attemptCancel)
					defer deadlineTimer.Stop()
				}
			}
			body, err := readLateError(resp)
			if err != nil {
				return fail("TIMING_RESPONSE_IO_ERROR", "Upstream error response read failed")
			}
			failure := failedEvent(resp, fmt.Sprintf("upstream_http_%d", resp.StatusCode), http.StatusText(resp.StatusCode), body)
			if !forwardedUpstream && capture != nil && retryHTTP(resp.StatusCode, body) {
				return &retryFailure{event: failure, retryAfter: resp.Header.Get("Retry-After")}
			}
			if err = emit(failure); err != nil {
				return err
			}
			return end()
		}
		encoding := strings.ToLower(strings.TrimSpace(resp.Header.Get("Content-Encoding")))
		if encoding != "" && encoding != "identity" && encoding != "gzip" {
			s.encodedSkip.Add(1)
			stopTimer()
			if hinted {
				return fail("TIMING_UNSUPPORTED_ENCODING", "Upstream response encoding is unsupported after a timing hint")
			}
			if err := startResponse(false, false); err != nil {
				return err
			}
			if err := copyStream(ctx, resp.Body, emit); err != nil {
				return fail("TIMING_RESPONSE_IO_ERROR", "Upstream response read failed")
			}
			return end()
		}
		chunks := make(chan bodyResult)
		go readResponse(ctx, resp.Body, encoding, chunks)
		defer func() { s.oversized.Add(probe.oversized); s.malformed.Add(probe.malformed) }()
		var prefix []byte
		var format byte
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timerC:
				if err := fire(); err != nil {
					return err
				}
			case chunk := <-chunks:
				data := chunk.data
				if format == 0 {
					prefix = append(prefix, data...)
					format = wireFormat(prefix, chunk.err != nil)
					if format == 'u' && isEventStream(resp.Header.Get("Content-Type")) {
						format = 's'
					}
					if format == 0 {
						continue
					}
					data = prefix
					prefix = nil
					if format == 's' {
						s.responsesSSE.Add(1)
						if !isEventStream(resp.Header.Get("Content-Type")) {
							s.typeMismatch.Add(1)
						}
						if err := startResponse(true, encoding == "gzip"); err != nil {
							return err
						}
					} else {
						s.responsesOther.Add(1)
						stopTimer()
						if hinted {
							return fail("TIMING_NON_SSE_AFTER_HINT", "Upstream returned a non-SSE response after a timing hint")
						}
						s.fast.Add(1)
						if err := startResponse(false, encoding == "gzip"); err != nil {
							return err
						}
					}
				}
				if len(data) > 0 {
					var err error
					if format == 's' {
						err = probe.feed(data, hinted, emitProbed)
					} else {
						err = emit(data)
					}
					if err != nil {
						if errors.Is(err, ErrEmptyCompletion) {
							s.emptyFailures.Add(1)
							return fail("TIMING_EMPTY_COMPLETION", "Upstream completed without output or usage after a timing hint")
						}
						return err
					}
				}
				if chunk.err != nil {
					if chunk.err != io.EOF {
						return fail("TIMING_RESPONSE_IO_ERROR", "Upstream response read failed")
					}
					if format == 's' {
						if err := probe.finish(hinted, emitProbed); err != nil {
							return fail("TIMING_RESPONSE_IO_ERROR", "Upstream stream ended before real output")
						}
					}
					return end()
				}
			}
		}
	}
	for {
		err := runAttempt(req)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var failure *retryFailure
		if !errors.As(err, &failure) {
			if err != nil && lastFailure != nil && !forwardedUpstream && !time.Now().Before(retryDeadline) {
				s.retryBudget.Add(1)
				if e := emit(lastFailure.event); e != nil {
					return e
				}
				return end()
			}
			return err
		}
		lastFailure = failure
		if attempts >= cfg.RetryMaxAttempts {
			s.retryExhausted.Add(1)
			if e := emit(failure.event); e != nil {
				return e
			}
			return end()
		}
		payload, ok := capture.snapshot()
		if !ok {
			s.retryIncomplete.Add(1)
			if e := emit(failure.event); e != nil {
				return e
			}
			return end()
		}
		if retryDeadline.IsZero() {
			retryDeadline = time.Now().Add(time.Duration(cfg.RetryTotalWaitMS) * time.Millisecond)
		}
		wait := retryWait(cfg, attempts, failure.retryAfter, time.Now())
		if wait >= time.Until(retryDeadline) {
			s.retryBudget.Add(1)
			if e := emit(failure.event); e != nil {
				return e
			}
			return end()
		}
		if !retrying {
			retrying = true
			s.retryActive.Add(1)
			defer s.retryActive.Add(-1)
		}
		pause := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			pause.Stop()
			return ctx.Err()
		case <-pause.C:
		}
		if !time.Now().Before(retryDeadline) {
			s.retryBudget.Add(1)
			if e := emit(failure.event); e != nil {
				return e
			}
			return end()
		}
		next := req.Clone(ctx)
		next.Body = newReplayReader(payload)
		next.GetBody = nil
		req = next
		attempts++
		s.retryAttempts.Add(1)
	}

}

func readResponse(ctx context.Context, body io.Reader, encoding string, out chan<- bodyResult) {
	send := func(b []byte, err error) bool {
		select {
		case out <- bodyResult{b, err}:
			return true
		case <-ctx.Done():
			return false
		}
	}
	if encoding == "gzip" {
		z, err := gzip.NewReader(body)
		if err != nil {
			send(nil, err)
			return
		}
		defer z.Close()
		body = z
	}
	for {
		b := make([]byte, 32768)
		n, err := body.Read(b)
		if n > 0 || err != nil {
			if !send(b[:n], err) {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func readLateError(resp *http.Response) ([]byte, error) {
	var reader io.Reader = resp.Body
	if strings.EqualFold(strings.TrimSpace(resp.Header.Get("Content-Encoding")), "gzip") {
		z, err := gzip.NewReader(reader)
		if err != nil {
			return nil, err
		}
		defer z.Close()
		reader = z
	}
	return io.ReadAll(io.LimitReader(reader, 65536))
}

// An explicit failed terminal avoids the host's bare-error EOF reconstruction,
// which otherwise drops numeric HTTP status and generates a response ID.
func failedEvent(resp *http.Response, code, message string, body []byte) []byte {
	errorBody := map[string]any{"code": code, "message": message, "type": "upstream_error"}
	if gjson.ValidBytes(body) {
		root := gjson.ParseBytes(body)
		if nested := root.Get("response.error"); nested.IsObject() {
			root = nested
		} else if nested := root.Get("error"); nested.IsObject() {
			root = nested
		}
		for _, field := range []string{"code", "message", "type"} {
			if v := root.Get(field); v.Type == gjson.String && v.String() != "" {
				errorBody[field] = v.String()
			}
		}
	}
	errorBody["upstream_code"] = errorBody["code"]
	if resp != nil {
		errorBody["status_code"] = resp.StatusCode
		errorBody["status"] = resp.StatusCode
		if v := resp.Header.Get("Retry-After"); v != "" {
			errorBody["retry_after"] = v
		}
		if v := resp.Header.Get("X-Request-Id"); v != "" {
			errorBody["upstream_request_id"] = v
		}
	}
	payload, _ := json.Marshal(map[string]any{"type": "response.failed", "response": map[string]any{"status": "failed", "error": errorBody}})
	return append(append([]byte("event: response.failed\ndata: "), payload...), []byte("\n\n")...)
}
