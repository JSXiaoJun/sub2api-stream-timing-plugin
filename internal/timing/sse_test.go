package timing

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

func event(v any) string { b, _ := json.Marshal(v); return "data: " + string(b) + "\n\n" }
func setup(kind string) string {
	item := map[string]any{"id": "item-real", "type": kind}
	if kind == "message" {
		item["role"] = "assistant"
		item["content"] = []any{}
	} else {
		item["summary"] = []any{}
	}
	return event(map[string]any{"type": "response.created", "sequence_number": 0, "response": map[string]any{"id": "resp-real", "status": "in_progress"}}) + event(map[string]any{"type": "response.output_item.added", "sequence_number": 1, "output_index": 0, "item": item})
}
func part(kind string) string {
	typ, idx, partType := "response.content_part.added", "content_index", "output_text"
	if kind == "reasoning" {
		typ, idx, partType = "response.reasoning_summary_part.added", "summary_index", "summary_text"
	}
	return event(map[string]any{"type": typ, "sequence_number": 2, "item_id": "item-real", "output_index": 0, idx: 0, "part": map[string]any{"type": partType, "text": ""}})
}
func events(t *testing.T, raw string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, e := range strings.Split(strings.ReplaceAll(raw, "\r\n", "\n"), "\n\n") {
		var data []string
		for _, line := range strings.Split(e, "\n") {
			if strings.HasPrefix(line, "data:") {
				data = append(data, strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
			}
		}
		if len(data) == 0 || strings.Join(data, "\n") == "[DONE]" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(strings.Join(data, "\n")), &m); err != nil {
			t.Fatal(err)
		}
		out = append(out, m)
	}
	return out
}

type smallReader struct{ r io.Reader }

func (r smallReader) Read(b []byte) (int, error) {
	if len(b) > 7 {
		b = b[:7]
	}
	return r.r.Read(b)
}

func TestHintPreservesRealLifecycleAndContent(t *testing.T) {
	for _, kind := range []string{"message", "reasoning"} {
		t.Run(kind, func(t *testing.T) {
			typ, idx := "response.output_text.delta", "content_index"
			if kind == "reasoning" {
				typ, idx = "response.reasoning_summary_text.delta", "summary_index"
			}
			body := setup(kind) + part(kind) + event(map[string]any{"type": typ, "sequence_number": 3, "item_id": "item-real", "output_index": 0, idx: 0, "delta": "real output"}) + event(map[string]any{"type": "response.completed", "sequence_number": 4, "response": map[string]any{"id": "resp-real", "usage": map[string]any{"input_tokens": 10, "output_tokens": 2}}})
			cfg := Defaults()
			cfg.Mode = "hint"
			var out bytes.Buffer
			st, err := Transform(context.Background(), smallReader{strings.NewReader(body)}, func(b []byte) error { _, e := out.Write(b); return e }, cfg)
			if err != nil {
				t.Fatal(err)
			}
			if st.Hints != 1 {
				t.Fatalf("expected one early empty delta, got %d", st.Hints)
			}
			es := events(t, out.String())
			if len(es) != 6 {
				t.Fatalf("expected 6 events, got %d", len(es))
			}
			if es[3]["type"] != typ || es[3]["delta"] != "" || es[3]["item_id"] != "item-real" || es[3][idx] != float64(0) {
				t.Fatal("hint lost real part identity", es[3])
			}
			for i, e := range es {
				if e["sequence_number"] != float64(i) {
					t.Fatal("nonmonotonic sequence", es)
				}
			}
			if es[4]["delta"] != "real output" || es[5]["response"].(map[string]any)["usage"].(map[string]any)["output_tokens"] != float64(2) {
				t.Fatal("content or billing changed")
			}
		})
	}
}

func TestHintDoesNotFabricateContextOrDuplicate(t *testing.T) {
	for _, body := range []string{part("message"), setup("message"), setup("message") + event(map[string]any{"type": "response.content_part.added", "sequence_number": 2, "item_id": "other", "output_index": 0, "content_index": 0, "part": map[string]any{"type": "output_text", "text": ""}}), setup("message") + event(map[string]any{"type": "response.output_text.delta", "sequence_number": 2, "delta": "already started"}) + part("message"), setup("message") + "event: response.failed\ndata: {\"type\":\"response.failed\",\"sequence_number\":2,\"response\":{\"error\":{\"code\":\"server_is_overloaded\"}}}\n\n"} {
		cfg := Defaults()
		cfg.Mode = "hint"
		var out bytes.Buffer
		st, err := Transform(context.Background(), strings.NewReader(body), func(b []byte) error { _, e := out.Write(b); return e }, cfg)
		if err != nil || st.Hints != 0 || out.String() != body {
			t.Fatal("noneligible response was modified", err, st)
		}
	}
}

func TestHugeEventsRemainValidAfterHint(t *testing.T) {
	// The top-level sequence occurs after a large string and an identically named nested key.
	large := `data: {"type":"response.output_text.delta","nested":{"sequence_number":99},"delta":"` + strings.Repeat("x", 2*1024*1024) + `","sequence_number":3}` + "\r\n\r\n"
	body := strings.ReplaceAll(setup("message")+part("message"), "\n", "\r\n") + large
	cfg := Defaults()
	cfg.Mode = "hint"
	var out bytes.Buffer
	st, err := Transform(context.Background(), strings.NewReader(body), func(b []byte) error { _, e := out.Write(b); return e }, cfg)
	if err != nil || st.Hints != 1 {
		t.Fatal(err, st)
	}
	es := events(t, out.String())
	last := es[len(es)-1]
	if last["sequence_number"] != float64(4) || last["nested"].(map[string]any)["sequence_number"] != float64(99) || len(last["delta"].(string)) != 2*1024*1024 {
		t.Fatal("large payload changed or sequence not adjusted")
	}
}

func TestOffIsByteExact(t *testing.T) {
	body := setup("message") + part("message") + "data: malformed\n\ndata: [DONE]\n\n"
	var out bytes.Buffer
	st, err := Transform(context.Background(), strings.NewReader(body), func(b []byte) error { _, e := out.Write(b); return e }, Defaults())
	if err != nil || st.Hints != 0 || out.String() != body {
		t.Fatal("off modified bytes", err)
	}
}

func TestMultilineDataAndIncompleteEvent(t *testing.T) {
	body := setup("message") + strings.Replace(part("message"), `,"part":`, ",\ndata: \"part\":", 1)
	cfg := Defaults()
	cfg.Mode = "hint"
	var out bytes.Buffer
	st, err := Transform(context.Background(), strings.NewReader(body), func(b []byte) error { _, e := out.Write(b); return e }, cfg)
	if err != nil || st.Hints != 1 {
		t.Fatal("multiline SSE not handled", err, st)
	}
	events(t, out.String())
	out.Reset()
	body = strings.TrimSuffix(setup("message")+part("message"), "\n\n")
	st, err = Transform(context.Background(), strings.NewReader(body), func(b []byte) error { _, e := out.Write(b); return e }, cfg)
	if err != nil || st.Hints != 0 || out.String() != body {
		t.Fatal("incomplete event injected", err, st)
	}
}

func TestCancellationAndOversizedPreamble(t *testing.T) {
	cfg := Defaults()
	cfg.Mode = "hint"
	cfg.MaxEventBytes = 4096
	body := event(map[string]any{"type": "response.created", "sequence_number": 0, "response": map[string]any{"id": "resp-real", "opaque": strings.Repeat("x", 40000)}}) + part("message")
	var out bytes.Buffer
	st, err := Transform(context.Background(), strings.NewReader(body), func(b []byte) error { _, e := out.Write(b); return e }, cfg)
	if err != nil || st.Hints != 0 || st.OversizedEvents != 1 || out.String() != body {
		t.Fatal("oversized preamble not passed unchanged", err, st)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = Transform(ctx, strings.NewReader(body), func([]byte) error { t.Fatal("emitted after cancellation"); return nil }, cfg)
	if err != context.Canceled {
		t.Fatal(err)
	}
}

func TestDelayAppliesToFirstOutputRatherThanHeaders(t *testing.T) {
	cfg := Defaults()
	cfg.Mode = "delay"
	cfg.DelayMS = 80
	preamble := setup("message") + part("message")
	output := event(map[string]any{"type": "response.output_text.delta", "sequence_number": 3, "delta": "real"})
	r, w := io.Pipe()
	defer r.Close()
	go func() {
		defer w.Close()
		io.WriteString(w, preamble)
		time.Sleep(100 * time.Millisecond)
		io.WriteString(w, output)
	}()
	started := time.Now()
	var out bytes.Buffer
	var preAt, deltaAt time.Duration
	preSeen, deltaSeen := false, false
	st, err := Transform(context.Background(), r, func(b []byte) error {
		out.Write(b)
		if !preSeen && strings.Contains(out.String(), "response.created") {
			preAt = time.Since(started)
			preSeen = true
		}
		if !deltaSeen && strings.Contains(out.String(), `"delta":"real"`) {
			deltaAt = time.Since(started)
			deltaSeen = true
		}
		return nil
	}, cfg)
	if err != nil || st.Delays != 1 || preAt > 70*time.Millisecond || deltaAt < 170*time.Millisecond || out.String() != preamble+output {
		t.Fatal("output delay not applied exactly once", err, st, preAt, deltaAt)
	}
	for _, typ := range []string{"error", "response.failed"} {
		body := preamble + event(map[string]any{"type": typ, "sequence_number": 3, "code": "server_is_overloaded"})
		out.Reset()
		started = time.Now()
		st, err = Transform(context.Background(), strings.NewReader(body), func(b []byte) error { out.Write(b); return nil }, cfg)
		if err != nil || st.Delays != 0 || time.Since(started) > 70*time.Millisecond || out.String() != body {
			t.Fatal("stream failure delayed", err, st)
		}
	}
}

func TestHintEmptyCompletionRemainsFailure(t *testing.T) {
	body := setup("message") + part("message") + event(map[string]any{"type": "response.completed", "sequence_number": 3, "response": map[string]any{"id": "resp-real", "status": "completed", "output": []any{}}})
	cfg := Defaults()
	cfg.Mode = "hint"
	var out bytes.Buffer
	st, err := Transform(context.Background(), strings.NewReader(body), func(b []byte) error { _, e := out.Write(b); return e }, cfg)
	if !errors.Is(err, ErrEmptyCompletion) || st.Hints != 1 || strings.Contains(out.String(), "response.completed") {
		t.Fatal("empty upstream completion became success", err, st)
	}
}

func TestEarlyNullOrZeroUsageDoesNotMakeEmptyCompletionSuccessful(t *testing.T) {
	for _, usage := range []any{nil, map[string]any{"input_tokens": 0, "output_tokens": 0}} {
		created := event(map[string]any{"type": "response.created", "sequence_number": 0, "response": map[string]any{"id": "resp-real", "usage": usage}})
		old := setup("message")
		split := strings.Index(old, "\n\n") + 2
		body := created + old[split:] + part("message") + event(map[string]any{"type": "response.completed", "sequence_number": 3, "response": map[string]any{"id": "resp-real", "status": "completed", "output": []any{}}})
		cfg := Defaults()
		cfg.Mode = "hint"
		_, err := Transform(context.Background(), strings.NewReader(body), func([]byte) error { return nil }, cfg)
		if !errors.Is(err, ErrEmptyCompletion) {
			t.Fatal("early null/zero usage made empty completion successful", usage, err)
		}
	}
}

func TestOversizedNonOutputDoesNotDisableEmptyCompletionGuard(t *testing.T) {
	for _, extra := range []string{
		":" + strings.Repeat(" heartbeat", 40000) + "\n\n",
		strings.Repeat("id: heartbeat\n", 30000) + "\n",
		event(map[string]any{"type": "response.in_progress", "sequence_number": 3, "response": map[string]any{"id": "resp-real", "padding": strings.Repeat("x", 300000)}}),
	} {
		body := setup("message") + part("message") + extra + event(map[string]any{"type": "response.completed", "sequence_number": 4, "response": map[string]any{"id": "resp-real", "output": []any{}}})
		cfg := Defaults()
		cfg.Mode = "hint"
		var out bytes.Buffer
		st, err := Transform(context.Background(), strings.NewReader(body), func(b []byte) error { _, e := out.Write(b); return e }, cfg)
		if !errors.Is(err, ErrEmptyCompletion) || st.Hints != 1 || st.OversizedEvents != 1 || strings.Contains(out.String(), "response.completed") {
			t.Fatal("large structural event disabled empty completion guard", err, st)
		}
	}
}

func TestUsageKeepsHostCompletionSemantics(t *testing.T) {
	for _, tc := range []struct {
		name            string
		early, terminal map[string]any
	}{
		{"early_input", map[string]any{"usage": map[string]any{"input_tokens": 1}}, nil},
		{"early_output", map[string]any{"usage": map[string]any{"output_tokens": 1}}, nil},
		{"early_cache", map[string]any{"usage": map[string]any{"input_tokens_details": map[string]any{"cached_tokens": 1}}}, nil},
		{"early_image", map[string]any{"usage": map[string]any{"output_tokens_details": map[string]any{"image_tokens": 1}}}, nil},
		{"terminal_null", nil, map[string]any{"usage": nil}},
		{"terminal_zero", nil, map[string]any{"usage": map[string]any{"input_tokens": 0, "output_tokens": 0}}},
		{"terminal_error", nil, map[string]any{"error": map[string]any{"code": "upstream_error"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			created := map[string]any{"id": "resp-real"}
			for k, v := range tc.early {
				created[k] = v
			}
			terminal := map[string]any{"id": "resp-real", "output": []any{}}
			for k, v := range tc.terminal {
				terminal[k] = v
			}
			old := setup("message")
			body := event(map[string]any{"type": "response.created", "sequence_number": 0, "response": created}) + old[strings.Index(old, "\n\n")+2:] + part("message") + event(map[string]any{"type": "response.completed", "sequence_number": 3, "response": terminal})
			cfg := Defaults()
			cfg.Mode = "hint"
			var out bytes.Buffer
			st, err := Transform(context.Background(), strings.NewReader(body), func(b []byte) error { _, e := out.Write(b); return e }, cfg)
			if err != nil || st.Hints != 1 || !strings.Contains(out.String(), "response.completed") {
				t.Fatal("host-compatible completion was dropped", err, st)
			}
		})
	}
}

func TestWhitespaceSSEFieldIsNotEventBoundary(t *testing.T) {
	body := setup("message") + part("message") + "data: {\"type\":\"response.output_text.delta\",\n \n\t\ndata: \"sequence_number\":3,\"delta\":\"real\"}\n\n"
	cfg := Defaults()
	cfg.Mode = "hint"
	var out bytes.Buffer
	_, err := Transform(context.Background(), strings.NewReader(body), func(b []byte) error { _, e := out.Write(b); return e }, cfg)
	if err != nil {
		t.Fatal(err)
	}
	es := events(t, out.String())
	if es[len(es)-1]["sequence_number"] != float64(4) {
		t.Fatal("ignored SSE field reset sequence", es)
	}
}
