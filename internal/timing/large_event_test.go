package timing

import (
	"bytes"
	"strings"
	"testing"
)

func TestTimedLargeRealDeltaKeepsCompletion(t *testing.T) {
	for _, named := range []bool{false, true} {
		for _, fragment := range []int{1, 137, 32768} {
			p := outputProbe{limit: 4096}
			var out bytes.Buffer
			emit := func(b []byte) error { _, e := out.Write(b); return e }
			pre := ""
			if named {
				pre = "event: response.output_text.delta\n"
			}
			// Deliberately put type last; quoted text and a nested type must not fool it.
			raw := pre + "data: {\"delta\":\"" + strings.Repeat("xx\\\"\\\\", 1500) + "\",\"other\":{\"type\":\"response.created\"},\"type\":\"response.output_text.delta\"}\n\n"
			for start := 0; start < len(raw); start += fragment {
				end := min(start+fragment, len(raw))
				if err := p.feed([]byte(raw[start:end]), true, emit); err != nil {
					t.Fatal(err)
				}
			}
			terminal := event(map[string]any{"type": "response.completed", "response": map[string]any{"usage": map[string]any{"input_tokens": 0, "output_tokens": 0}, "output": []any{}}})
			if err := p.feed([]byte(terminal), true, emit); err != nil {
				t.Fatal("real output misclassified as empty", named, fragment, err)
			}
			if out.String() != raw+terminal || !p.raw || p.oversized != 1 {
				t.Fatal("large event changed", named, fragment)
			}
		}
	}
}

func TestTimedLargeMetadataKeepsEmptyGuard(t *testing.T) {
	for _, kind := range []string{"response.created", "response.in_progress"} {
		p := outputProbe{limit: 4096}
		var out bytes.Buffer
		emit := func(b []byte) error { _, e := out.Write(b); return e }
		raw := "data: {\"metadata\":\"" + strings.Repeat("x", 6000) + "\",\"nested\":{\"type\":\"response.output_text.delta\"},\"type\":\"" + kind + "\"}\n\n"
		if err := p.feed([]byte(raw), true, emit); err != nil {
			t.Fatal(err)
		}
		terminal := event(map[string]any{"type": "response.completed", "response": map[string]any{"usage": nil, "output": []any{}}})
		if err := p.feed([]byte(terminal), true, emit); err != ErrEmptyCompletion {
			t.Fatal("metadata accepted as output", err)
		}
	}
}

func TestTimedLargeAddedItemOutput(t *testing.T) {
	for _, item := range []map[string]any{
		{"type": "message", "content": []any{map[string]any{"type": "output_text", "text": strings.Repeat("x", 6000)}}},
		{"type": "function_call", "arguments": strings.Repeat("x", 6000)},
	} {
		p := outputProbe{limit: 4096}
		emit := func([]byte) error { return nil }
		if e := p.feed([]byte(event(map[string]any{"type": "response.output_item.added", "item": item})), true, emit); e != nil {
			t.Fatal(e)
		}
		if !p.raw {
			t.Fatal("large added output lost")
		}
	}
}

func TestTimedLargeTerminal(t *testing.T) {
	for _, kind := range []string{"usage", "output", "failed", "empty"} {
		response := map[string]any{"padding": strings.Repeat("x", 7000)}
		typ := "response.completed"
		switch kind {
		case "usage":
			response["usage"] = map[string]any{"input_tokens": 10}
		case "output":
			response["output"] = []any{map[string]any{"type": "message", "content": []any{map[string]any{"text": "real"}}}}
		case "failed":
			typ = "response.failed"
			response["error"] = map[string]any{"code": "fixture"}
		}
		raw := event(map[string]any{"type": typ, "response": response})
		p := outputProbe{limit: 4096}
		var out bytes.Buffer
		emit := func(b []byte) error { _, e := out.Write(b); return e }
		var err error
		for start := 0; start < len(raw); start += 257 {
			end := min(start+257, len(raw))
			err = p.feed([]byte(raw[start:end]), true, emit)
			if err != nil {
				break
			}
		}
		if kind == "empty" {
			if err != ErrEmptyCompletion || out.String() == raw {
				t.Fatal("oversized empty terminal leaked success", err)
			}
			continue
		}
		if err != nil || p.finish(true, emit) != nil || !p.terminal || out.String() != raw {
			t.Fatal("large terminal corrupted", kind, err, p.terminal)
		}
	}
}

func TestTimedLargeEmptyTerminalSplitBeforeNewline(t *testing.T) {
	raw := strings.TrimRight(event(map[string]any{"type": "response.completed", "response": map[string]any{"padding": strings.Repeat("x", 6000), "output": []any{}, "usage": nil}}), "\n")
	p := outputProbe{limit: 4096}
	var out bytes.Buffer
	emit := func(b []byte) error { _, e := out.Write(b); return e }
	if e := p.feed([]byte(raw), true, emit); e != ErrEmptyCompletion || out.Len() != 0 {
		t.Fatal("complete empty JSON escaped before newline", e, out.Len())
	}
}

func TestTimedLargeEarlyUsageKeepsCompletion(t *testing.T) {
	p := outputProbe{limit: 4096}
	emit := func([]byte) error { return nil }
	pre := event(map[string]any{"type": "response.created", "response": map[string]any{"padding": strings.Repeat("x", 6000), "usage": map[string]any{"input_tokens": 10}}})
	if e := p.feed([]byte(pre), true, emit); e != nil {
		t.Fatal(e)
	}
	terminal := event(map[string]any{"type": "response.completed", "response": map[string]any{"output": []any{}, "usage": nil}})
	if e := p.feed([]byte(terminal), true, emit); e != nil || !p.hasUsage {
		t.Fatal("early usage lost", e)
	}
}

func TestTimedLargeMetadataIsNotCompletionEvidence(t *testing.T) {
	for _, metadata := range []map[string]any{{"error": map[string]any{"code": "historical"}}, {"usage": map[string]any{"input_tokens": 10}}} {
		p := outputProbe{limit: 4096}
		var out bytes.Buffer
		emit := func(b []byte) error { _, e := out.Write(b); return e }
		raw := event(map[string]any{"type": "response.completed", "response": map[string]any{"padding": strings.Repeat("x", 6000), "metadata": metadata, "output": []any{}, "usage": nil}})
		if e := p.feed([]byte(raw), true, emit); e != ErrEmptyCompletion || out.Len() != 0 {
			t.Fatal("metadata confused with completion evidence", e)
		}
	}
}
