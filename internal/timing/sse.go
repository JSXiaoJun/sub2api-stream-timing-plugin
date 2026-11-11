package timing

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"github.com/tidwall/gjson"
	"io"
	"strings"
	"time"
)

type StreamStats struct{ Hints, Delays, OversizedEvents, MalformedEvents int }

var ErrEmptyCompletion = errors.New("empty completion after stream hint")

// Transform inserts at most one empty delta AFTER a real, empty content part.
// Before insertion events are buffered within a fixed bound. After insertion
// the stream is patched incrementally, including arbitrarily large image lines.
func Transform(ctx context.Context, r io.Reader, emit func([]byte) error, cfg Config) (StreamStats, error) {
	return transformWithRelease(ctx, r, emit, cfg, func() {})
}

// releaseSlot only releases admission for the initial transform. Streaming,
// sequence rewriting and the post-hint failure guard remain alive afterwards.
func transformWithRelease(ctx context.Context, r io.Reader, emit func([]byte) error, cfg Config, releaseSlot func()) (StreamStats, error) {
	var st StreamStats
	if cfg.Mode == "off" {
		return st, copyStream(ctx, r, emit)
	}
	br := bufio.NewReaderSize(r, 32768)
	var pending []byte
	tracker := newHintTracker()
	overflow, lineStart := false, true
	for {
		if err := ctx.Err(); err != nil {
			return st, err
		}
		frag, err := br.ReadSlice('\n')
		if len(frag) > 0 {
			blank := lineStart && sseBlank(frag) && err != bufio.ErrBufferFull
			if overflow {
				if e := emit(frag); e != nil {
					return st, e
				}
				if blank {
					overflow = false
				}
			} else if len(pending)+len(frag) > cfg.MaxEventBytes {
				st.OversizedEvents++
				tracker.disabled = true
				releaseSlot()
				if len(pending) > 0 {
					if e := emit(pending); e != nil {
						return st, e
					}
				}
				pending = nil
				if e := emit(frag); e != nil {
					return st, e
				}
				overflow = !blank
			} else {
				pending = append(pending, frag...)
				if blank {
					var hint []byte
					var malformed bool
					if cfg.Mode == "hint" {
						hint, malformed = tracker.observe(pending)
					} else if st.Delays == 0 && !tracker.disabled && delayCandidate(pending) {
						if e := waitDelay(ctx, cfg.DelayMS); e != nil {
							return st, e
						}
						st.Delays++
						tracker.disabled = true
					}
					if malformed {
						st.MalformedEvents++
					}
					if e := emit(pending); e != nil {
						return st, e
					}
					pending = pending[:0]
					if hint != nil {
						if e := emit(hint); e != nil {
							return st, e
						}
						st.Hints++
						releaseSlot()
						err := afterHint(ctx, br, emit, cfg, tracker.hasUsage, &st)
						return st, err
					}
					if tracker.disabled {
						releaseSlot()
						// Nothing further can be transformed. Release buffers and
						// forward subsequent bytes with normal streaming latency.
						pending = nil
						return st, copyStream(ctx, br, emit)
					}
				}
			}
			lineStart = err != bufio.ErrBufferFull
		}
		if err != nil && err != bufio.ErrBufferFull {
			if len(pending) > 0 {
				if e := emit(pending); e != nil {
					return st, e
				}
			}
			if err == io.EOF {
				return st, nil
			}
			return st, err
		}
	}
}

func delayCandidate(raw []byte) bool {
	var data []string
	for _, line := range strings.Split(strings.ReplaceAll(string(raw), "\r\n", "\n"), "\n") {
		if strings.HasPrefix(line, "data:") {
			data = append(data, strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		}
	}
	payload := strings.Join(data, "\n")
	if !gjson.Valid(payload) {
		return false
	}
	root := gjson.Parse(payload)
	typ := root.Get("type").String()
	switch typ {
	case "response.created", "response.in_progress", "keepalive", "error", "response.failed":
		return false
	case "response.output_item.added":
		item := root.Get("item")
		switch item.Get("type").String() {
		case "message", "reasoning":
			for _, k := range []string{"content", "summary"} {
				for _, p := range item.Get(k).Array() {
					if p.Get("text").String() != "" {
						return true
					}
				}
			}
			return item.Get("encrypted_content").String() != ""
		case "function_call":
			return item.Get("arguments").String() != ""
		case "custom_tool_call":
			return item.Get("input").String() != ""
		default:
			return true
		}
	case "response.content_part.added", "response.reasoning_summary_part.added":
		return root.Get("part.text").String() != "" || (root.Get("part.type").String() != "output_text" && root.Get("part.type").String() != "summary_text")
	default:
		return typ != ""
	}
}

func copyStream(ctx context.Context, r io.Reader, emit func([]byte) error) error {
	b := make([]byte, 32768)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := r.Read(b)
		if n > 0 {
			if e := emit(b[:n]); e != nil {
				return e
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func waitDelay(ctx context.Context, ms int) error {
	if ms == 0 {
		return ctx.Err()
	}
	t := time.NewTimer(time.Duration(ms) * time.Millisecond)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type hintItem struct{ id, kind string }
type hintTracker struct {
	responseID string
	items      map[int64]hintItem
	disabled   bool
	lastSeq    int64
	hasUsage   bool
}

func newHintTracker() *hintTracker { return &hintTracker{items: make(map[int64]hintItem), lastSeq: -1} }

func (t *hintTracker) observe(raw []byte) ([]byte, bool) {
	if t.disabled {
		return nil, false
	}
	var data []string
	var named string
	for _, line := range strings.Split(strings.ReplaceAll(string(raw), "\r\n", "\n"), "\n") {
		if strings.HasPrefix(line, "event:") {
			named = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		}
		if strings.HasPrefix(line, "data:") {
			data = append(data, strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		}
	}
	if len(data) == 0 {
		return nil, false
	}
	payload := strings.Join(data, "\n")
	if payload == "[DONE]" {
		t.disabled = true
		return nil, false
	}
	if !gjson.Valid(payload) {
		t.disabled = true
		return nil, true
	}
	root := gjson.Parse(payload)
	t.hasUsage = t.hasUsage || hasPositiveUsage(root)
	typ := root.Get("type").String()
	if named != "" && named != typ {
		t.disabled = true
		return nil, true
	}
	seq := root.Get("sequence_number")
	if seq.Type != gjson.Number || seq.Int() < 0 || seq.Int() <= t.lastSeq || seq.Int() > 9007199254740990 || seq.Float() != float64(seq.Int()) {
		t.disabled = true
		return nil, false
	}
	t.lastSeq = seq.Int()
	switch typ {
	case "response.created", "response.in_progress":
		id := root.Get("response.id").String()
		if id != "" {
			if t.responseID != "" && id != t.responseID {
				t.disabled = true
			}
			t.responseID = id
		}
	case "response.output_item.added":
		item := root.Get("item")
		kind := item.Get("type").String()
		id := item.Get("id").String()
		idx := root.Get("output_index")
		if len(t.items) >= 16 || id == "" || idx.Type != gjson.Number || idx.Int() < 0 || idx.Float() != float64(idx.Int()) {
			t.disabled = true
			break
		}
		if kind != "message" && kind != "reasoning" {
			t.disabled = true
			break
		}
		for _, key := range []string{"content", "summary"} {
			for _, p := range item.Get(key).Array() {
				if p.Get("text").String() != "" {
					t.disabled = true
				}
			}
		}
		if item.Get("encrypted_content").String() != "" {
			t.disabled = true
		}
		t.items[idx.Int()] = hintItem{id: id, kind: kind}
	case "response.content_part.added", "response.reasoning_summary_part.added":
		idx := root.Get("output_index")
		item, ok := t.items[idx.Int()]
		indexKey, partType, deltaType, kind := "content_index", "output_text", "response.output_text.delta", "message"
		if typ == "response.reasoning_summary_part.added" {
			indexKey, partType, deltaType, kind = "summary_index", "summary_text", "response.reasoning_summary_text.delta", "reasoning"
		}
		partIndex := root.Get(indexKey)
		part := root.Get("part")
		if !ok || t.responseID == "" || item.id != root.Get("item_id").String() || item.kind != kind || part.Get("type").String() != partType || part.Get("text").Type != gjson.String || part.Get("text").String() != "" || idx.Type != gjson.Number || idx.Float() != float64(idx.Int()) || partIndex.Type != gjson.Number || partIndex.Int() < 0 || partIndex.Float() != float64(partIndex.Int()) {
			t.disabled = true
			break
		}
		m := map[string]any{"type": deltaType, "sequence_number": seq.Int() + 1, "item_id": item.id, "output_index": idx.Int(), indexKey: partIndex.Int(), "delta": ""}
		b, _ := json.Marshal(m)
		return append(append([]byte("event: "+deltaType+"\ndata: "), b...), []byte("\n\n")...), false
	default:
		// Any other semantic event, error or terminal status ends eligibility.
		t.disabled = true
	}
	return nil, false
}
