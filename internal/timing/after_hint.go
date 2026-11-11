package timing

import (
	"bufio"
	"bytes"
	"context"
	"github.com/tidwall/gjson"
	"io"
	"strings"
)

func sseBlank(b []byte) bool { return bytes.Equal(b, []byte("\n")) || bytes.Equal(b, []byte("\r\n")) }

// An empty hint is not proof of upstream success. Until a genuine semantic
// event follows it, inspect bounded events for an empty terminal completion.
// Once real output starts, sequence rewriting streams without event buffering.
func afterHint(ctx context.Context, br *bufio.Reader, emit func([]byte) error, cfg Config, hasUsage bool, stats *StreamStats) error {
	guard, overflow := true, false
	var pending []byte
	var patch sequencePatch
	lineStart, dataLine := true, false
	patchChunk := func(b []byte, start bool) []byte {
		if start {
			dataLine = bytes.HasPrefix(b, []byte("data:"))
		}
		if !dataLine {
			return b
		}
		if start {
			return append(append([]byte(nil), b[:5]...), patch.feed(b[5:])...)
		}
		return patch.feed(b)
	}
	patchPending := func(b []byte) error {
		start := true
		for len(b) > 0 {
			i := bytes.IndexByte(b, '\n')
			var line []byte
			if i >= 0 {
				line, b = b[:i+1], b[i+1:]
			} else {
				line, b = b, nil
			}
			if err := emit(patchChunk(line, start)); err != nil {
				return err
			}
			start = i >= 0
		}
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		frag, err := br.ReadSlice('\n')
		if len(frag) > 0 {
			blank := lineStart && sseBlank(frag) && err != bufio.ErrBufferFull
			if guard && !overflow {
				pending = append(pending, frag...)
				if len(pending) > cfg.MaxEventBytes {
					stats.OversizedEvents++
					overflow = true
					if e := patchPending(pending); e != nil {
						return e
					}
					pending = nil
				} else if blank {
					payload := sseData(pending)
					if payload != "" && gjson.Valid(payload) {
						root := gjson.Parse(payload)
						typ := root.Get("type").String()
						hasUsage = hasUsage || hasPositiveUsage(root)
						if (typ == "response.completed" || typ == "response.done") && !hasUsage && !root.Get("usage").Exists() && !root.Get("response.usage").Exists() && !root.Get("error").Exists() && !root.Get("response.error").Exists() && len(root.Get("response.output").Array()) == 0 {
							return ErrEmptyCompletion
						}
						if !isEmptyStructural(root, typ) {
							guard = false
						}
					} else if payload != "" {
						guard = false
					}
					if e := patchPending(pending); e != nil {
						return e
					}
					pending = nil
				}
			} else {
				if e := emit(patchChunk(frag, lineStart)); e != nil {
					return e
				}
			}
			if blank {
				patch = sequencePatch{}
				// An oversized event is not evidence of real output. Resume
				// bounded inspection at the next event boundary.
				overflow = false
			}
			lineStart = err != bufio.ErrBufferFull
		}
		if err != nil && err != bufio.ErrBufferFull {
			if len(pending) > 0 {
				if e := patchPending(pending); e != nil {
					return e
				}
			}
			if tail := patch.finish(); len(tail) > 0 {
				if e := emit(tail); e != nil {
					return e
				}
			}
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

// Early usage:null and zero-valued placeholders do not prove success. Only
// positive token counts survive across events; the terminal event's own usage
// field is checked separately, matching the host's empty-completion rule.
func hasPositiveUsage(root gjson.Result) bool {
	for _, prefix := range []string{"", "response.", "data.", "data.response."} {
		usage := root.Get(prefix + "usage")
		if !usage.IsObject() {
			continue
		}
		for _, path := range []string{
			"input_tokens", "prompt_tokens", "output_tokens", "completion_tokens",
			"input_tokens_details.cached_tokens", "prompt_tokens_details.cached_tokens",
			"cache_read_input_tokens", "cache_read_tokens", "cached_tokens",
			"cache_creation_input_tokens", "cache_creation_tokens", "cache_write_tokens", "cache_write_input_tokens",
			"input_tokens_details.cache_write_tokens", "prompt_tokens_details.cache_write_tokens",
			"input_tokens_details.cache_creation_tokens", "prompt_tokens_details.cache_creation_tokens",
			"input_tokens_details.image_tokens", "prompt_tokens_details.image_tokens",
			"output_tokens_details.image_tokens", "completion_tokens_details.image_tokens",
		} {
			if usage.Get(path).Int() > 0 {
				return true
			}
		}
		for _, path := range []string{"input_tokens_details.image_tokens", "output_tokens_details.image_tokens"} {
			if root.Get(prefix+"tool_usage.image_gen."+path).Int() > 0 {
				return true
			}
		}
	}
	return false
}

func sseData(raw []byte) string {
	var data []string
	for _, line := range strings.Split(strings.ReplaceAll(string(raw), "\r\n", "\n"), "\n") {
		if strings.HasPrefix(line, "data:") {
			data = append(data, strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		}
	}
	return strings.Join(data, "\n")
}

func isEmptyStructural(root gjson.Result, typ string) bool {
	switch typ {
	case "response.created", "response.in_progress", "keepalive":
		return true
	case "response.content_part.added", "response.reasoning_summary_part.added":
		return root.Get("part.text").String() == "" && (root.Get("part.type").String() == "output_text" || root.Get("part.type").String() == "summary_text")
	case "response.output_item.added":
		item := root.Get("item")
		if item.Get("encrypted_content").String() != "" {
			return false
		}
		if item.Get("type").String() != "message" && item.Get("type").String() != "reasoning" {
			return false
		}
		for _, path := range []string{"content", "summary"} {
			for _, p := range item.Get(path).Array() {
				if p.Get("text").String() != "" {
					return false
				}
			}
		}
		return true
	default:
		return false
	}
}
