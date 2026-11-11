package timing

import (
	"bufio"
	"bytes"
	"github.com/tidwall/gjson"
	"io"
	"strings"
)

// Only bounded prefixes are inspected. The asynchronous timed path calls this
// without blocking so a stalled prefix cannot prevent the timer from firing.
func wireFormat(prefix []byte, eof bool) byte {
	p := bytes.TrimLeft(prefix, " \t\r\n")
	if len(p) > 0 && p[0] == 0xef {
		if len(p) < 3 && !eof {
			return 0
		}
		p = bytes.TrimLeft(bytes.TrimPrefix(p, []byte{0xef, 0xbb, 0xbf}), "\r\n")
	}
	if len(p) > 0 && (p[0] == '{' || p[0] == '[') {
		return 'j'
	}
	for _, field := range []string{"data:", "event:", ":", "id:", "retry:"} {
		if bytes.HasPrefix(p, []byte(field)) {
			return 's'
		}
	}
	if len(p) >= 16 || len(prefix) >= 512 || eof {
		return 'u'
	}
	return 0
}

func sniffReader(r io.Reader) (io.Reader, byte, error) {
	br := bufio.NewReaderSize(r, 4096)
	prefix := make([]byte, 0, 512)
	for {
		b, err := br.ReadByte()
		if err == nil {
			prefix = append(prefix, b)
		}
		if err != nil && err != io.EOF {
			return nil, 0, err
		}
		if format := wireFormat(prefix, err == io.EOF); format != 0 {
			return io.MultiReader(bytes.NewReader(prefix), br), format, nil
		}
	}
}

// Match the host's output staging boundary, including empty tool-call items.
func quietEvent(root gjson.Result, typ string) bool {
	if typ == "response.output_item.added" {
		item := root.Get("item")
		switch item.Get("type").String() {
		case "function_call":
			return item.Get("arguments").String() == ""
		case "custom_tool_call":
			return item.Get("input").String() == ""
		case "compaction":
			return item.Get("encrypted_content").String() == ""
		}
	}
	return isEmptyStructural(root, typ)
}

type outputProbe struct {
	limit                             int
	pending                           []byte
	lineBytes                         int
	firstCR                           bool
	overflow, raw, hasUsage, terminal bool
	stopHint                          bool
	oversized, malformed              uint64
	large                             largeEventProbe
}

func (p *outputProbe) feed(data []byte, hinted bool, emit func([]byte) error) error {
	if p.raw {
		return emit(data)
	}
	for len(data) > 0 {
		i := bytes.IndexByte(data, '\n')
		n := len(data)
		if i >= 0 {
			n = i + 1
		}
		frag := data[:n]
		data = data[n:]
		if p.lineBytes == 0 {
			p.firstCR = frag[0] == '\r'
		}
		p.lineBytes += len(frag)
		blank := i >= 0 && (p.lineBytes == 1 || (p.lineBytes == 2 && p.firstCR))
		if i >= 0 {
			p.lineBytes = 0
			p.firstCR = false
		}
		if p.overflow {
			p.large.feed(frag)
			if hinted && p.large.emptyTerminal() && !p.hasUsage && p.large.depth == 0 && !p.large.inString {
				return ErrEmptyCompletion
			}
			if err := emit(frag); err != nil {
				return err
			}
		} else {
			p.pending = append(p.pending, frag...)
			if len(p.pending) > p.limit {
				p.oversized++
				p.stopHint = true
				p.overflow = true
				p.large = largeEventProbe{}
				p.large.feed(p.pending)
				if hinted && p.large.emptyTerminal() && !p.hasUsage && p.large.depth == 0 && !p.large.inString {
					return ErrEmptyCompletion
				}
				if err := emit(p.pending); err != nil {
					return err
				}
				p.pending = nil
			} else if blank {
				payload := sseData(p.pending)
				if payload != "" && payload != "[DONE]" {
					if !gjson.Valid(payload) {
						p.malformed++
						p.stopHint = true
					} else {
						root := gjson.Parse(payload)
						typ := root.Get("type").String()
						if typ == "" {
							for _, line := range bytes.Split(p.pending, []byte("\n")) {
								if bytes.HasPrefix(line, []byte("event:")) {
									typ = strings.TrimSpace(string(line[6:]))
								}
							}
						}
						p.hasUsage = p.hasUsage || hasPositiveUsage(root)
						terminal := typ == "response.completed" || typ == "response.done"
						if hinted && terminal && !p.hasUsage && !root.Get("error").IsObject() && !root.Get("response.error").IsObject() && len(root.Get("response.output").Array()) == 0 {
							return ErrEmptyCompletion
						}
						if !quietEvent(root, typ) {
							p.stopHint = true
							p.raw = true
						}
						if terminal || typ == "response.failed" || typ == "response.incomplete" || typ == "error" {
							p.terminal = true
						}
					}
				} else if payload == "[DONE]" {
					p.stopHint = true
					if hinted && !p.terminal {
						return ErrEmptyCompletion
					}
					p.terminal = true
					p.raw = true
				}
				if err := emit(p.pending); err != nil {
					return err
				}
				p.pending = nil
			}
		}
		if blank {
			if p.overflow {
				p.hasUsage = p.hasUsage || p.large.hasUsage
			}
			if p.overflow && p.large.terminal() {
				p.terminal = true
				p.raw = true
			}
			if p.overflow && p.large.output() {
				p.raw = true
			}
			p.overflow = false
		}
		if p.raw && len(data) > 0 {
			return emit(data)
		}
	}
	return nil
}

func (p *outputProbe) finish(hinted bool, emit func([]byte) error) error {
	if p.overflow && p.large.terminal() {
		p.terminal = true
		p.raw = true
	}
	// Buffered incomplete bytes have not reached the caller. Drop them so a
	// failure terminal can start at an event boundary. Overflow/raw bytes must
	// instead end with a transport error, handled by forwardTimed.
	if hinted && !p.raw && !p.terminal {
		p.pending = nil
		return io.ErrUnexpectedEOF
	}
	if len(p.pending) > 0 {
		if err := emit(p.pending); err != nil {
			return err
		}
		p.pending = nil
	}
	return nil
}

func isEventStream(contentType string) bool {
	return strings.EqualFold(strings.TrimSpace(strings.SplitN(contentType, ";", 2)[0]), "text/event-stream")
}
