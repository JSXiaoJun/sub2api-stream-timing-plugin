package timing

import (
	"encoding/json"
	"strconv"
	"strings"
)

// A bounded lexer records an oversized event's type without retaining its
// strings. Data-only SSE may put type AFTER a multi-MB delta. Original bytes
// are forwarded separately and never rewritten.
type largeEventProbe struct {
	field, eventName, jsonType                                       string
	linePrefix, token                                                []byte
	lineValue, inString, escaped, overflow, expectValue, valueString bool
	depth, stringDepth                                               int
	key, candidate, stringKey                                        string
	containers                                                       [16]string
	itemOutput, partOutput                                           bool
	hasUsage, responseOutput, errorObject                            bool
	number                                                           []byte
	root                                                             bool
}

func (p *largeEventProbe) feed(b []byte) {
	for _, c := range b {
		if c == '\n' {
			if p.field == "data" {
				p.jsonByte('\n')
			}
			p.field = ""
			p.linePrefix = nil
			p.lineValue = false
			continue
		}
		if !p.lineValue {
			if c == ':' {
				p.field = string(p.linePrefix)
				p.lineValue = true
				continue
			}
			if len(p.linePrefix) < 8 {
				p.linePrefix = append(p.linePrefix, c)
			}
			continue
		}
		switch p.field {
		case "data":
			p.jsonByte(c)
		case "event":
			if len(p.eventName) < 160 {
				p.eventName += string(c)
			}
		}
	}
}

func (p *largeEventProbe) jsonByte(c byte) {
	if len(p.number) > 0 {
		if (c >= '0' && c <= '9') || c == '.' || c == 'e' || c == 'E' || c == '+' || c == '-' {
			if len(p.number) < 64 {
				p.number = append(p.number, c)
			}
			return
		}
		if v, e := strconv.ParseFloat(string(p.number), 64); e == nil && v > 0 {
			if p.depth >= 2 && p.depth < len(p.containers) {
				p.hasUsage = p.hasUsage || realUsagePath(strings.Join(p.containers[2:p.depth+1], ".")+"."+p.key)
			}
		}
		p.number = nil
	}
	if p.inString {
		if len(p.token) < 160 {
			p.token = append(p.token, c)
		} else {
			p.overflow = true
		}
		if p.escaped {
			p.escaped = false
			return
		}
		if c == '\\' {
			p.escaped = true
			return
		}
		if c != '"' {
			return
		}
		p.inString = false
		value := ""
		if !p.overflow {
			json.Unmarshal(p.token, &value)
		}
		if p.valueString && p.stringDepth == 1 && p.stringKey == "type" {
			p.jsonType = value
		}
		if p.valueString && (p.overflow || value != "") {
			if p.stringDepth == 2 && p.containers[2] == "item" && (p.stringKey == "arguments" || p.stringKey == "input" || p.stringKey == "encrypted_content") {
				p.itemOutput = true
			}
			if p.stringDepth == 4 && p.containers[2] == "item" && (p.containers[3] == "content" || p.containers[3] == "summary") && p.stringKey == "text" {
				p.itemOutput = true
			}
			if p.stringDepth == 2 && p.containers[2] == "part" && p.stringKey == "text" {
				p.partOutput = true
			}
		}
		p.candidate = value
		return
	}
	switch c {
	case '{', '[':
		p.depth++
		if p.depth == 1 {
			p.root = true
		}
		if p.depth == 4 && p.containers[2] == "response" && p.containers[3] == "output" {
			p.responseOutput = true
		}
		if p.expectValue && p.key == "error" && c == '{' && (p.depth == 2 || (p.depth == 3 && p.containers[2] == "response")) {
			p.errorObject = true
		}
		if p.depth < len(p.containers) && p.depth >= 0 {
			p.containers[p.depth] = ""
			if p.expectValue {
				p.containers[p.depth] = p.key
			}
		}
		p.expectValue = false
	case '}', ']':
		p.depth--
		p.expectValue = false
	case ':':
		p.key = p.candidate
		p.expectValue = true
	case ',':
		p.expectValue = false
	case '"':
		p.inString = true
		p.escaped = false
		p.overflow = false
		p.token = append(p.token[:0], c)
		p.valueString = p.expectValue
		p.stringKey = p.key
		p.stringDepth = p.depth
		p.expectValue = false
	default:
		if p.expectValue && (c == '-' || (c >= '0' && c <= '9')) {
			p.number = append(p.number, c)
			p.expectValue = false
		}
	}
}

func realUsagePath(path string) bool {
	for _, prefix := range []string{"data.response.", "data.", "response."} {
		if strings.HasPrefix(path, prefix) {
			path = strings.TrimPrefix(path, prefix)
			break
		}
	}
	switch path {
	case "usage.input_tokens", "usage.prompt_tokens", "usage.output_tokens", "usage.completion_tokens",
		"usage.input_tokens_details.cached_tokens", "usage.prompt_tokens_details.cached_tokens",
		"usage.cache_read_input_tokens", "usage.cache_read_tokens", "usage.cached_tokens",
		"usage.cache_creation_input_tokens", "usage.cache_creation_tokens", "usage.cache_write_tokens", "usage.cache_write_input_tokens",
		"usage.input_tokens_details.cache_write_tokens", "usage.prompt_tokens_details.cache_write_tokens",
		"usage.input_tokens_details.cache_creation_tokens", "usage.prompt_tokens_details.cache_creation_tokens",
		"usage.input_tokens_details.image_tokens", "usage.prompt_tokens_details.image_tokens",
		"usage.output_tokens_details.image_tokens", "usage.completion_tokens_details.image_tokens",
		"tool_usage.image_gen.input_tokens_details.image_tokens", "tool_usage.image_gen.output_tokens_details.image_tokens":
		return true
	}
	return false
}

func (p *largeEventProbe) typ() string {
	typ := p.jsonType
	if typ == "" {
		typ = strings.TrimSpace(p.eventName)
	}
	return typ
}

func (p *largeEventProbe) terminal() bool {
	if !p.root || p.depth != 0 || p.inString {
		return false
	}
	switch p.typ() {
	case "response.completed", "response.done", "response.failed", "response.incomplete", "error":
		return true
	}
	return false
}

func (p *largeEventProbe) emptyTerminal() bool {
	if !p.root {
		return false
	}
	typ := p.typ()
	return (typ == "response.completed" || typ == "response.done") && !p.hasUsage && !p.responseOutput && !p.errorObject
}

func (p *largeEventProbe) output() bool {
	if !p.root || p.depth != 0 || p.inString {
		return false
	}
	typ := p.typ()
	// These event kinds themselves start semantic output in both host handlers.
	// Oversized metadata, comments and empty added items keep the empty guard.
	if typ == "response.output_item.added" {
		return p.itemOutput
	}
	if typ == "response.content_part.added" || typ == "response.reasoning_summary_part.added" {
		return p.partOutput
	}
	return strings.HasPrefix(typ, "response.") && (strings.HasSuffix(typ, ".delta") || (strings.HasSuffix(typ, ".done") && typ != "response.done"))
}
