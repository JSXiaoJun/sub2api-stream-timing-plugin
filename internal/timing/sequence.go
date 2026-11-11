package timing

import "strconv"

// sequencePatch adds one only to the top-level sequence_number JSON integer.
// It retains at most 20 number bytes and 16 key bytes, never response content.
// State survives ReadSlice fragments and multiline SSE data fields.
type sequencePatch struct {
	depth                                                                      int
	inString, escaped, keyString, expectKey, expectColon, expectNumber, number bool
	key                                                                        []byte
	digits                                                                     []byte
	keyMatched                                                                 bool
}

func (p *sequencePatch) feed(b []byte) []byte {
	out := make([]byte, 0, len(b)+24)
	for _, c := range b {
		if p.number {
			if c >= '0' && c <= '9' && len(p.digits) < 20 {
				p.digits = append(p.digits, c)
				continue
			}
			out = append(out, p.finish()...)
		}
		if p.inString {
			out = append(out, c)
			if p.escaped {
				p.escaped = false
				if p.keyString {
					p.keyMatched = false
				}
				continue
			}
			if c == '\\' {
				p.escaped = true
				if p.keyString {
					p.keyMatched = false
				}
				continue
			}
			if c == '"' {
				p.inString = false
				if p.keyString {
					p.keyMatched = p.keyMatched && string(p.key) == "sequence_number"
					p.expectColon = true
					p.expectKey = false
				}
				p.keyString = false
			} else if p.keyString {
				if len(p.key) < 16 {
					p.key = append(p.key, c)
				} else {
					p.keyMatched = false
				}
			}
			continue
		}
		if p.expectNumber {
			if c == ' ' || c == '\t' || c == '\r' || c == '\n' {
				out = append(out, c)
				continue
			}
			p.expectNumber = false
			if c >= '0' && c <= '9' {
				p.number = true
				p.digits = append(p.digits[:0], c)
				continue
			}
		}
		if p.expectColon {
			if c == ' ' || c == '\t' || c == '\r' || c == '\n' {
				out = append(out, c)
				continue
			}
			p.expectColon = false
			if c == ':' {
				p.expectNumber = p.keyMatched
				out = append(out, c)
				continue
			}
		}
		switch c {
		case '{', '[':
			p.depth++
			if p.depth == 1 && c == '{' {
				p.expectKey = true
			}
		case '}', ']':
			p.depth--
			p.expectKey = false
		case ',':
			if p.depth == 1 {
				p.expectKey = true
			}
		case '"':
			p.inString = true
			p.keyString = p.depth == 1 && p.expectKey
			p.key = p.key[:0]
			p.keyMatched = p.keyString
		}
		out = append(out, c)
	}
	return out
}

func (p *sequencePatch) finish() []byte {
	if !p.number {
		return nil
	}
	p.number = false
	v, err := strconv.ParseUint(string(p.digits), 10, 64)
	if err != nil || v == ^uint64(0) {
		return append([]byte(nil), p.digits...)
	}
	return strconv.AppendUint(nil, v+1, 10)
}
