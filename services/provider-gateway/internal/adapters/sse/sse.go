// Package sse is a minimal Server-Sent Events line scanner, shared by the
// three real provider adapters (openai, anthropic, google) — all three
// stream their responses as SSE (OpenAI and Google natively; Google via
// its documented ?alt=sse REST variant), unlike provider-fake's plain
// newline-delimited JSON (Step D1). Each adapter still owns its own
// per-event JSON schema — this package only handles the SSE framing
// (event:/data: lines, blank-line-terminated frames), not payload shape.
package sse

import (
	"bufio"
	"io"
	"strings"
)

// Event is one decoded SSE frame. Name is empty for providers that don't
// use named events (OpenAI, Google); Anthropic sets it (message_start,
// content_block_delta, message_delta, ...).
type Event struct {
	Name string
	Data string
}

// Scanner reads SSE frames off a streamed HTTP response body.
type Scanner struct {
	sc *bufio.Scanner
}

// NewScanner wraps r. Buffer size is generous (1MB max token) since a
// single data: line carries one full provider response chunk as JSON.
func NewScanner(r io.Reader) *Scanner {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	return &Scanner{sc: sc}
}

// Next returns the next SSE event, or io.EOF once the stream ends cleanly
// (no more frames, no scanner error).
func (s *Scanner) Next() (Event, error) {
	var ev Event
	var dataLines []string
	sawAny := false

	for s.sc.Scan() {
		line := s.sc.Text()
		if line == "" {
			if sawAny {
				ev.Data = strings.Join(dataLines, "\n")
				return ev, nil
			}
			continue
		}
		sawAny = true
		switch {
		case strings.HasPrefix(line, "event:"):
			ev.Name = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			dataLines = append(dataLines, strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		default:
			// SSE comment lines (":...") and any other field (id:,
			// retry:) this gateway doesn't need — ignored.
		}
	}
	if err := s.sc.Err(); err != nil {
		return Event{}, err
	}
	if sawAny {
		// Stream ended without a trailing blank line — still a
		// complete frame (some servers omit the final separator).
		ev.Data = strings.Join(dataLines, "\n")
		return ev, nil
	}
	return Event{}, io.EOF
}
