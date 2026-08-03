package sse

import (
	"io"
	"strings"
	"testing"
)

func TestScannerParsesNamedAndUnnamedEvents(t *testing.T) {
	raw := "data: {\"a\":1}\n\n" +
		"event: content_block_delta\ndata: {\"b\":2}\n\n" +
		": this is a comment, ignored\n" +
		"data: {\"c\":3}\n\n"

	sc := NewScanner(strings.NewReader(raw))

	want := []Event{
		{Name: "", Data: `{"a":1}`},
		{Name: "content_block_delta", Data: `{"b":2}`},
		{Name: "", Data: `{"c":3}`},
	}
	for i, w := range want {
		ev, err := sc.Next()
		if err != nil {
			t.Fatalf("event %d: unexpected error: %v", i, err)
		}
		if ev != w {
			t.Errorf("event %d = %+v, want %+v", i, ev, w)
		}
	}
	if _, err := sc.Next(); err != io.EOF {
		t.Errorf("final Next() error = %v, want io.EOF", err)
	}
}

func TestScannerHandlesStreamEndingWithoutTrailingBlankLine(t *testing.T) {
	raw := "data: {\"only\":true}"
	sc := NewScanner(strings.NewReader(raw))

	ev, err := sc.Next()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.Data != `{"only":true}` {
		t.Errorf("Data = %q, want %q", ev.Data, `{"only":true}`)
	}
	if _, err := sc.Next(); err != io.EOF {
		t.Errorf("final Next() error = %v, want io.EOF", err)
	}
}

func TestScannerOnEmptyStreamReturnsEOFImmediately(t *testing.T) {
	sc := NewScanner(strings.NewReader(""))
	if _, err := sc.Next(); err != io.EOF {
		t.Errorf("Next() on empty stream = %v, want io.EOF", err)
	}
}
