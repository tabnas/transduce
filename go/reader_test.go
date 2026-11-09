// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

// ParserEventSourceFromReader (admin ADR-30 and ADR-33): the rule passes
// of a source the parser reads from a reader, against the same text read
// whole. The port of rs/tests/reader_test.rs. Every committed fixture of a
// grammar the harness builds is handed over a few bytes at a time. JSON's,
// JSONC's and JSON Lines' grammars, whose matchers are the engine's own,
// are given lex.window and are read a window at a time; every other
// grammar reads the reader whole first. The passes, the value and the
// outcome must be the same, and a reader that fails, or bytes that are not
// UTF-8, are INPUT_INVALID with the engine's unreadable. The pruning, the
// abort flag, the limits and the metrics hold from a reader as they do
// over a text.

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	tabnas "github.com/tabnas/parser/go"
)

// pieces is a reader that hands its bytes over at most size at a time.
type pieces struct {
	bytes []byte
	size  int
}

func (p *pieces) Read(buf []byte) (int, error) {
	if len(p.bytes) == 0 {
		return 0, io.EOF
	}
	n := min(p.size, len(buf), len(p.bytes))
	copy(buf, p.bytes[:n])
	p.bytes = p.bytes[n:]
	return n, nil
}

// failing is a reader that hands over good bytes, a few at a time, and
// then fails.
type failing struct {
	bytes    []byte
	at, good int
}

func (f *failing) Read(buf []byte) (int, error) {
	if f.at >= f.good {
		return 0, errors.New("the disk went away")
	}
	n := min(3, len(buf), f.good-f.at)
	copy(buf, f.bytes[f.at:f.at+n])
	f.at += n
	return n, nil
}

// abortingRecorder records the passes and raises abort once it has seen
// at of them.
type abortingRecorder struct {
	passRecorder
	abort *AbortFlag
	at    int
}

func (r *abortingRecorder) ParserEvent(ev ParserEvent) (Flow, *Fail) {
	flow, f := r.passRecorder.ParserEvent(ev)
	if len(r.seen) == r.at {
		r.abort.Abort()
	}
	return flow, f
}

// readerGrammar is the grammar a fixture's extension names, and whether
// its matchers are the engine's own, so that it may be given lex.window.
func readerGrammar(file string) (string, bool, bool) {
	switch ext := strings.TrimPrefix(filepath.Ext(file), "."); ext {
	case "json", "jsonc", "jsonl":
		return ext, true, true
	case "json5", "jsonic", "yaml", "zon", "csv", "toml", "ini", "xml":
		return ext, false, true
	case "md":
		return "markdown", false, true
	case "rss":
		return "feed", false, true
	}
	return "", false, false
}

var readerWindowOn = true

func readerParser(name string, window bool) *tabnas.Tabnas {
	j := makeGrammar(name)
	if window {
		j.SetOptions(tabnas.Options{Lex: &tabnas.LexOptions{Window: &readerWindowOn}})
	}
	return j
}

// readerShown is what a run showed: its outcome, its passes in the
// fixture encoding and the value it returned.
type readerShown struct {
	outcome string
	passes  []string
	value   any
}

func readerRun(source *ParserEventSource, rec ParserSink, seen func() []ParserEvent) readerShown {
	flow, f, value := source.RunWithValue(rec)
	outcome := "ok " + flow.String()
	if f != nil {
		limit := ""
		if f.Limit != nil {
			limit = f.Limit.Name
		}
		outcome = fmt.Sprintf("%s %s %d %d %s", f.Code, f.Message, f.Row, f.Column, limit)
	}
	passes := []string{}
	for _, ev := range seen() {
		passes = append(passes, ev.JSON())
	}
	return readerShown{outcome, passes, value}
}

func readerRecorded(source *ParserEventSource) readerShown {
	var rec passRecorder
	return readerRun(source, &rec, func() []ParserEvent { return rec.seen })
}

func TestEveryFixtureStreamsTheSameFromAReader(t *testing.T) {
	dir := filepath.Join("..", "rs", "tests", "fixtures")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("the Rust fixtures: %v", err)
	}
	var files []string
	for _, e := range entries {
		files = append(files, e.Name())
	}
	sort.Strings(files)
	compared := 0
	for _, file := range files {
		name, window, ok := readerGrammar(file)
		if !ok {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, file))
		if err != nil {
			t.Fatal(err)
		}
		text := string(b)
		whole := readerRecorded(NewParserEventSource(readerParser(name, window), text))
		for _, size := range []int{1, 3, 64} {
			read := readerRecorded(ParserEventSourceFromReader(readerParser(name, window), &pieces{bytes: []byte(text), size: size}))
			if !reflect.DeepEqual(read, whole) {
				t.Errorf("%s in pieces of %d:\nfrom a reader %+v\nread whole    %+v", file, size, read, whole)
			}
			compared++
		}
	}
	fmt.Printf("reader: %d runs from a reader compared with the text read whole\n", compared)
	if compared < 60 {
		t.Fatal(compared)
	}
}

func TestAnUnreadableSourceIsInvalidInput(t *testing.T) {
	text := []byte("[1,\n2,\n3333,\n4]")
	for _, window := range []bool{true, false} {
		var rec passRecorder
		_, f := ParserEventSourceFromReader(readerParser("json", window), &failing{bytes: text, good: 9}).Run(&rec)
		if f == nil || f.Code != CodeInputInvalid {
			t.Fatal(window, f)
		}
		if !strings.HasPrefix(f.Message, "unreadable: ") || !strings.Contains(f.Message, "the disk went away") {
			t.Fatal(window, f.Message)
		}
		if f.Row != 3 || f.Column != 3 {
			t.Fatal(window, f.Row, f.Column)
		}
		// Read whole first, the source fails before any pass.
		if !window && len(rec.seen) != 0 {
			t.Fatal(rec.seen)
		}
	}
	// Bytes that are not UTF-8 end the source where they stand.
	for _, window := range []bool{true, false} {
		bytes := []byte{0x5b, 0x31, 0x2c, 0xff, 0x5d}
		_, f := ParserEventSourceFromReader(readerParser("json", window), &pieces{bytes: bytes, size: 2}).Run(&passRecorder{})
		if f == nil || f.Code != CodeInputInvalid || !strings.HasPrefix(f.Message, "unreadable: ") ||
			!strings.Contains(f.Message, "stream did not contain valid UTF-8") {
			t.Fatal(window, f)
		}
		if f.Row != 1 || f.Column != 4 {
			t.Fatal(window, f.Row, f.Column)
		}
	}
	// No reader at all is the engine's unreadable too.
	_, f := ParserEventSourceFromReader(readerParser("json", false), nil).Run(&passRecorder{})
	if f == nil || f.Code != CodeInputInvalid || !strings.HasPrefix(f.Message, "unreadable: ") {
		t.Fatal(f)
	}
}

func TestPruningTheAbortFlagTheLimitsAndTheMetricsHoldFromAReader(t *testing.T) {
	text := recordsJSON(50)
	for _, window := range []bool{true, false} {
		both := func(configure func(*ParserEventSource) *ParserEventSource) [2]*ParserEventSource {
			return [2]*ParserEventSource{
				configure(NewParserEventSource(readerParser("json", window), text)),
				configure(ParserEventSourceFromReader(readerParser("json", window), &pieces{bytes: []byte(text), size: 5})),
			}
		}

		// Pruning: the same passes, and the same tree left behind, its
		// lists emptied.
		var pruned [2]readerShown
		for i, s := range both(func(s *ParserEventSource) *ParserEventSource { return s.Grammar("json").Prune() }) {
			pruned[i] = readerRecorded(s)
		}
		if pruned[0].outcome != "ok continue" || !reflect.DeepEqual(pruned[1], pruned[0]) {
			t.Fatalf("window %v: pruned from a reader %+v, over the text %+v", window, pruned[1].outcome, pruned[0].outcome)
		}
		tree := DatumFromValue(pruned[0].value)
		records, ok := tree.GetPath(
			[]Segment{KeySegment("response"), KeySegment("payload"), KeySegment("deep"), KeySegment("records")})
		if !ok || records.Kind != DatumArray || len(records.Items) != 0 {
			t.Fatalf("window %v: the records are not emptied: %v", window, records)
		}

		// The abort flag: raised by the sink part way, the run is ABORTED
		// after the same passes.
		var aborted [2]readerShown
		for i, s := range both(func(s *ParserEventSource) *ParserEventSource { return s }) {
			abort := NewAbortFlag()
			rec := &abortingRecorder{abort: abort, at: 7}
			aborted[i] = readerRun(s.Abort(abort), rec, func() []ParserEvent { return rec.seen })
		}
		if !strings.HasPrefix(aborted[0].outcome, CodeAborted.String()+" ") || len(aborted[0].passes) != 7 ||
			!reflect.DeepEqual(aborted[1], aborted[0]) {
			t.Fatalf("window %v: aborted from a reader %+v, over the text %+v", window, aborted[1], aborted[0])
		}

		// A limit: refused by name before the sink sees the pass.
		var limited [2]readerShown
		for i, s := range both(func(s *ParserEventSource) *ParserEventSource {
			limits := DefaultLimits()
			limits.MaxScalarBytes = 8
			return s.Limits(limits)
		}) {
			limited[i] = readerRecorded(s)
		}
		if !strings.HasPrefix(limited[0].outcome, CodeResourceLimitExceeded.String()+" ") ||
			!strings.HasSuffix(limited[0].outcome, " max_scalar_bytes") || !reflect.DeepEqual(limited[1], limited[0]) {
			t.Fatalf("window %v: limited from a reader %+v, over the text %+v", window, limited[1], limited[0])
		}

		// The metrics count the events delivered.
		var counted [2][2]int
		for i, s := range both(func(s *ParserEventSource) *ParserEventSource { return s }) {
			m := NewMetrics()
			shown := readerRecorded(s.Metrics(m))
			counted[i] = [2]int{int(m.Events.Load()), len(shown.passes)}
		}
		if counted[0][0] == 0 || counted[0][0] != counted[0][1] || counted[1] != counted[0] {
			t.Fatalf("window %v: counted %v", window, counted)
		}
	}
}
