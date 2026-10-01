// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

// Ported from the Rust unit tests in rs/src/source/{mod,parser,lines}.rs,
// in both modes.

import (
	"bufio"
	"bytes"
	"io"
	"strings"
	"testing"
)

const parserDoc = `{"a":[1,2.50,"x",{"b":null}],"c":{},"d":[],"e":1e21,"f":true}`

// sourceModes is every mode a ParserSource runs.
func sourceModes() []SourceMode {
	return []SourceMode{MaterializeMode(), IncrementalMode(Prune{})}
}

func record(mode SourceMode, src string) (*Fail, []Event) {
	var rec Recorder
	_, f := NewParserSource(makeGrammar("json"), src).Grammar("json").Mode(mode).Run(&rec)
	return f, rec.Events
}

func TestAValueWalksInDocumentOrder(t *testing.T) {
	v, err := makeGrammar("json").Parse(`{"a":[1,"x"],"b":null}`)
	if err != nil {
		t.Fatal(err)
	}
	var rec Recorder
	ValueSource{Value: v}.Run(&rec)
	want := []Event{EvObjectStart(), EvKey("a"), EvArrayStart(), EvNumber(1), EvString("x"), EvArrayEnd(),
		EvKey("b"), EvNull(), EvObjectEnd(), EvEnd()}
	if !eventsEqualCore(rec.Events, want) {
		t.Fatal(rec.Events)
	}
}

func TestIncrementalModeNeedsAVerifiedGrammarNameAndEmitsNothingWithoutOne(t *testing.T) {
	var rec Recorder
	_, f := NewParserSource(makeGrammar("json"), parserDoc).Mode(IncrementalMode(Prune{})).Run(&rec)
	if f == nil || f.Code != CodeStreamabilityUnknown || len(rec.Events) != 0 {
		t.Fatal(f)
	}
	if !strings.Contains(f.Message, "ParserSource.Grammar") {
		t.Fatal(f)
	}
	_, f = NewParserSource(makeGrammar("csv"), "a,b\n1,2\n").Grammar("csv").Mode(IncrementalMode(Prune{})).Run(&rec)
	if f == nil || f.Code != CodeStreamabilityUnknown || len(rec.Events) != 0 {
		t.Fatal(f)
	}
	if !strings.Contains(f.Message, `"csv"`) {
		t.Fatal(f)
	}
	// Materialize needs no name.
	if _, f := NewParserSource(makeGrammar("csv"), "a,b\n1,2\n").Run(&rec); f != nil || rec.Events[len(rec.Events)-1].Kind != End {
		t.Fatal(f)
	}
	// The unverified switch lifts the gate for the suite: csv then runs
	// and is refused during the parse.
	rec = Recorder{}
	_, f = NewParserSource(makeGrammar("csv"), "a,b\n1,2\n").Unverified().Mode(IncrementalMode(Prune{})).Run(&rec)
	if f == nil || f.Code != CodeStreamabilityUnknown || hasEndEvent(rec.Events) {
		t.Fatal(f)
	}
	if !strings.Contains(f.Message, "the incremental source cannot follow a grammar that builds") || len(rec.Events) == 0 {
		t.Fatal(f, rec.Events)
	}
}

func hasEndEvent(events []Event) bool {
	for _, ev := range events {
		if ev.Kind == End {
			return true
		}
	}
	return false
}

func TestAStopFromTheSinkStopsTheParse(t *testing.T) {
	for _, mode := range sourceModes() {
		seen := 0
		sink := FnSink(func(Event) (Flow, *Fail) {
			seen++
			if seen == 3 {
				return Stop, nil
			}
			return Continue, nil
		})
		flow, f := NewParserSource(makeGrammar("json"), parserDoc).Grammar("json").Mode(mode).Run(sink)
		if f != nil || flow != Stop || seen != 3 {
			t.Fatal(mode, flow, f, seen)
		}
	}
}

func TestASinkFailureComesBackUnchanged(t *testing.T) {
	for _, mode := range sourceModes() {
		sink := FnSink(func(ev Event) (Flow, *Fail) {
			if ev.Kind == Key && ev.Text == "c" {
				return Continue, OutputFail("disk full").AtPath(".c")
			}
			return Continue, nil
		})
		_, f := NewParserSource(makeGrammar("json"), parserDoc).Grammar("json").Mode(mode).Run(sink)
		if f == nil || f.Code != CodeOutputFailed || f.Path != ".c" {
			t.Fatal(mode, f)
		}
	}
}

func TestAnAbortedFlagCancelsTheParseAsAborted(t *testing.T) {
	for _, mode := range sourceModes() {
		abort := NewAbortFlag()
		abort.Abort()
		_, f := NewParserSource(makeGrammar("json"), parserDoc).Grammar("json").Mode(mode).Abort(abort).Run(&Recorder{})
		if f == nil || f.Code != CodeAborted {
			t.Fatal(mode, f)
		}
	}
}

func TestAParseErrorIsInvalidInputWithItsPosition(t *testing.T) {
	for _, mode := range sourceModes() {
		f, _ := record(mode, "{\"a\": 1,\n \"b\": }")
		if f == nil || f.Code != CodeInputInvalid || !strings.HasPrefix(f.Message, "unexpected") || f.Row != 2 || f.Column != 7 {
			t.Fatal(mode, f)
		}
	}
}

// tabnas-ini installs a depth guard of its own on the engine's parse
// budget; the source's budget is chained to it, so the grammar's guard
// still fires, and its cancel is the grammar's refusal, not an abort.
func TestAGrammarsOwnGuardIsInvalidInputThatNamesTheGrammar(t *testing.T) {
	// A section header 200 segments deep, past tabnas-ini's DepthLimit.
	src := "[" + strings.TrimSuffix(strings.Repeat("a.", 200), ".") + "]\nx=1\n"
	_, f := NewParserSource(makeGrammar("ini"), src).Run(&Recorder{})
	if f == nil || f.Code != CodeInputInvalid || !strings.HasPrefix(f.Message, "the grammar stopped the parse") ||
		!strings.Contains(f.Message, "cancel") {
		t.Fatal(f)
	}
}

func TestSourceLimitsApplyInEveryModeByName(t *testing.T) {
	for _, mode := range sourceModes() {
		for _, c := range []struct {
			src   string
			set   func(*Limits)
			limit string
		}{
			{`{"ab":1}`, func(l *Limits) { l.MaxKeyBytes = 1 }, "max_key_bytes"},
			{"[[[1]]]", func(l *Limits) { l.MaxDepth = 2 }, "max_depth"},
			{`["abc"]`, func(l *Limits) { l.MaxScalarBytes = 2 }, "max_scalar_bytes"},
		} {
			limits := DefaultLimits()
			c.set(&limits)
			_, f := NewParserSource(makeGrammar("json"), c.src).Grammar("json").Mode(mode).Limits(limits).Run(&Recorder{})
			if f == nil || f.Code != CodeResourceLimitExceeded || f.Limit.Name != c.limit {
				t.Fatal(mode, c.limit, f)
			}
		}
	}
}

func TestMetricsCountTheSourceEvents(t *testing.T) {
	for _, mode := range sourceModes() {
		m := NewMetrics()
		var rec Recorder
		if _, f := NewParserSource(makeGrammar("json"), parserDoc).Grammar("json").Mode(mode).Metrics(m).Run(&rec); f != nil {
			t.Fatal(f)
		}
		if m.Events.Load() != uint64(len(rec.Events)) || m.Keys.Load() != 6 || m.Scalars.Load() != 6 {
			t.Fatal(mode, m.Events.Load(), m.Keys.Load(), m.Scalars.Load())
		}
	}
}

// trickle is a reader that hands out at most step bytes per read, so
// every boundary the line reader could meet is met.
type trickle struct {
	data []byte
	step int
}

func (r *trickle) Read(out []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := min(r.step, len(out), len(r.data))
	copy(out, r.data[:n])
	r.data = r.data[n:]
	return n, nil
}

func trickled(data string, step int) io.Reader {
	return bufio.NewReaderSize(&trickle{data: []byte(data), step: max(step, 1)}, 16)
}

const linesJSONL = "{\"a\":1.50,\"b\":[true,null]}\r\n\n  \n{\"a\":2,\"b\":\"x\"}\n[3]\n\"s\""

func walkedEvents(t testing.TB, value any) []Event {
	var rec Recorder
	if _, f := (ValueSource{Value: value}).Run(&rec); f != nil {
		t.Fatal(f)
	}
	return rec.Events
}

func TestJSONLMatchesTheWholeFileParseAtEveryReaderBoundary(t *testing.T) {
	whole, err := makeGrammar("jsonl").Parse(linesJSONL)
	if err != nil {
		t.Fatal(err)
	}
	want := walkedEvents(t, whole)
	for step := 1; step <= len(linesJSONL); step++ {
		var rec Recorder
		if _, f := NewLinesSource(trickled(linesJSONL, step), JSONLFormat()).Run(&rec); f != nil || !eventsEqualCore(rec.Events, want) {
			t.Fatalf("walk, step %d: %v %v", step, f, rec.Events)
		}
		rec = Recorder{}
		_, f := NewLinesSource(trickled(linesJSONL, step), JSONLFormat()).RunIncremental(&rec)
		var stripped []Event
		lexeme := false
		for _, ev := range rec.Events {
			lexeme = lexeme || (ev.Kind == Number && ev.HasLexeme && ev.Lexeme == "1.50")
			stripped = append(stripped, ev.WithoutLexeme())
		}
		if f != nil || !eventsEqualCore(stripped, want) || !lexeme {
			t.Fatalf("incremental, step %d: %v %v", step, f, rec.Events)
		}
	}
}

func TestABadJSONLLineNamesItsLineNumber(t *testing.T) {
	text := "{\"a\":1}\n\n{\"a\": }\n{\"a\":2}\n"
	_, f := NewLinesSource(strings.NewReader(text), JSONLFormat()).Run(&Recorder{})
	if f == nil || f.Code != CodeInputInvalid || f.Row != 3 || f.Column != 7 {
		t.Fatal(f)
	}
	_, f = NewLinesSource(strings.NewReader(text), JSONLFormat()).RunIncremental(&Recorder{})
	if f == nil || f.Code != CodeInputInvalid || f.Row != 3 {
		t.Fatal(f)
	}
}

func TestEmptyInputIsAnEmptyArrayForBothFormats(t *testing.T) {
	for _, format := range []LineFormat{JSONLFormat(), CSVFormat()} {
		var rec Recorder
		NewLinesSource(strings.NewReader(""), format).Run(&rec)
		if !eventsEqualCore(rec.Events, []Event{EvArrayStart(), EvArrayEnd(), EvEnd()}) {
			t.Fatal(rec.Events)
		}
		rec = Recorder{}
		if _, f := NewLinesSource(strings.NewReader("\n\n"), format).RunIncremental(&rec); f != nil || len(rec.Events) != 3 {
			t.Fatal(f, rec.Events)
		}
	}
}

const linesCSV = "id,note,n\r\n1,\"multi\r\nline, with \"\"quotes\"\"\",2.50\r\n3,plain,4\r\n\r\n5,\"a\",6"

// csvWhole is the whole file's events: ParserSource over the CSV grammar,
// which puts the records' members back in the header's order.
func csvWhole(t testing.TB, text string, options map[string]any) []Event {
	var rec Recorder
	parser, f := csvParser(LineFormat{Kind: FormatCSV, Header: options["header"] != false, Options: options})
	if f != nil {
		t.Fatal(f)
	}
	if _, f := NewParserSource(parser, text).Run(&rec); f != nil {
		t.Fatal(f)
	}
	return rec.Events
}

func TestCSVMatchesTheWholeFileParseAtEveryChunkSizeAndReaderBoundary(t *testing.T) {
	want := csvWhole(t, linesCSV, map[string]any{})
	objects := 0
	for _, ev := range want {
		if ev.Kind == ObjectStart {
			objects++
		}
	}
	if objects != 3 || want[2] != EvKey("id") || want[4] != EvKey("note") || want[6] != EvKey("n") {
		t.Fatal(want)
	}
	for chunk := 0; chunk <= len(linesCSV)+1; chunk++ {
		var rec Recorder
		if _, f := NewLinesSource(strings.NewReader(linesCSV), CSVFormat()).ChunkBytes(chunk).Run(&rec); f != nil || !eventsEqualCore(rec.Events, want) {
			t.Fatalf("chunk %d: %v %v", chunk, f, rec.Events)
		}
	}
	for step := 1; step <= len(linesCSV); step++ {
		var rec Recorder
		if _, f := NewLinesSource(trickled(linesCSV, step), CSVFormat()).ChunkBytes(7).RunIncremental(&rec); f != nil || !eventsEqualCore(rec.Events, want) {
			t.Fatalf("step %d: %v %v", step, f, rec.Events)
		}
	}
}

func TestCSVWithoutAHeaderYieldsTheGrammarsRecords(t *testing.T) {
	text := "1,2\n3,\"4\n5\"\n"
	for _, object := range []bool{true, false} {
		options := map[string]any{"header": false, "object": object}
		want := csvWhole(t, text, options)
		for chunk := 0; chunk <= len(text); chunk++ {
			var rec Recorder
			_, f := NewLinesSource(strings.NewReader(text), CSVFormatWith(false, map[string]any{"object": object})).ChunkBytes(chunk).Run(&rec)
			if f != nil || !eventsEqualCore(rec.Events, want) {
				t.Fatalf("object %v, chunk %d: %v %v", object, chunk, f, rec.Events)
			}
		}
	}
}

func TestAHeaderOnlyFileIsAnEmptyTable(t *testing.T) {
	var rec Recorder
	if _, f := NewLinesSource(strings.NewReader("a,b\n"), CSVFormat()).Run(&rec); f != nil || len(rec.Events) != 3 {
		t.Fatal(f, rec.Events)
	}
}

// counting counts the bytes handed out, so a test can see how far past
// the limit a reader was pulled.
type counting struct {
	inner io.Reader
	read  int
}

func (c *counting) Read(out []byte) (int, error) {
	n, err := c.inner.Read(out)
	c.read += n
	return n, err
}

// endless repeats one byte without end and never a newline.
type endless byte

func (e endless) Read(out []byte) (int, error) {
	for i := range out {
		out[i] = byte(e)
	}
	return len(out), nil
}

func TestAnUnterminatedLineIsRefusedAtTheLimitNotAfterBeingReadWhole(t *testing.T) {
	const buffer = 4096
	limits := DefaultLimits()
	limits.MaxRecordBytes = 8
	for _, format := range []LineFormat{JSONLFormat(), CSVFormat()} {
		c := &counting{inner: endless('a')}
		_, f := NewLinesSource(bufio.NewReaderSize(c, buffer), format).Limits(limits).Run(&Recorder{})
		if f == nil || f.Limit == nil || f.Limit.Name != "max_record_bytes" || f.Row != 1 {
			t.Fatal(f)
		}
		if c.read > limits.MaxRecordBytes+buffer {
			t.Fatalf("%v: %d bytes were pulled from an endless line", format.Kind, c.read)
		}
	}
}

func TestAnOversizedRecordNamesMaxRecordBytes(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxRecordBytes = 8
	_, f := NewLinesSource(strings.NewReader("{\"a\":1}\n{\"a\":123456}\n"), JSONLFormat()).Limits(limits).Run(&Recorder{})
	if f == nil || f.Limit.Name != "max_record_bytes" || f.Row != 2 {
		t.Fatal(f)
	}
	_, f = NewLinesSource(strings.NewReader("a\n\"long\nquoted\nfield\"\n"), CSVFormat()).Limits(limits).Run(&Recorder{})
	if f == nil || f.Limit.Name != "max_record_bytes" || f.Row != 2 {
		t.Fatal(f)
	}
}

func TestAStopAndAnAbortEndTheRunOnBothPaths(t *testing.T) {
	text := "{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n"
	stopper := func() Sink {
		n := 0
		return FnSink(func(Event) (Flow, *Fail) {
			n++
			if n == 4 {
				return Stop, nil
			}
			return Continue, nil
		})
	}
	if flow, f := NewLinesSource(strings.NewReader(text), JSONLFormat()).Run(stopper()); f != nil || flow != Stop {
		t.Fatal(flow, f)
	}
	abort := NewAbortFlag()
	abort.Abort()
	if _, f := NewLinesSource(strings.NewReader(text), JSONLFormat()).Abort(abort).Run(&Recorder{}); f == nil || f.Code != CodeAborted {
		t.Fatal(f)
	}
	if flow, f := NewLinesSource(strings.NewReader(text), JSONLFormat()).RunIncremental(stopper()); f != nil || flow != Stop {
		t.Fatal(flow, f)
	}
	if _, f := NewLinesSource(strings.NewReader(text), JSONLFormat()).Abort(abort).RunIncremental(&Recorder{}); f == nil || f.Code != CodeAborted {
		t.Fatal(f)
	}
}

func TestInvalidUTF8IsInvalidInputAtItsLine(t *testing.T) {
	data := []byte("{\"a\":1}\n{\"a\":\"\xff\"}\n")
	_, f := NewLinesSource(bytes.NewReader(data), JSONLFormat()).Run(&Recorder{})
	if f == nil || f.Code != CodeInputInvalid || f.Row != 2 || f.Column != 7 {
		t.Fatal(f)
	}
	_, f = NewLinesSource(bytes.NewReader([]byte("a\n\xff\n")), CSVFormat()).Run(&Recorder{})
	if f == nil || f.Code != CodeInputInvalid || f.Row != 2 {
		t.Fatal(f)
	}
}
