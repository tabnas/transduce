// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

// The line sources cut records where the grammar ends them: the Go twins
// of the tests beside rs/src/source/lines.rs that a fixture row cannot
// carry (the grammar options the `options` column has no room for, a
// reader that hands out a byte at a time, the abort flag), each holding
// the streamed reading to the whole parse of the same text through the
// same grammar options.

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"

	tabnas "github.com/tabnas/parser/go"
)

// reading is one text's reading: its events, or its failure as the
// engine's code, row and column.
type reading struct {
	events   []Event
	failed   bool
	code     string
	row, col uint64
}

func (r reading) same(o reading) bool {
	if r.failed || o.failed {
		return r.failed == o.failed && r.code == o.code && r.row == o.row && r.col == o.col
	}
	return eventsEqualCore(r.events, o.events)
}

func (r reading) String() string {
	if r.failed {
		return fmt.Sprintf("%s at %d:%d", r.code, r.row, r.col)
	}
	return fmt.Sprint(r.events)
}

// sourceReading is what a run gave: its events, or its INPUT_INVALID
// failure with the engine's code at the head of its message.
func sourceReading(t testing.TB, run func(Sink) (Flow, *Fail)) reading {
	t.Helper()
	var rec Recorder
	flow, f := run(&rec)
	if f != nil {
		if f.Code != CodeInputInvalid {
			t.Fatalf("not invalid input: %v", f)
		}
		code, _, _ := strings.Cut(f.Message, ":")
		return reading{failed: true, code: code, row: f.Row, col: f.Column}
	}
	if flow != Continue {
		t.Fatalf("stopped: %v", flow)
	}
	return reading{events: rec.Events}
}

// csvStreamsAsWhole holds the CSV line source to the whole parse of text
// through the same grammar options (ParserSource, which keeps the
// header's member order), at chunk sizes from a chunk per record to the
// whole text and, at chunk size 0, a byte and three bytes a read, and
// returns that reading.
func csvStreamsAsWhole(t testing.TB, text string, header bool, options map[string]any) reading {
	t.Helper()
	format := CSVFormatWith(header, options)
	want := sourceReading(t, func(s Sink) (Flow, *Fail) {
		parser, f := csvParser(format)
		if f != nil {
			t.Fatal(f)
		}
		return NewParserSource(parser, text).Run(s)
	})
	for chunk := 0; chunk <= len(text)+1; chunk++ {
		if !(chunk < 4 || chunk&(chunk-1) == 0 || chunk >= len(text)) {
			continue
		}
		got := sourceReading(t, func(s Sink) (Flow, *Fail) {
			return NewLinesSource(strings.NewReader(text), format).ChunkBytes(chunk).Run(s)
		})
		if !got.same(want) {
			t.Fatalf("%q, chunk %d: %v, not %v", text, chunk, got, want)
		}
	}
	for _, step := range []int{1, 3} {
		got := sourceReading(t, func(s Sink) (Flow, *Fail) {
			return NewLinesSource(trickled(text, step), format).ChunkBytes(0).Run(s)
		})
		if !got.same(want) {
			t.Fatalf("%q, step %d: %v, not %v", text, step, got, want)
		}
	}
	return want
}

// jsonlStreamsAsWhole holds the JSON Lines source to the whole parse of
// text by the JSON Lines grammar, whole and a byte a read, and returns
// that reading.
func jsonlStreamsAsWhole(t testing.TB, text string) reading {
	t.Helper()
	var want reading
	if value, err := makeGrammar("jsonl").Parse(text); err != nil {
		te, ok := err.(*tabnas.TabnasError)
		if !ok {
			t.Fatal(err)
		}
		want = reading{failed: true, code: te.Code, row: uint64(te.Row), col: uint64(te.Col)}
	} else {
		want = reading{events: walkedEvents(t, value)}
	}
	for _, step := range []int{0, 1, 3} {
		got := sourceReading(t, func(s Sink) (Flow, *Fail) {
			var r io.Reader = strings.NewReader(text)
			if step > 0 {
				r = trickled(text, step)
			}
			return NewLinesSource(r, JSONLFormat()).Run(s)
		})
		if !got.same(want) {
			t.Fatalf("%q, step %d: %v, not %v", text, step, got, want)
		}
	}
	return want
}

func objectCount(t testing.TB, r reading) int {
	t.Helper()
	if r.failed {
		t.Fatalf("failed: %v", r)
	}
	n := 0
	for _, ev := range r.events {
		if ev.Kind == ObjectStart {
			n++
		}
	}
	return n
}

func TestQuotesInsideAFieldAreReadAsTheGrammarReadsThem(t *testing.T) {
	// tabnas-csv's corpus file
	// papa-misplaced-quotes-in-data-twice-not-as-opening-quotes.csv: a
	// quote inside a field is that field's text.
	const misplaced = "A,B\",C\nD,E\",F"
	if n := objectCount(t, csvStreamsAsWhole(t, misplaced, true, nil)); n != 1 {
		t.Fatal(n)
	}
	corpus := map[string]any{"object": false}
	csvStreamsAsWhole(t, misplaced, false, corpus)
	more := misplaced + "\nG,H,I\nJ\"K,L,\"M\nN\"\n"
	if n := objectCount(t, csvStreamsAsWhole(t, more, true, nil)); n != 3 {
		t.Fatal(n)
	}
	csvStreamsAsWhole(t, more, false, corpus)
}

func TestTheHeaderIsTheFirstRecordTheGrammarReads(t *testing.T) {
	empty := map[string]any{"record": map[string]any{"empty": true}}
	relaxed := map[string]any{"strict": false}
	comment := map[string]any{"comment": true}
	for _, c := range []struct {
		text    string
		options map[string]any
		records int
	}{
		{"\n\na,b\n1,2\n3,4\n", nil, 2},
		{"\r\na,b\r\n1,2\r\n3,4\r\n", nil, 2},
		{"\na,b\n\n1,2\n", empty, 3},
		{"  \na,b\n1,2\n3,4\n", nil, 3},
		{"  \na,b\n1,2\n3,4\n", relaxed, 2},
		{"# x \"\na,b\n1,2\n3,4\n", comment, 2},
		{"// x \"\na,b\n1,2\n3,4\n", relaxed, 2},
		{"a,b # c \"\n1,2\n3,4\n", comment, 2},
	} {
		if n := objectCount(t, csvStreamsAsWhole(t, c.text, true, c.options)); n != c.records {
			t.Fatalf("%q: %d records", c.text, n)
		}
	}
}

func TestAFieldSpansLinesOnlyWhereTheGrammarReadsOneThatDoes(t *testing.T) {
	quote := map[string]any{"string": map[string]any{"quote": "'"}}
	tildes := map[string]any{"field": map[string]any{"separation": "~~"}}
	comment := map[string]any{"comment": true}
	for _, c := range []struct {
		text    string
		options map[string]any
	}{
		{"a,b\n`x\ny`,z\n1,2\n", nil},
		{"a,b\n'x\\\ny',z\n1,2\n", nil},
		{"a,b\n'x,\"y',z\n\"p\nq\",r\n", nil},
		{"a,b\nx, \"p\nq\"\n1,2\n", nil},
		{"a~~b\nx~~\"p\nq\"\n1~~2\n", tildes},
		{"a,b\n'x\ny',z\n1,2\n", quote},
		{"a,b\n\"x,y\n1,2\n", quote},
		{"a,b\n/* x\ny */1,2\n3,4\n", comment},
	} {
		csvStreamsAsWhole(t, c.text, true, c.options)
	}
}

func TestALineCharacterInsideAFixedTokenEndsNoPiece(t *testing.T) {
	// The lexer reads a fixed token before a line, so a field separator
	// that holds a line character owns it; where one does, a piece takes
	// a whole run of line characters.
	starts := map[string]any{"field": map[string]any{"separation": "\n~"}}
	inside := map[string]any{"field": map[string]any{"separation": "~\n~"}}
	for _, c := range []struct {
		text    string
		options map[string]any
	}{
		{"a\n~b\nx\n~y\n", starts},
		{"a\n~b\n\n~c\nx\n~y\n", starts},
		{"a\n~b\r\n~c\nx\n~y", starts},
		{"a~\n~b\nx~\n~y\n", inside},
	} {
		csvStreamsAsWhole(t, c.text, true, c.options)
	}
}

func TestRecordsOneLineHoldsAreBoundedAndCutOneByOne(t *testing.T) {
	// Many records and no `\n`: each record is under the limit and the
	// line far over it, so the limit is the record's.
	limits := DefaultLimits()
	limits.MaxRecordBytes = 8
	semicolons := map[string]any{"record": map[string]any{"separators": ";"}}
	for _, c := range []struct {
		text    string
		options map[string]any
	}{
		{"a,b\r" + strings.Repeat("1,2\r", 200), nil},
		{"a,b;" + strings.Repeat("1,x;", 200), semicolons},
	} {
		want := csvStreamsAsWhole(t, c.text, true, c.options)
		if n := objectCount(t, want); n != 200 {
			t.Fatalf("%d records", n)
		}
		for _, chunk := range []int{0, 64, DefaultChunkBytes} {
			got := sourceReading(t, func(s Sink) (Flow, *Fail) {
				return NewLinesSource(strings.NewReader(c.text), CSVFormatWith(true, c.options)).
					Limits(limits).ChunkBytes(chunk).Run(s)
			})
			if !got.same(want) {
				t.Fatalf("chunk %d: %v", chunk, got)
			}
		}
	}
	records := strings.Repeat("{\"a\":1}\r", 200)
	want := jsonlStreamsAsWhole(t, records)
	got := sourceReading(t, func(s Sink) (Flow, *Fail) {
		return NewLinesSource(strings.NewReader(records), JSONLFormat()).Limits(limits).Run(s)
	})
	if objectCount(t, want) != 200 || !got.same(want) {
		t.Fatal(got)
	}
	// A record over the limit still fails, at the row it starts on, which
	// is counted as the engine counts rows: by `\n`, or by the separator.
	for _, c := range []struct {
		text    string
		options map[string]any
		row     uint64
	}{
		{"a,b\r1,2\r123456789,x\r", nil, 1},
		{"a,b;1,x;123456789,x;", semicolons, 3},
	} {
		_, f := NewLinesSource(strings.NewReader(c.text), CSVFormatWith(true, c.options)).Limits(limits).Run(&Recorder{})
		if f == nil || f.Limit == nil || f.Limit.Name != "max_record_bytes" || f.Row != c.row {
			t.Fatalf("%q: %v", c.text, f)
		}
	}
}

func TestALineTokenOfTwoCharactersIsNeverCut(t *testing.T) {
	// Under record.empty a run of line characters ends at a repeated one,
	// so `\r\n`, and `\n\r` as much, is one line token.
	for _, header := range []bool{false, true} {
		options := map[string]any{"record": map[string]any{"empty": true}}
		for _, text := range []string{"a,b\r\n1,2\r\n\r\n3,4\r\n", "a,b\n\r1,2\n\r\n\r3,4", "a\r\n\r\r\nb\r\n"} {
			csvStreamsAsWhole(t, text, header, options)
		}
	}
}

func TestARunOfTwoLineCharactersReadsAsTypeScriptReadsIt(t *testing.T) {
	// The inputs tabnas/parser#271 decided, CSV with record.empty and no
	// header. TypeScript and Rust read four records from each, `\n\r` one
	// line token as `␞¶` and `¶␞` are where those are the separators, and
	// put the unterminated string after two CRLF line ends at row 3. The
	// line source holds to the whole parse at every chunk size, and that
	// is TypeScript's reading, which the Go engine has had since #271.
	empty := map[string]any{"record": map[string]any{"empty": true}}
	marks := map[string]any{"record": map[string]any{"separators": "␞¶", "empty": true}}
	const records = 4
	for _, c := range []struct {
		text    string
		options map[string]any
	}{
		{"a,b\n\r1,2\n\r\n\r3,4", empty},
		{"a,b␞¶£,é␞£,2¶␞3,4", marks},
	} {
		if n := objectCount(t, csvStreamsAsWhole(t, c.text, false, c.options)); n != records {
			t.Fatalf("%q: %d records, not %d", c.text, n, records)
		}
	}
	if got := csvStreamsAsWhole(t, "a,b\r\n1,2\r\n\"x", false, empty); !got.failed ||
		got.code != "unterminated_string" || got.row != 3 || got.col != 1 {
		t.Fatalf("%v", got)
	}
}

func TestUnderLineSingleAPieceEndsWhereTheLexerEndsALineToken(t *testing.T) {
	// The canonical reading stops a run of line characters at its first
	// repeated one, so `\n\r` is one line token as `\r\n` is, and the Go
	// engine reads it so since tabnas/parser#271. A piece ends just past
	// each token, read whole or a byte at a time (which follows a token
	// across reads), and the record scanner's line run is that token.
	crlf, marks := []rune{'\n', '\r'}, []rune{'␞', '¶'}
	for _, c := range []struct {
		single lineSingle
		line   []rune
		text   string
		pieces []string
	}{
		{singleRun, crlf, "a\n\r1\n\r\n\rb", []string{"a\n\r", "1\n\r", "\n\r", "b"}},
		{singleRun, crlf, "a\n\r\nb\r\n\r\rc", []string{"a\n\r", "\n", "b\r\n", "\r", "\r", "c"}},
		{singleRun, marks, "x␞¶y¶␞¶z", []string{"x␞¶", "y¶␞", "¶", "z"}},
	} {
		for _, step := range []int{0, 1} {
			var r io.Reader = strings.NewReader(c.text)
			if step > 0 {
				r = trickled(c.text, step)
			}
			p := newPieces(r, c.line, map[rune]bool{'\n': true}, c.single, nil)
			var got []string
			for {
				_, piece, ok, f := p.nextPiece(1<<20, func() *Fail { panic("no limit") })
				if f != nil {
					t.Fatal(f)
				}
				if !ok {
					break
				}
				got = append(got, piece)
			}
			if fmt.Sprintf("%q", got) != fmt.Sprintf("%q", c.pieces) {
				t.Fatalf("reading %d, %q, step %d: pieces %q, not %q", c.single, c.text, step, got, c.pieces)
			}
		}
		lx := &lexis{single: c.single, lineSet: runeSet(c.line)}
		at := 0
		for _, piece := range c.pieces {
			if i := strings.IndexFunc(piece, func(r rune) bool { return lx.lineSet[r] }); i >= 0 {
				if n := lx.lineRun(c.text[at+i:]); n != len(piece)-i {
					t.Fatalf("reading %d, %q at %d: a line run of %d bytes, not %d", c.single, c.text, at+i, n, len(piece)-i)
				}
			}
			at += len(piece)
		}
	}
}

func TestASeparatorOfSeveralBytesIsFoundAcrossReads(t *testing.T) {
	csvStreamsAsWhole(t, "a,b␞1,é␞3,4␞", true, map[string]any{"record": map[string]any{"separators": "␞"}})
	astral := map[string]any{"record": map[string]any{"separators": "😀"}}
	if n := objectCount(t, csvStreamsAsWhole(t, "a,b😀1,é😀3,4😀", true, astral)); n != 2 {
		t.Fatal(n)
	}
	csvStreamsAsWhole(t, "a,b😀1,2😀3,\"x", true, astral)
	csvStreamsAsWhole(t, "a,b␞¶£,é␞£,2¶␞3,4", false,
		map[string]any{"record": map[string]any{"separators": "␞¶", "empty": true}})
}

func TestAJSONLLineIsBlankOnlyWhenTheGrammarReadsItSo(t *testing.T) {
	// Space and tab are the grammar's blanks; any other line is parsed.
	for _, blank := range []string{"\f", "\v", " ", "\u0085", " ", "　"} {
		want := jsonlStreamsAsWhole(t, "{\"a\":1}\n"+blank+"\n{\"b\":2}\n")
		if !want.failed || want.row != 2 {
			t.Fatalf("%q: %v", blank, want)
		}
	}
	if n := objectCount(t, jsonlStreamsAsWhole(t, "{\"a\":1}\n \t\n\r\n{\"b\":2}\n   ")); n != 2 {
		t.Fatal(n)
	}
}

func TestALineEndingInAJSONLRecordIsTheGrammars(t *testing.T) {
	// A lone CR ends a record; a line character inside a string is the
	// string's, refused there.
	if n := objectCount(t, jsonlStreamsAsWhole(t, "{\"a\":1}\r{\"b\":2}\r\r \r{\"c\":3}\n")); n != 3 {
		t.Fatal(n)
	}
	for _, text := range []string{
		"{\"a\":\r1}\n", "{\"a\":1}\r{\"b\": }\n", "{\"a\":\"x\ry\"}\n",
		"{\"a\":\"x\ny\"}\n{\"b\":1}\n", "[\"\\\n\"]\n", "{\"a\":\"x\r\ny\"}\r\n",
	} {
		if !jsonlStreamsAsWhole(t, text).failed {
			t.Fatalf("%q parsed", text)
		}
	}
}

func TestALongLineTokenIsFollowedOnce(t *testing.T) {
	// Where a fixed token holds a line character, a piece follows a whole
	// run of line characters across reads from where it stopped: read a
	// byte at a time, a run costs its length and not its square.
	const run = 100_000
	text := "a\n~b\nc\n~d" + strings.Repeat("\n", run) + "x\n~y\n"
	p := newPieces(trickled(text, 1), []rune{'\n', '\r'}, map[rune]bool{'\n': true}, singleOff, []string{"\n~"})
	type got struct {
		row uint64
		n   int
	}
	var pieces []got
	for {
		row, piece, ok, f := p.nextPiece(1<<62, func() *Fail { panic("no limit") })
		if f != nil {
			t.Fatal(f)
		}
		if !ok {
			break
		}
		pieces = append(pieces, got{row, len(piece)})
	}
	want := []got{{1, 5}, {3, 4 + run}, {4 + run, 5}}
	if fmt.Sprint(pieces) != fmt.Sprint(want) {
		t.Fatalf("%v, not %v", pieces, want)
	}
}

func TestAnAbortNamesTheRowTheRecordOrChunkItWasReadingStartsOn(t *testing.T) {
	aborter := func(abort *AbortFlag, after int) Sink {
		n := 0
		return FnSink(func(Event) (Flow, *Fail) {
			n++
			if n == after {
				abort.Abort()
			}
			return Continue, nil
		})
	}
	// Raised with the second record's first event: the run stops in that
	// record, which starts on row 3. Already raised: the run's first event
	// fails, before any record is read, and names no row.
	text := "{\"a\":1}\n\n{\"a\":2}\n{\"a\":3}\n"
	for _, c := range []struct {
		after int
		row   uint64
	}{{6, 3}, {0, 0}} {
		abort := NewAbortFlag()
		if c.after == 0 {
			abort.Abort()
		}
		_, f := NewLinesSource(strings.NewReader(text), JSONLFormat()).Abort(abort).Run(aborter(abort, c.after))
		if f == nil || f.Code != CodeAborted || f.Row != c.row || f.Column != 0 {
			t.Fatalf("after %d: %v", c.after, f)
		}
		if !AdapterBuilt() {
			continue
		}
		abort = NewAbortFlag()
		if c.after == 0 {
			abort.Abort()
		}
		_, f = NewLinesSource(strings.NewReader(text), JSONLFormat()).Abort(abort).RunIncremental(aborter(abort, c.after))
		if f == nil || f.Code != CodeAborted || f.Row != c.row || f.Column != 0 {
			t.Fatalf("incremental, after %d: %v", c.after, f)
		}
	}
	// CSV: the first row of the chunk, with a chunk for each record, and
	// with one chunk for the whole text.
	for _, c := range []struct {
		chunk int
		row   uint64
	}{{0, 3}, {DefaultChunkBytes, 1}} {
		abort := NewAbortFlag()
		_, f := NewLinesSource(bytes.NewReader([]byte("a\n1\n2\n3\n")), CSVFormat()).
			Abort(abort).ChunkBytes(c.chunk).Run(aborter(abort, 5))
		if f == nil || f.Code != CodeAborted || f.Row != c.row || f.Column != 0 {
			t.Fatalf("chunk %d: %v", c.chunk, f)
		}
	}
}
