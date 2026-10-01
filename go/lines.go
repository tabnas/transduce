// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"unicode/utf8"

	tabnascsv "github.com/tabnas/csv/go"
	tabnasjson "github.com/tabnas/json/go"
	tabnas "github.com/tabnas/parser/go"
)

// DefaultChunkBytes is how much of the input one CSV parse holds, at
// most one record over.
const DefaultChunkBytes = 256 * 1024

// LineFormatKind names a line-delimited format.
type LineFormatKind uint8

const (
	// FormatJSONL is one JSON value per line; blank lines are skipped.
	FormatJSONL LineFormatKind = iota
	// FormatCSV is CSV records.
	FormatCSV
)

// LineFormat is the line-delimited format to read. For FormatCSV,
// Header says whether the first record names the fields (it overrides
// Options["header"]); Options are the CSV grammar's own (object: false
// for arrays instead of objects, and so on), as tabnascsv.Make takes
// them.
type LineFormat struct {
	Kind    LineFormatKind
	Header  bool
	Options map[string]any
}

// JSONLFormat is JSON Lines.
func JSONLFormat() LineFormat { return LineFormat{Kind: FormatJSONL} }

// CSVFormat is CSV with a header line and the grammar's default options.
func CSVFormat() LineFormat { return LineFormat{Kind: FormatCSV, Header: true} }

// CSVFormatWith is CSV with the given grammar options; header decides
// whether the first record names the fields.
func CSVFormatWith(header bool, options map[string]any) LineFormat {
	return LineFormat{Kind: FormatCSV, Header: header, Options: options}
}

// LinesSource reads JSON Lines or CSV from any io.Reader a record (or a
// chunk of records) at a time.
//
// The engine parses a whole string, so a document is retained at least
// once however it is consumed. Line-delimited formats do not need to be
// one document: JSON Lines is one value per line, and CSV is a header
// plus independent records, so each can be parsed a piece at a time
// with one reused parser and the memory a run needs stops depending on
// the file's size. Its events are exactly the whole-file parse's (the
// array of the per-line values, or of the records).
//
// JSON Lines: each non-blank line is parsed with one tabnas-json parser.
// Run walks each value (numbers carry no lexeme); RunIncremental puts
// each line through the rule-event adapter, reset per line, so numbers
// keep their lexemes. A line that does not parse is INPUT_INVALID with
// the line's number as the row.
//
// CSV: the input is cut into chunks of whole records at newlines outside
// quotes (a `"` toggles quoting, so `""` inside a field is two toggles
// and a quoted field may span lines). The header record is kept and
// prepended to every chunk after the first when Header is on, so the
// reused tabnas-csv parser names each record's fields as the whole file
// would. A chunk closes at the first record boundary at or past
// DefaultChunkBytes (or ChunkBytes), so one chunk, and never a fraction
// of a record, is what a parse holds.
//
// A single record larger than max_record_bytes (a line, for JSON Lines)
// fails with that limit's name rather than growing a chunk without
// bound, and the bound holds while the record is READ: a line is taken
// from the reader in pieces of at most its buffer and refused the moment
// it passes the limit. max_record_bytes counts the record's source bytes
// here, its line ending included.
type LinesSource struct {
	reader     io.Reader
	format     LineFormat
	limits     Limits
	abort      *AbortFlag
	metrics    *Metrics
	chunkBytes int
}

// NewLinesSource is a source over reader with default limits, its own
// abort flag, fresh metrics and DefaultChunkBytes.
func NewLinesSource(reader io.Reader, format LineFormat) *LinesSource {
	return &LinesSource{
		reader:     reader,
		format:     format,
		limits:     DefaultLimits(),
		abort:      NewAbortFlag(),
		metrics:    NewMetrics(),
		chunkBytes: DefaultChunkBytes,
	}
}

// Limits sets the run's limits.
func (l *LinesSource) Limits(limits Limits) *LinesSource {
	l.limits = limits
	return l
}

// Abort sets the abort flag the run polls.
func (l *LinesSource) Abort(abort *AbortFlag) *LinesSource {
	l.abort = abort
	return l
}

// Metrics sets the metrics the run counts into.
func (l *LinesSource) Metrics(metrics *Metrics) *LinesSource {
	l.metrics = metrics
	return l
}

// ChunkBytes sets the CSV chunk size; a chunk closes at the first record
// boundary at or past it. Zero closes a chunk at every record.
func (l *LinesSource) ChunkBytes(bytes int) *LinesSource {
	l.chunkBytes = bytes
	return l
}

// Run drives sink through the walk, both formats; JSON Lines numbers
// carry no lexeme on this path.
func (l *LinesSource) Run(sink Sink) (Flow, *Fail) {
	guarded := NewGuarded(sink, l.limits, l.abort, l.metrics)
	flow, f := l.drive(guarded)
	guarded.Flush()
	return flow, f
}

// RunIncremental drives sink with JSON Lines through the rule-event
// adapter (lexemes kept), and CSV through the walk. It installs the
// adapter on tabnas-json without ParserSource's gate, which is sound only
// while json is verified, so a build in which it is not (no adapter)
// refuses a JSON Lines run with STREAMABILITY_UNKNOWN before reading.
func (l *LinesSource) RunIncremental(sink Sink) (Flow, *Fail) {
	if l.format.Kind == FormatJSONL {
		if !Incremental("json") {
			return Continue, unverifiedLines()
		}
		return jsonlIncremental(l, sink)
	}
	return l.Run(sink)
}

func unverifiedLines() *Fail {
	return NewFail(CodeStreamabilityUnknown,
		`the JSON Lines incremental path needs grammar "json" in IncrementalGrammars, and this build `+
			`verifies none (it has no incremental adapter); run LinesSource.Run`)
}

func lineGuard(parser *tabnas.Tabnas, abort *AbortFlag) *parseGuard {
	return installGuard(parser, func() bool { return !abort.IsAborted() })
}

// lineFailure is an engine error on one line: the line's number is the
// row.
func lineFailure(err error, line uint64, abort *AbortFlag) *Fail {
	f := engineFailure(err, abort)
	if f.Code != CodeAborted {
		f.Row = line
		f.Column = 0
		if te, ok := err.(*tabnas.TabnasError); ok {
			f.Column = uint64(te.Col)
		}
	}
	return f
}

func sendAll(g *Guarded, evs ...Event) (Flow, *Fail) {
	for _, ev := range evs {
		flow, f := g.Event(ev)
		if f != nil || flow == Stop {
			return flow, f
		}
	}
	return Continue, nil
}

// drive is the walking path, both formats.
func (l *LinesSource) drive(g *Guarded) (Flow, *Fail) {
	if l.format.Kind == FormatJSONL {
		parser := tabnasjson.Make()
		lineGuard(parser, l.abort)
		lines := newLines(l.reader, l.limits.MaxRecordBytes)
		if flow, f := sendAll(g, EvArrayStart()); f != nil || flow == Stop {
			return flow, f
		}
		for {
			number, line, ok, f := lines.nextLine()
			if f != nil {
				return Continue, f
			}
			if !ok {
				break
			}
			if strings.TrimSpace(line) == "" {
				continue
			}
			value, err := parser.Parse(line)
			if err != nil {
				return Continue, lineFailure(err, number, l.abort)
			}
			if flow, f := WalkValue(value, g); f != nil || flow == Stop {
				return flow, f
			}
		}
		return sendAll(g, EvArrayEnd(), EvEnd())
	}

	parser, f := csvParser(l.format)
	if f != nil {
		return Continue, f
	}
	guard := lineGuard(parser, l.abort)
	chunks := newChunks(newLines(l.reader, l.limits.MaxRecordBytes), l.format.Header, l.chunkBytes)
	if flow, f := sendAll(g, EvArrayStart()); f != nil || flow == Stop {
		return flow, f
	}
	for {
		chunk, ok, f := chunks.nextChunk()
		if f != nil {
			return Continue, f
		}
		if !ok {
			break
		}
		value, err := parser.Parse(chunk.text)
		if err != nil {
			return Continue, chunk.failure(err, l.abort)
		}
		fields := guard.fieldOrder()
		for _, record := range recordsOf(value) {
			if flow, f := walkValueOrdered(record, g, fields); f != nil || flow == Stop {
				return flow, f
			}
		}
	}
	return sendAll(g, EvArrayEnd(), EvEnd())
}

// csvParser is the CSV grammar for the format, Header deciding whether
// the first record names the fields.
func csvParser(format LineFormat) (*tabnas.Tabnas, *Fail) {
	options := make(map[string]any, len(format.Options)+1)
	for k, v := range format.Options {
		options[k] = v
	}
	options["header"] = format.Header
	parser, err := tabnascsv.Make(options)
	if err != nil {
		return nil, InputFail(fmt.Sprintf("the CSV grammar refused its options: %v", err))
	}
	return parser, nil
}

// recordsOf is the records of a parsed CSV chunk: the elements of its
// array.
func recordsOf(value any) []any {
	switch v := value.(type) {
	case []any:
		return v
	case tabnas.ListRef:
		return v.Val
	}
	return nil
}

// lines reads lines from a reader, numbered from 1, each with its line
// ending, through one reused buffer.
type lines struct {
	reader   *bufio.Reader
	buf      []byte
	number   uint64
	maxBytes int
}

func newLines(r io.Reader, maxBytes int) *lines {
	br, ok := r.(*bufio.Reader)
	if !ok {
		br = bufio.NewReader(r)
	}
	return &lines{reader: br, maxBytes: maxBytes}
}

// nextLine is the next line without its line ending; ok is false at the
// end.
func (l *lines) nextLine() (uint64, string, bool, *Fail) {
	number, raw, ok, f := l.nextRaw()
	if f != nil || !ok {
		return 0, "", ok, f
	}
	end := len(raw)
	if end > 0 && raw[end-1] == '\n' {
		end--
		if end > 0 && raw[end-1] == '\r' {
			end--
		}
	}
	text := raw[:end]
	if !utf8.Valid(text) {
		return 0, "", false, notUTF8(number, text)
	}
	return number, string(text), true, nil
}

func notUTF8(number uint64, b []byte) *Fail {
	valid := 0
	for valid < len(b) {
		r, size := utf8.DecodeRune(b[valid:])
		if r == utf8.RuneError && size <= 1 {
			break
		}
		valid += size
	}
	return InputFail(fmt.Sprintf("line %d is not UTF-8: invalid utf-8 sequence from index %d", number, valid)).
		At(number, uint64(valid)+1)
}

// nextRaw is the next line with its line ending, as bytes, taken from
// the reader in pieces of at most its buffer, so a line is refused the
// moment it passes maxBytes instead of after it has been read whole.
func (l *lines) nextRaw() (uint64, []byte, bool, *Fail) {
	l.buf = l.buf[:0]
	number := l.number + 1
	for {
		piece, err := l.reader.ReadSlice('\n')
		take := len(piece)
		if l.maxBytes < math.MaxInt {
			// One byte over is all the failure needs to know.
			if room := l.maxBytes + 1 - len(l.buf); take > room {
				take = room
			}
		}
		l.buf = append(l.buf, piece[:take]...)
		if len(l.buf) > l.maxBytes {
			return 0, nil, false, LimitFail("max_record_bytes", uint64(l.maxBytes),
				fmt.Sprintf("line %d is longer than %d bytes", number, l.maxBytes)).At(number, 1)
		}
		if err == nil {
			break
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if err == io.EOF {
			break
		}
		return 0, nil, false, InputFail(fmt.Sprintf("reading line %d: %v", number, err))
	}
	if len(l.buf) == 0 {
		return 0, nil, false, nil
	}
	l.number = number
	return number, l.buf, true, nil
}

// chunk is one CSV chunk ready to parse: whole records, the header
// prepended when it is not the chunk that carried it.
type chunk struct {
	text string
	// firstLine is the file line the chunk's first record came from.
	firstLine uint64
	// prefixLines is the lines in text before the first record: 1 when
	// the header is in the text, else 0.
	prefixLines uint64
}

// failure is an engine error inside the chunk, at the file's line.
func (c *chunk) failure(err error, abort *AbortFlag) *Fail {
	rowInChunk := uint64(1)
	if te, ok := err.(*tabnas.TabnasError); ok && te.Row > 1 {
		rowInChunk = uint64(te.Row)
	}
	line := uint64(1)
	if rowInChunk > c.prefixLines {
		line = c.firstLine + rowInChunk - c.prefixLines - 1
	}
	return lineFailure(err, line, abort)
}

// chunks cuts a CSV reader into chunks of whole records.
type chunks struct {
	lines          *lines
	header         *string
	wantHeader     bool
	chunkBytes     int
	maxRecordBytes int
	done           bool
}

func newChunks(l *lines, header bool, chunkBytes int) *chunks {
	return &chunks{lines: l, wantHeader: header, chunkBytes: chunkBytes, maxRecordBytes: l.maxBytes}
}

// readRecord reads one whole record (lines until the quotes balance)
// into text, and gives its first line's number; ok is false at the end.
func (c *chunks) readRecord(text *strings.Builder) (uint64, bool, *Fail) {
	start := text.Len()
	var first uint64
	inQuotes := false
	for {
		number, raw, ok, f := c.lines.nextRaw()
		if f != nil {
			return 0, false, f
		}
		if !ok {
			// An unterminated quote at the end of the input stays in the
			// text for the parser to report as the grammar does.
			return first, first != 0, nil
		}
		if first == 0 {
			first = number
		}
		if !utf8.Valid(raw) {
			return 0, false, notUTF8(number, raw)
		}
		for _, b := range raw {
			if b == '"' {
				inQuotes = !inQuotes
			}
		}
		text.Write(raw)
		if text.Len()-start > c.maxRecordBytes {
			return 0, false, LimitFail("max_record_bytes", uint64(c.maxRecordBytes), fmt.Sprintf(
				"the record starting at line %d is longer than %d bytes", first, c.maxRecordBytes)).At(first, 1)
		}
		if !inQuotes {
			return first, true, nil
		}
	}
}

func (c *chunks) nextChunk() (*chunk, bool, *Fail) {
	if c.done {
		return nil, false, nil
	}
	var text strings.Builder
	var prefixLines uint64
	switch {
	case c.wantHeader && c.header == nil:
		// The first chunk carries the header as its own first record.
		_, ok, f := c.readRecord(&text)
		if f != nil {
			return nil, false, f
		}
		if !ok {
			c.done = true
			return nil, false, nil
		}
		h := text.String()
		c.header = &h
		prefixLines = 1
	case c.header != nil:
		text.WriteString(*c.header)
		prefixLines = 1
	}
	var first uint64
	for {
		line, ok, f := c.readRecord(&text)
		if f != nil {
			return nil, false, f
		}
		if !ok {
			c.done = true
			break
		}
		if first == 0 {
			first = line
		}
		if text.Len() >= c.chunkBytes {
			break
		}
	}
	if first == 0 {
		// Nothing but the header (or nothing at all) was left: a
		// header-only file is the grammar's empty table, and a chunk of
		// just the prepended header has no records to emit.
		return nil, false, nil
	}
	return &chunk{text: text.String(), firstLine: first, prefixLines: prefixLines}, true, nil
}
