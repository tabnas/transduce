// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math"
	"reflect"
	"sort"
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
	// FormatJSONL is one JSON value per line; blank lines (nothing but
	// spaces and tabs) are skipped.
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
// Both formats cut the input where the grammar ends a record, reading
// the text as the grammar's own lexer does (with the line characters,
// quotes, separators and comments its parser's resolved options set), and
// nothing read is left unparsed but a blank JSON Lines record, which the
// grammar skips too.
//
// JSON Lines: each record is parsed with one tabnas-json parser. A record
// is the text between line characters outside a string, so a lone `\r`
// ends one as `\n` does. A record of nothing but the grammar's spaces (a
// space or a tab) is blank and skipped; any other is parsed, so a line
// holding a form feed or a no-break space fails as the grammar fails it.
// Run walks each value (numbers carry no lexeme); RunIncremental puts
// each record through the rule-event adapter, reset per record, so
// numbers keep their lexemes. A record that does not parse is
// INPUT_INVALID with its line's number as the row.
//
// CSV: the input is cut into chunks of whole records. A quote opens a
// quoted field only where the lexer starts a token (a record's start,
// after the field separator or a space), so a quote inside a field is
// that field's text; the configured quote reads `""` as one quote; a
// quoted field, a backtick string and a block comment may span lines; and
// a line character anywhere else, a lone `\r` as much as `\n`, ends a
// record. The header record, the first record the grammar reads (a blank
// or comment line before it is none), is kept and prepended to every
// chunk after the first when Header is on, so the reused tabnas-csv
// parser names each record's fields as the whole file would. A chunk
// closes at the first record boundary at or past DefaultChunkBytes (or
// ChunkBytes), so one chunk, and never a fraction of a record, is what a
// parse holds. An input that ends inside a quoted field fails with the
// grammar's unterminated_string, in the header as anywhere.
//
// The reader is taken a piece at a time, and a piece ends just past each
// of the grammar's line endings: one of its line characters (a lone `\r`
// or a configured separator as much as `\n`), a `\r\n`, or under
// record.empty the whole line token the lexer reads. A line character
// inside one of the grammar's fixed tokens (a field separator such as
// "\n~") is the token's and ends no piece. So a record never waits for a
// `\n` to end, and a chunk can close after any record, however many of
// them one `\n`-terminated line holds.
//
// A single record larger than max_record_bytes (a record with its line
// ending; for the CSV header, everything up to its end) fails with that
// limit's name rather than growing a chunk without bound, and the bound
// holds while the record is READ: a record is taken from the reader a
// buffer at a time and refused the moment it passes the limit.
//
// A run the abort flag cancels is ABORTED with the row the record (JSON
// Lines) or chunk (CSV) it was reading starts on, and no column: the
// abort lands between two of the engine's steps, where the engine has no
// position of its own.
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

// lineFailure is an engine error in the record or chunk that starts on
// row start, at row line of the input. An abort names start and no
// column: it lands between two of the engine's steps, where the engine
// often holds no token to place it at and falls back to 1:1 of the text
// it was parsing, so its own position says nothing.
func lineFailure(err error, line, start uint64, abort *AbortFlag) *Fail {
	f := engineFailure(err, abort)
	f.Column = 0
	if f.Code == CodeAborted {
		f.Row = start
		return f
	}
	f.Row = line
	if te, ok := err.(*tabnas.TabnasError); ok {
		f.Column = uint64(te.Col)
	}
	return f
}

// atRecord is a failure while the record or chunk that starts on row
// start was read: an abort that names no row names that one.
func atRecord(f *Fail, start uint64) *Fail {
	if f != nil && f.Code == CodeAborted && f.Row == 0 {
		f.Row = start
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
		records := newJSONRecords(l.reader, parser, l.limits.MaxRecordBytes)
		if flow, f := sendAll(g, EvArrayStart()); f != nil || flow == Stop {
			return flow, f
		}
		for {
			number, record, ok, f := records.next()
			if f != nil {
				return Continue, f
			}
			if !ok {
				break
			}
			value, err := parser.Parse(record)
			if err != nil {
				return Continue, lineFailure(err, number, number, l.abort)
			}
			if flow, f := WalkValue(value, g); f != nil || flow == Stop {
				return flow, atRecord(f, number)
			}
		}
		return sendAll(g, EvArrayEnd(), EvEnd())
	}

	parser, f := csvParser(l.format)
	if f != nil {
		return Continue, f
	}
	guard := lineGuard(parser, l.abort)
	chunks := newChunks(l.reader, parser, l.format, l.chunkBytes, l.limits.MaxRecordBytes)
	if flow, f := sendAll(g, EvArrayStart()); f != nil || flow == Stop {
		return flow, f
	}
	for {
		chunk, ok, f := chunks.next()
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
				return flow, atRecord(f, chunk.firstLine)
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

// lexed is the characters of chars, in a fixed order, while the engine
// lexes their kind, else none.
func lexed(on bool, chars map[rune]bool) []rune {
	if !on {
		return nil
	}
	out := make([]rune, 0, len(chars))
	for c, in := range chars {
		if in {
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func runeSet(runes []rune) map[rune]bool {
	set := make(map[rune]bool, len(runes))
	for _, c := range runes {
		set[c] = true
	}
	return set
}

// fixedTokens is the sources of a parser's fixed tokens, longest first,
// while it lexes them.
func fixedTokens(cfg *tabnas.LexConfig) []string {
	if !cfg.FixedLex {
		return nil
	}
	out := make([]string, 0, len(cfg.FixedSorted))
	for _, src := range cfg.FixedSorted {
		if src != "" {
			out = append(out, src)
		}
	}
	return out
}

// pieces is the input from a reader in pieces, each ending just past a
// line ending: one of the grammar's line characters, or a `\r\n`, which
// this engine's lexer reads as one line token under line.single
// (record.empty) as much as in a run. A line character inside one of
// the grammar's fixed tokens (a field separator such as "\n~") is the
// token's and ends no piece, and where a fixed token holds one, a piece
// takes the whole run of line characters the lexer reads as one token, so
// that no piece starts inside a line token. A record therefore never ends
// inside a piece, only at its end. Each piece carries the row the engine
// gives its first character: one more than the row characters before it.
type pieces struct {
	reader *bufio.Reader
	// Bytes read and not yet handed out: the piece being read starts at
	// start, and holds no line ending before scanned.
	pending []byte
	start   int
	scanned int
	eof     bool
	// The line characters, their UTF-8 forms, and the bytes that end a
	// form.
	line   []rune
	forms  [][]byte
	ends   [256]bool
	single bool
	// The fixed tokens that hold a line character, as UTF-8.
	spanning [][]byte
	// The line token being followed when what was read ran out, to pick
	// up where it stopped.
	following *following
	rows      map[rune]bool
	// Where the next piece starts: its row, and its byte offset in it.
	row    uint64
	offset uint64
}

type following struct {
	end   int
	token []rune
}

func newPieces(r io.Reader, line []rune, rows map[rune]bool, single bool, fixed []string) *pieces {
	br, ok := r.(*bufio.Reader)
	if !ok {
		br = bufio.NewReader(r)
	}
	p := &pieces{reader: br, line: line, single: single, rows: rows, row: 1}
	lineSet := runeSet(line)
	for _, c := range line {
		form := []byte(string(c))
		p.forms = append(p.forms, form)
		p.ends[form[len(form)-1]] = true
	}
	for _, token := range fixed {
		if strings.IndexFunc(token, func(c rune) bool { return lineSet[c] }) >= 0 {
			p.spanning = append(p.spanning, []byte(token))
		}
	}
	return p
}

// nextPiece is the next piece and the row it starts on; ok is false at
// the end of the input. It is taken from the reader a buffer at a time
// and refused with over's failure the moment it passes budget bytes, so a
// piece without an end costs one buffer past the budget and no more.
func (p *pieces) nextPiece(budget int, over func() *Fail) (uint64, string, bool, *Fail) {
	end := 0
	for {
		if e, ok := p.ending(); ok {
			end = e
			break
		}
		if len(p.pending)-p.start > budget {
			return 0, "", false, over()
		}
		more, f := p.more()
		if f != nil {
			return 0, "", false, f
		}
		if !more {
			// At the end of the input every question has its answer.
			if e, ok := p.ending(); ok {
				end = e
				break
			}
			if len(p.pending) == p.start {
				return 0, "", false, nil
			}
			end = len(p.pending)
			break
		}
	}
	if end-p.start > budget {
		return 0, "", false, over()
	}
	from, row := p.start, p.row
	p.start, p.scanned = end, end
	raw := p.pending[from:end]
	if !utf8.Valid(raw) {
		valid := 0
		for valid < len(raw) {
			r, size := utf8.DecodeRune(raw[valid:])
			if r == utf8.RuneError && size <= 1 {
				break
			}
			valid += size
		}
		badRow, offset := advance(p.rows, p.row, p.offset, string(raw[:valid]))
		return 0, "", false, InputFail(fmt.Sprintf("line %d is not UTF-8 from its byte %d", badRow, offset+1)).
			At(badRow, offset+1)
	}
	piece := string(raw)
	p.row, p.offset = advance(p.rows, p.row, p.offset, piece)
	return row, piece, true, nil
}

// more reads one more buffer of the input onto what is pending, letting
// go of what was handed out; false at the end of the input.
func (p *pieces) more() (bool, *Fail) {
	if p.eof {
		return false, nil
	}
	if p.start > 0 {
		p.pending = append(p.pending[:0], p.pending[p.start:]...)
		p.scanned -= p.start
		if p.following != nil {
			p.following.end -= p.start
		}
		p.start = 0
	}
	if _, err := p.reader.Peek(1); err != nil {
		if err == io.EOF {
			p.eof = true
			return false, nil
		}
		return false, InputFail(fmt.Sprintf("reading line %d: %v", p.row, err))
	}
	n := p.reader.Buffered()
	buf, _ := p.reader.Peek(n)
	p.pending = append(p.pending, buf...)
	_, _ = p.reader.Discard(n)
	return true, nil
}

// ending is where the piece being read ends: just past the first line
// ending after scanned; ok is false while what is read holds none, or
// cannot yet tell where one ends.
func (p *pieces) ending() (int, bool) {
	if f := p.following; f != nil {
		p.following = nil
		return p.follow(f.end, f.token)
	}
	from := max(p.scanned, p.start)
	for from < len(p.pending) {
		at := -1
		for i, b := range p.pending[from:] {
			if p.ends[b] {
				at = i
				break
			}
		}
		if at < 0 {
			break
		}
		end := from + at + 1
		k := -1
		for i, form := range p.forms {
			if bytes.HasSuffix(p.pending[p.start:end], form) {
				k = i
				break
			}
		}
		if k < 0 {
			from = end
			continue
		}
		at = end - len(p.forms[k])
		inside, sure := p.inFixed(at, k)
		if !sure {
			p.scanned = at
			return 0, false
		}
		if inside {
			from = end
			continue
		}
		return p.follow(end, []rune{p.line[k]})
	}
	p.scanned = len(p.pending)
	return 0, false
}

// inFixed is whether the kth line character, at at, lies inside one of
// the fixed tokens that hold a line character; sure is false while what
// is read cannot yet tell.
func (p *pieces) inFixed(at, k int) (inside, sure bool) {
	form := p.forms[k]
	unsure := false
	for _, token := range p.spanning {
		for j := 0; j < len(token); j++ {
			if !bytes.HasPrefix(token[j:], form) {
				continue
			}
			tokenStart := at - j
			if tokenStart < p.start || !bytes.Equal(p.pending[tokenStart:at], token[:j]) {
				continue
			}
			rest := token[j+len(form):]
			after := at + len(form)
			have := p.pending[after:min(after+len(rest), len(p.pending))]
			if !bytes.HasPrefix(rest, have) {
				continue
			}
			if len(have) == len(rest) {
				return true, true
			}
			unsure = unsure || !p.eof
		}
	}
	return false, !unsure
}

// follow is where the line token whose characters so far are token,
// ending at end, ends: under line.single, a `\n` after a `\r` and
// nothing else, as this engine's lexer reads one (`\r\n`, or any other
// line character alone); where a fixed token holds a line character,
// every line character, as the lexer reads a run; otherwise a `\n` after
// a `\r`, so that `\r\n` is one line ending and a record is held to its
// own line. ok is false while what is read cannot yet tell, with the
// token kept to pick up from once more is read, so a long run is followed
// once and not again from its start on every read.
func (p *pieces) follow(end int, token []rune) (int, bool) {
	if p.single {
		// The lexer takes the `\n` of a `\r\n` whether or not `\n` is a
		// line character of the grammar's.
		if len(token) == 1 && token[0] == '\r' {
			switch {
			case end < len(p.pending) && p.pending[end] == '\n':
				return end + 1, true
			case end == len(p.pending) && !p.eof:
				p.following = &following{end: end, token: token}
				return 0, false
			}
		}
		return end, true
	}
	for {
		grew, unsure := false, false
		for i, form := range p.forms {
			c := p.line[i]
			goesOn := len(p.spanning) > 0 || (len(token) == 1 && token[0] == '\r' && c == '\n')
			if !goesOn {
				continue
			}
			have := p.pending[end:min(end+len(form), len(p.pending))]
			if !bytes.HasPrefix(form, have) {
				continue
			}
			if len(have) == len(form) {
				end += len(form)
				token = append(token, c)
				grew = true
				break
			}
			unsure = unsure || !p.eof
		}
		if grew {
			continue
		}
		if unsure {
			p.following = &following{end: end, token: token}
			return 0, false
		}
		return end, true
	}
}

// advance is the row, and the byte offset in it, just after text, which
// starts at row and offset: a row character starts the next row.
func advance(rows map[rune]bool, row, offset uint64, text string) (uint64, uint64) {
	count := uint64(0)
	last := -1
	for i, c := range text {
		if rows[c] {
			count++
			last = i + utf8.RuneLen(c)
		}
	}
	if last < 0 {
		return row, offset + uint64(len(text))
	}
	return row + count, uint64(len(text) - last)
}

// jsonRecords is JSON Lines records from the input's pieces. A record
// ends at a line character outside a string, as the grammar's lexer ends
// one: a line character inside a string is the string's, which the
// grammar refuses there, so the record goes on into the next piece and
// fails whole as the grammar fails it. A record of nothing but the
// grammar's spaces is blank and skipped.
type jsonRecords struct {
	pieces   *pieces
	line     map[rune]bool
	space    map[rune]bool
	quotes   map[rune]bool
	escape   rune
	buf      []byte
	maxBytes int
}

func newJSONRecords(r io.Reader, parser *tabnas.Tabnas, maxBytes int) *jsonRecords {
	cfg := parser.Config()
	line := lexed(cfg.LineLex, cfg.LineChars)
	return &jsonRecords{
		pieces:   newPieces(r, line, cfg.RowChars, cfg.LineSingle, fixedTokens(cfg)),
		line:     runeSet(line),
		space:    runeSet(lexed(cfg.SpaceLex, cfg.SpaceChars)),
		quotes:   runeSet(lexed(cfg.StringLex, cfg.StringChars)),
		escape:   cfg.EscapeChar,
		maxBytes: maxBytes,
	}
}

// next is the next record that is not blank, without its line ending,
// and the row it starts on; ok is false at the end of the input. A record
// with its line ending longer than maxBytes fails with that limit.
func (j *jsonRecords) next() (uint64, string, bool, *Fail) {
	for {
		j.buf = j.buf[:0]
		var start uint64
		var quote rune
		inString, escaped := false, false
		end := -1
		for end < 0 {
			first := start
			if first == 0 {
				first = j.pieces.row
			}
			limit := j.maxBytes
			over := func() *Fail {
				return LimitFail("max_record_bytes", uint64(limit),
					fmt.Sprintf("line %d is longer than %d bytes", first, limit)).At(first, 1)
			}
			row, piece, ok, f := j.pieces.nextPiece(max(limit-len(j.buf), 0), over)
			if f != nil {
				return 0, "", false, f
			}
			if !ok {
				break
			}
			if start == 0 {
				start = row
			}
			held := len(j.buf)
			for i, c := range piece {
				if inString {
					switch {
					case escaped:
						escaped = false
					case c == j.escape:
						escaped = true
					case c == quote:
						inString = false
					}
				} else if j.quotes[c] {
					inString, quote = true, c
				} else if j.line[c] {
					end = held + i
					break
				}
			}
			j.buf = append(j.buf, piece...)
		}
		if start == 0 {
			return 0, "", false, nil
		}
		if end < 0 {
			end = len(j.buf)
		}
		record := string(j.buf[:end])
		for _, c := range record {
			if !j.space[c] {
				return start, record, true, nil
			}
		}
	}
}

// chunk is one CSV chunk ready to parse: whole records, the header
// prepended when it is not the chunk that carried it.
type chunk struct {
	text string
	// firstLine is the file line the chunk's own text, after
	// prefixLines, starts on.
	firstLine uint64
	// prefixLines is the lines in text before the chunk's own: those of
	// the prepended header in a chunk after the first, and none in the
	// first, which is the file's text from its start.
	prefixLines uint64
}

// failure is an engine error inside the chunk, at the file's line.
func (c *chunk) failure(err error, abort *AbortFlag) *Fail {
	rowInChunk := uint64(1)
	if te, ok := err.(*tabnas.TabnasError); ok && te.Row > 1 {
		rowInChunk = uint64(te.Row)
	}
	line := uint64(1) // the error is in the prepended header itself
	if rowInChunk > c.prefixLines {
		line = c.firstLine + rowInChunk - c.prefixLines - 1
	}
	return lineFailure(err, line, c.firstLine, abort)
}

// chunks cuts a CSV reader into chunks of whole records, where the
// grammar ends them.
type chunks struct {
	pieces  *pieces
	scanner *scanner
	// Whether the first record names the fields, and whether a blank line
	// is a record (record.empty): together, which record the header is.
	wantHeader  bool
	recordEmpty bool
	// The header record as the grammar reads it, once read: every chunk
	// after the first starts with it.
	header         *string
	chunkBytes     int
	maxRecordBytes int
	started        bool
	done           bool
}

func newChunks(r io.Reader, parser *tabnas.Tabnas, format LineFormat, chunkBytes, maxRecordBytes int) *chunks {
	lx := csvLexis(parser, format)
	cfg := parser.Config()
	return &chunks{
		pieces:         newPieces(r, lx.line, cfg.RowChars, cfg.LineSingle, lx.fixed),
		scanner:        &scanner{lexis: lx},
		wantHeader:     format.Header,
		recordEmpty:    jsTruthy(optionMap(format.Options["record"])["empty"]),
		chunkBytes:     chunkBytes,
		maxRecordBytes: maxRecordBytes,
	}
}

// next is the next chunk; ok is false once nothing is left to read. Every
// piece read goes into a chunk, the first one's header included, so every
// byte of the input is parsed.
func (c *chunks) next() (*chunk, bool, *Fail) {
	if c.done {
		return nil, false, nil
	}
	var text []byte
	if c.started && c.header != nil {
		text = append(text, *c.header...)
	}
	c.started = true
	prefixLines, _ := advance(c.pieces.rows, 0, 0, string(text))
	// The chunk closes only past body: past the header in front of it, or
	// in the first chunk past the header record itself.
	body := len(text)
	var firstLine uint64
	// The text a chunk cannot close inside, from its offset and row: one
	// record, or before the header everything up to its end.
	openAt, openRow := len(text), uint64(0)
	// Where the last record ended, which is where the next one starts.
	lastEnd := 0
	for {
		first := openRow
		if first == 0 {
			first = c.pieces.row
		}
		limit := c.maxRecordBytes
		over := func() *Fail {
			return LimitFail("max_record_bytes", uint64(limit), fmt.Sprintf(
				"the record starting at line %d is longer than %d bytes", first, limit)).At(first, 1)
		}
		number, piece, ok, f := c.pieces.nextPiece(max(limit-(len(text)-openAt), 0), over)
		if f != nil {
			return nil, false, f
		}
		if !ok {
			c.done = true
			break
		}
		if firstLine == 0 {
			firstLine = number
		}
		if openRow == 0 {
			openRow = number
		}
		at := len(text)
		text = append(text, piece...)
		seeking := c.wantHeader && c.header == nil
		foundFrom, foundTo := -1, -1
		ended := c.scanner.piece(piece, func(end int, content bool) {
			if seeking && foundTo < 0 && (content || c.recordEmpty) {
				foundFrom, foundTo = lastEnd, at+end
			}
			lastEnd = at + end
		})
		if foundTo >= 0 {
			h := string(text[foundFrom:foundTo])
			c.header = &h
			body = foundTo
		}
		// A chunk may close where a piece ends a record, once the header
		// is behind it.
		if ended && !(c.wantHeader && c.header == nil) {
			openAt, openRow = len(text), 0
			if len(text) > body && len(text) >= c.chunkBytes {
				break
			}
		}
	}
	if firstLine == 0 {
		// Nothing was left to read.
		return nil, false, nil
	}
	return &chunk{text: string(text), firstLine: firstLine, prefixLines: prefixLines}, true, nil
}

// optionMap is a nested option map, or nil.
func optionMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

// jsTruthy is JavaScript's `!!x`, which is how the CSV grammar reads its
// boolean options.
func jsTruthy(v any) bool {
	if v == nil {
		return false
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Bool:
		return rv.Bool()
	case reflect.String:
		return rv.Len() > 0
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int() != 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return rv.Uint() != 0
	case reflect.Float32, reflect.Float64:
		return rv.Float() != 0 && !math.IsNaN(rv.Float())
	}
	return true
}

// jsBool is a boolean option compared with `===`: ok is false for
// anything but a boolean.
func jsBool(v any) (value, ok bool) {
	if v == nil {
		return false, false
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Bool {
		return rv.Bool(), true
	}
	return false, false
}

// lexis is what decides where the CSV grammar ends a record, read from
// its parser's resolved options and from the options the plugin builds
// its quote matcher from, so a separator, quote, record separator or
// comment setting moves the chunker's cut as it moves the grammar. Each
// set is empty while the engine does not lex its kind.
type lexis struct {
	// Line characters: outside a token, each ends a record.
	line    []rune
	lineSet map[rune]bool
	// Whether each line character is a line token of its own, a `\r\n`
	// aside (record.empty), so that "\n\n" is two line tokens.
	single bool
	space  map[rune]bool
	// The fixed tokens, longest first: the field separator, and outside
	// strict mode the JSON structure characters.
	fixed []string
	// The RFC 4180 quote, while the grammar's own matcher reads it: a
	// doubled one inside is one quote, and a line character is text.
	quote    rune
	hasQuote bool
	// The engine's own string quotes, after that one: the escape takes the
	// next character whatever it is, and a line character is text only
	// inside the multi ones (a backtick).
	strings map[rune]bool
	multi   map[rune]bool
	escape  rune
	// The comment markers, longest first.
	comments []lexComment
	// What ends a run of text, as characters and as token starts.
	stops        map[rune]bool
	stopPrefixes []string
	// Whether the grammar ignores a space (outside strict mode) and a
	// comment, so that a record of nothing else is blank.
	spaceIgnored   bool
	commentIgnored bool
}

// lexComment is a comment marker: a line comment, which a line character
// ends without being part of, or a block comment and its end.
type lexComment struct {
	start, end string
	line       bool
}

func csvLexis(parser *tabnas.Tabnas, format LineFormat) *lexis {
	cfg := parser.Config()
	lx := &lexis{single: cfg.LineSingle, escape: cfg.EscapeChar}
	lx.line = lexed(cfg.LineLex, cfg.LineChars)
	lx.lineSet = runeSet(lx.line)
	lx.space = runeSet(lexed(cfg.SpaceLex, cfg.SpaceChars))
	lx.fixed = fixedTokens(cfg)
	if cfg.CommentLex {
		for _, start := range cfg.CommentLine {
			if start != "" {
				lx.comments = append(lx.comments, lexComment{start: start, line: true})
			}
		}
		for _, pair := range cfg.CommentBlock {
			if pair[0] != "" {
				lx.comments = append(lx.comments, lexComment{start: pair[0], end: pair[1]})
			}
		}
		sort.SliceStable(lx.comments, func(i, j int) bool {
			return len(lx.comments[i].start) > len(lx.comments[j].start)
		})
	}
	// The RFC 4180 matcher as the plugin installs it: on in strict mode
	// unless string.csv is false, off otherwise unless it is true, and
	// inert for a quote that is not one UTF-16 code unit.
	strict := true
	if v, ok := format.Options["strict"]; ok && v != nil {
		strict = jsTruthy(v)
	}
	stringOpts := optionMap(format.Options["string"])
	csvOpt, csvSet := jsBool(stringOpts["csv"])
	matcher := (strict && !(csvSet && !csvOpt)) || (!strict && csvSet && csvOpt)
	quote := `"`
	if q, ok := stringOpts["quote"]; ok && q != nil {
		quote, _ = q.(string)
	}
	if r, size := utf8.DecodeRuneInString(quote); matcher && size > 0 && size == len(quote) && r <= 0xFFFF {
		lx.quote, lx.hasQuote = r, true
	}
	lx.strings = runeSet(lexed(cfg.StringLex, cfg.StringChars))
	lx.multi = runeSet(lexed(true, cfg.MultiChars))
	ignored := runeSetTins(parser.TokenSet("IGNORE"))
	lx.spaceIgnored = ignored[tabnas.TinSP]
	lx.commentIgnored = ignored[tabnas.TinCM]
	// What the engine's text matcher stops at; it refuses the two Unicode
	// line separators while it lexes lines.
	lx.stops = make(map[rune]bool)
	for c := range lx.space {
		lx.stops[c] = true
	}
	for c := range lx.lineSet {
		lx.stops[c] = true
	}
	for c, on := range cfg.EnderChars {
		if on {
			lx.stops[c] = true
		}
	}
	if cfg.LineLex {
		lx.stops[' '], lx.stops[' '] = true, true
	}
	lx.stopPrefixes = append(lx.stopPrefixes, lx.fixed...)
	for _, cm := range lx.comments {
		lx.stopPrefixes = append(lx.stopPrefixes, cm.start)
	}
	for _, seq := range cfg.EnderSeqs {
		if seq != "" {
			lx.stopPrefixes = append(lx.stopPrefixes, seq)
		}
	}
	return lx
}

func runeSetTins(tins []tabnas.Tin) map[tabnas.Tin]bool {
	set := make(map[tabnas.Tin]bool, len(tins))
	for _, tin := range tins {
		set[tin] = true
	}
	return set
}

// fixedAt is the length of the longest fixed token at the head of rest.
func (lx *lexis) fixedAt(rest string) int {
	for _, src := range lx.fixed {
		if strings.HasPrefix(rest, src) {
			return len(src)
		}
	}
	return 0
}

// commentAt is the comment that starts at the head of rest, by its
// index, or -1.
func (lx *lexis) commentAt(rest string) int {
	for i, cm := range lx.comments {
		if strings.HasPrefix(rest, cm.start) {
			return i
		}
	}
	return -1
}

// lineRun is the length of the line token at the head of rest: under
// line.single a `\r\n` or one line character, as this engine's lexer reads
// one, and otherwise every line character in a run.
func (lx *lexis) lineRun(rest string) int {
	if lx.single {
		c, n := utf8.DecodeRuneInString(rest)
		if c == '\r' && strings.HasPrefix(rest[n:], "\n") {
			return n + 1
		}
		return n
	}
	n := 0
	for i, c := range rest {
		if !lx.lineSet[c] {
			break
		}
		n = i + utf8.RuneLen(c)
	}
	return n
}

// endsText is whether a run of text stops at the head of rest, whose
// first character is c.
func (lx *lexis) endsText(rest string, c rune) bool {
	if lx.stops[c] {
		return true
	}
	for _, prefix := range lx.stopPrefixes {
		if strings.HasPrefix(rest, prefix) {
			return true
		}
	}
	return false
}

// scanAt is where the record scanner is, between two characters.
type scanAt uint8

const (
	// atStart is where the lexer starts a token.
	atStart scanAt = iota
	// atText is inside text, a number or a keyword: a run that only what
	// endsText names ends, so a quote inside one is text.
	atText
	// atQuoted is inside an RFC 4180 quoted field.
	atQuoted
	// atString is inside one of the engine's own strings.
	atString
	// atComment is inside a comment.
	atComment
)

// scanner follows CSV text through the grammar's tokens, a piece at a
// time, to find where its records end.
type scanner struct {
	lexis *lexis
	at    scanAt
	// The open string's quote and whether it may span lines, or the open
	// comment, by its index.
	quote   rune
	multi   bool
	comment int
	// Whether the record so far holds anything the grammar does not
	// ignore, so that it is not blank.
	content bool
}

// piece scans one piece of the input, which ends just past a line ending
// unless it is the last, calling end(offset, content) where each record
// in it ends: offset is just past the line token that ends it, and
// content says whether it held anything the grammar does not ignore. It
// returns whether the piece ends a record, so that a chunk may close
// after it.
func (s *scanner) piece(text string, end func(int, bool)) bool {
	lx := s.lexis
	ended := false
	for i := 0; i < len(text); {
		c, size := utf8.DecodeRuneInString(text[i:])
		rest := text[i:]
		ended = false
		switch s.at {
		case atStart:
			// In the lexer's order: the RFC 4180 matcher, fixed tokens,
			// space, lines, strings, comments, and then a run of text.
			if lx.hasQuote && c == lx.quote {
				s.at, s.content = atQuoted, true
				i += size
			} else if n := lx.fixedAt(rest); n > 0 {
				s.content = true
				i += n
			} else if lx.space[c] {
				s.content = s.content || !lx.spaceIgnored
				i += size
			} else if lx.lineSet[c] {
				i += lx.lineRun(rest)
				end(i, s.content)
				s.content, ended = false, true
			} else if lx.strings[c] {
				s.at, s.quote, s.multi, s.content = atString, c, lx.multi[c], true
				i += size
			} else if k := lx.commentAt(rest); k >= 0 {
				s.at, s.comment = atComment, k
				s.content = s.content || !lx.commentIgnored
				i += len(lx.comments[k].start)
			} else {
				// A character no token starts with (an ender) is one the
				// grammar refuses, and lexing resumes after it.
				if !lx.endsText(rest, c) {
					s.at = atText
				}
				s.content = true
				i += size
			}
		case atText:
			if lx.endsText(rest, c) {
				s.at = atStart
			} else {
				i += size
			}
		case atQuoted:
			i += size
			if c == lx.quote {
				if strings.HasPrefix(text[i:], string(c)) {
					i += size
				} else {
					s.at = atStart
				}
			}
		case atString:
			switch {
			case c == s.quote:
				s.at = atStart
				i += size
			case c == lx.escape:
				i += size
				if i < len(text) {
					_, n := utf8.DecodeRuneInString(text[i:])
					i += n
				}
			case !s.multi && lx.lineSet[c]:
				// The grammar refuses the string here, so the line
				// character is read as a line.
				s.at = atStart
			default:
				i += size
			}
		case atComment:
			cm := lx.comments[s.comment]
			switch {
			case !cm.line && cm.end != "" && strings.HasPrefix(rest, cm.end):
				s.at = atStart
				i += len(cm.end)
			case cm.line && lx.lineSet[c]:
				s.at = atStart
			default:
				i += size
			}
		}
	}
	return ended
}
