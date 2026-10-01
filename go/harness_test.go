// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

// The harness behind the shared fixtures in ../test/spec, which every
// runtime of this crate runs. What a row means is docs/reference.md's
// "Shared fixtures": which source its `grammar` and `mode` name, what
// each JSON column decodes to, and how the result is encoded as the
// fixture's expected JSON. This reproduces rs/tests/common/mod.rs.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	tabnascsv "github.com/tabnas/csv/go"
	tabnasfeed "github.com/tabnas/feed/go"
	tabnasini "github.com/tabnas/ini/go"
	tabnasjson "github.com/tabnas/json/go"
	tabnasjson5 "github.com/tabnas/json5/go"
	tabnasjsonc "github.com/tabnas/jsonc/go"
	jsonic "github.com/tabnas/jsonic/go"
	tabnasjsonl "github.com/tabnas/jsonl/go"
	tabnasmarkdown "github.com/tabnas/markdown/go"
	tabnas "github.com/tabnas/parser/go"
	support "github.com/tabnas/support/go"
	tabnastoml "github.com/tabnas/toml/go"
	tabnasxml "github.com/tabnas/xml/go"
	tabnasyaml "github.com/tabnas/yaml/go"
	tabnaszon "github.com/tabnas/zon/go"
)

// specFixtures is every fixture file and so every runner: a new file
// without a runner fails TestEveryFixtureHasARunner rather than passing
// unread.
var specFixtures = []string{"events.tsv", "limits.tsv", "lines.tsv", "route.tsv", "scan.tsv", "table.tsv"}

// rustIncremental is the Rust crate's verified list, which the fixtures'
// incremental rows were written against: a row that streams one of these
// grammars incrementally needs the adapter.
var rustIncremental = map[string]bool{
	"json": true, "json5": true, "jsonc": true, "jsonic": true,
	"jsonl": true, "markdown": true, "yaml": true, "zon": true,
}

// needsAdapter reports whether a row runs the rule-event adapter: an
// incremental run of a grammar the Rust list verifies, or JSON Lines on
// the line source's incremental path. Such a row is skipped, by name, in
// a build without the adapter.
func needsAdapter(row *support.Row) bool {
	switch row.Named("mode") {
	case "incremental":
		return rustIncremental[row.Named("grammar")]
	case "lines-incremental":
		return row.Named("grammar") == "jsonl"
	}
	return false
}

func specDir(t *testing.T) string {
	t.Helper()
	dir, err := support.FindSpecDir("")
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

// makeGrammar is the grammar a row (or the differential suite) names.
func makeGrammar(name string) *tabnas.Tabnas {
	use := func(plugin tabnas.Plugin, defaults map[string]any) *tabnas.Tabnas {
		j := jsonic.Make()
		var err error
		if defaults != nil {
			err = j.UseDefaults(plugin, defaults)
		} else {
			err = j.Use(plugin)
		}
		if err != nil {
			panic(fmt.Sprintf("grammar %s: %v", name, err))
		}
		return j
	}
	switch name {
	case "json":
		return tabnasjson.Make()
	case "jsonl":
		return tabnasjsonl.Make()
	case "json5":
		return use(tabnasjson5.Json5, tabnasjson5.Defaults())
	case "jsonc":
		return use(tabnasjsonc.Jsonc, nil)
	case "jsonic":
		return jsonic.Make()
	case "yaml":
		return tabnasyaml.MakeJsonic()
	case "zon":
		return tabnaszon.MakeJsonic()
	case "csv":
		j, err := tabnascsv.Make()
		if err != nil {
			panic(err)
		}
		return j
	case "toml":
		return tabnastoml.MakeJsonic()
	case "ini":
		return tabnasini.MakeJsonic()
	case "xml":
		return use(tabnasxml.Xml, tabnasxml.Defaults)
	case "markdown":
		return tabnasmarkdown.Make()
	case "feed":
		return use(tabnasfeed.Feed, tabnasfeed.Defaults)
	}
	panic(fmt.Sprintf("no grammar %q in the fixture harness", name))
}

// jsonCell decodes a JSON column, numbers kept as json.Number; nil for
// an empty cell.
func jsonCell(t testing.TB, row *support.Row, name string) any {
	cell := row.Named(name)
	if cell == "" {
		return nil
	}
	dec := json.NewDecoder(strings.NewReader(cell))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		t.Fatalf("%s: column %s is not JSON: %v: %s", row.Where(), name, err, cell)
	}
	return v
}

func asInt(t testing.TB, v any) int {
	n, ok := v.(json.Number)
	if !ok {
		t.Fatalf("not a number: %v", v)
	}
	i, err := n.Int64()
	if err != nil || i < 0 {
		t.Fatalf("not a non-negative integer: %v", v)
	}
	return int(i)
}

// selectorOf builds a selector from its JSON steps.
func selectorOf(t testing.TB, v any) Selector {
	steps, ok := v.([]any)
	if !ok {
		t.Fatalf("a selector is an array of steps: %v", v)
	}
	sel := Root()
	for _, step := range steps {
		switch s := step.(type) {
		case string:
			sel = sel.Property(s)
		case json.Number:
			sel = sel.Index(asInt(t, s))
		case map[string]any:
			switch s["each"] {
			case "index":
				sel = sel.EachIndex()
			case "member":
				sel = sel.EachMember()
			default:
				t.Fatalf("not an each step: %v", s)
			}
		default:
			t.Fatalf("not a selector step: %v", step)
		}
	}
	return sel
}

// segmentsOf builds a concrete path from its JSON segments.
func segmentsOf(t testing.TB, v any) []Segment {
	items, ok := v.([]any)
	if !ok {
		t.Fatalf("a path is an array of segments: %v", v)
	}
	out := make([]Segment, 0, len(items))
	for _, item := range items {
		switch s := item.(type) {
		case string:
			out = append(out, KeySegment(s))
		case json.Number:
			out = append(out, IndexSegment(asInt(t, s)))
		default:
			t.Fatalf("not a path segment: %v", item)
		}
	}
	return out
}

// limitsOf is DefaultLimits with the row's `limits` column over it.
func limitsOf(t testing.TB, row *support.Row) Limits {
	limits := DefaultLimits()
	cell := jsonCell(t, row, "limits")
	if cell == nil {
		return limits
	}
	for name, value := range cell.(map[string]any) {
		n := asInt(t, value)
		switch name {
		case "max_depth":
			limits.MaxDepth = n
		case "max_key_bytes":
			limits.MaxKeyBytes = n
		case "max_scalar_bytes":
			limits.MaxScalarBytes = n
		case "max_metadata_bytes":
			limits.MaxMetadataBytes = n
		case "max_columns":
			limits.MaxColumns = n
		case "max_record_bytes":
			limits.MaxRecordBytes = n
		case "max_capture_bytes":
			limits.MaxCaptureBytes = n
		case "max_output_bytes":
			u := uint64(n)
			limits.MaxOutputBytes = &u
		default:
			t.Fatalf("%s: no limit %q", row.Where(), name)
		}
	}
	return limits
}

func duplicatesOf(t testing.TB, row *support.Row) Duplicates {
	switch row.Named("duplicates") {
	case "", "reject":
		return Reject
	case "last_wins":
		return LastWins
	case "first_wins":
		return FirstWins
	}
	t.Fatalf("%s: no duplicates policy %q", row.Where(), row.Named("duplicates"))
	return Reject
}

func pruneOf(t testing.TB, row *support.Row) Prune {
	switch v := jsonCell(t, row, "prune").(type) {
	case nil:
		return Prune{}
	case string:
		if v == "all" {
			return Prune{Kind: PruneAllArrays}
		}
	}
	return PruneUnderSelector(selectorOf(t, jsonCell(t, row, "prune")))
}

// lineOptions is the line format and chunk size from the row's
// `options` column.
func lineOptions(t testing.TB, row *support.Row) (LineFormat, int, bool) {
	opts, _ := jsonCell(t, row, "options").(map[string]any)
	chunk, hasChunk := 0, false
	if c, ok := opts["chunk_bytes"]; ok {
		chunk, hasChunk = asInt(t, c), true
	}
	switch row.Named("grammar") {
	case "jsonl":
		return JSONLFormat(), chunk, hasChunk
	case "csv":
		options := map[string]any{}
		header := true
		for _, name := range []string{"object", "strict", "number", "value", "trim"} {
			if b, ok := opts[name].(bool); ok {
				options[name] = b
			}
		}
		if b, ok := opts["header"].(bool); ok {
			header = b
		}
		return CSVFormatWith(header, options), chunk, hasChunk
	}
	t.Fatalf("%s: no line format for grammar %q", row.Where(), row.Named("grammar"))
	return LineFormat{}, 0, false
}

// drive runs the row's source into sink.
func drive(t testing.TB, row *support.Row, input string, sink Sink) (Flow, *Fail) {
	name := row.Named("grammar")
	limits := limitsOf(t, row)
	switch mode := row.Named("mode"); mode {
	case "materialize", "incremental":
		m := MaterializeMode()
		if mode == "incremental" {
			m = IncrementalMode(pruneOf(t, row))
		}
		return NewParserSource(makeGrammar(name), input).Grammar(name).Mode(m).Limits(limits).Run(sink)
	case "value":
		value, err := makeGrammar(name).Parse(input)
		if err != nil {
			return Continue, failFromError(err)
		}
		return ValueSource{Value: value}.Run(sink)
	case "lines", "lines-incremental":
		format, chunk, hasChunk := lineOptions(t, row)
		src := NewLinesSource(bytes.NewReader([]byte(input)), format).Limits(limits)
		if hasChunk {
			src = src.ChunkBytes(chunk)
		}
		if mode == "lines" {
			return src.Run(sink)
		}
		return src.RunIncremental(sink)
	}
	t.Fatalf("%s: no mode %q", row.Where(), row.Named("mode"))
	return Continue, nil
}

// The encodings.

func numberValue(value float64, lexeme string) any {
	var l any
	if lexeme != "" {
		l = lexeme
	}
	return []any{"number", value, l}
}

// eventValue is one event's encoding.
func eventValue(ev Event) any {
	switch ev.Kind {
	case Key:
		return []any{"key", ev.Text}
	case Bool:
		return []any{"bool", ev.Bool}
	case Number:
		return numberValue(ev.Value, ev.Lexeme)
	case String:
		return []any{"string", ev.Text}
	}
	return []any{ev.Kind.String()}
}

func eventsValue(events []Event) []any {
	out := make([]any, len(events))
	for i, ev := range events {
		out[i] = eventValue(ev)
	}
	return out
}

// datumValue is a retained value's encoding.
func datumValue(d *Datum) any {
	switch d.Kind {
	case DatumNull:
		return nil
	case DatumBool:
		return d.Bool
	case DatumNumber:
		return numberValue(d.Value, d.Lexeme)
	case DatumString:
		return d.Text
	case DatumArray:
		out := []any{"array"}
		for i := range d.Items {
			out = append(out, datumValue(&d.Items[i]))
		}
		return out
	}
	out := []any{"object"}
	for i := range d.Members {
		out = append(out, []any{d.Members[i].Key, datumValue(&d.Members[i].Value)})
	}
	return out
}

// cellValue is a table cell's encoding.
func cellValue(c Cell) any {
	switch c.Kind {
	case CellBool:
		return c.Bool
	case CellNumber:
		return numberValue(c.Value, c.Lexeme)
	case CellString:
		return c.Text
	case CellMissing:
		return []any{"missing"}
	}
	return nil
}

// failed is a run that failed: the failure, and for an event stream the
// events that left before it.
type failed struct {
	fail   *Fail
	prefix []any
	hasPre bool
}

// rowFailure is a failure as the shared runner sees it: the code (with
// any difference from the row's path, limit and prefix columns
// attached, so the row fails with it in its report) and the position.
type rowFailure struct {
	code     string
	message  string
	row, col int
}

func (r *rowFailure) Error() string { return r.code + ": " + r.message }
func (r *rowFailure) Code() string  { return r.code }
func (r *rowFailure) Row() int      { return r.row }
func (r *rowFailure) Col() int      { return r.col }

func toFailure(t testing.TB, fd *failed, row *support.Row) error {
	f := fd.fail
	var differences []string
	if want := row.Named("path"); want != "" {
		got := f.Path
		if got == "" {
			got = "<none>"
		}
		if got != want {
			differences = append(differences, fmt.Sprintf("path %s, the fixture pins %s", got, want))
		}
	}
	if want := row.Named("limit"); want != "" {
		got := "<none>"
		if f.Limit != nil {
			got = f.Limit.Name
		}
		if got != want {
			differences = append(differences, fmt.Sprintf("limit %s, the fixture pins %s", got, want))
		}
	}
	if want := row.Named("prefix"); want != "" {
		wantV, err := support.ParseExpect(want)
		if err != nil {
			t.Fatalf("%s: prefix is not JSON: %v", row.Where(), err)
		}
		if !fd.hasPre || !support.EqualValue(fd.prefix, wantV) {
			got := "<none>"
			if fd.hasPre {
				got = support.FormatValue(fd.prefix)
			}
			differences = append(differences, fmt.Sprintf("prefix %s, the fixture pins %s", got, want))
		}
	}
	code := f.Code.String()
	if len(differences) > 0 {
		code = code + " (" + strings.Join(differences, "; ") + ")"
	}
	return &rowFailure{code: code, message: f.Error(), row: int(f.Row), col: int(f.Column)}
}

// The stages.

func eventsStage(t testing.TB, row *support.Row, input string) (any, *failed) {
	var rec Recorder
	_, f := drive(t, row, input, &rec)
	if f != nil {
		return nil, &failed{fail: f, prefix: eventsValue(rec.Events), hasPre: true}
	}
	return eventsValue(rec.Events), nil
}

// deliveries records what a route delivered: [tag, path] for an observed
// capture, [tag, path, value] for a materialized one, and "end".
type deliveries struct{ out []any }

func (d *deliveries) Selected(s Selected) (Flow, *Fail) {
	entry := []any{s.Tag, s.Path.String()}
	if s.Value != nil {
		entry = append(entry, datumValue(s.Value))
	}
	d.out = append(d.out, entry)
	return Continue, nil
}

func (d *deliveries) End() (Flow, *Fail) {
	d.out = append(d.out, "end")
	return Continue, nil
}

func capturesOf(t testing.TB, row *support.Row) []CaptureSpec {
	cell := jsonCell(t, row, "captures")
	if cell == nil {
		return nil
	}
	var specs []CaptureSpec
	for _, item := range cell.([]any) {
		spec := item.(map[string]any)
		tag, _ := spec["tag"].(string)
		sel := selectorOf(t, spec["select"])
		switch spec["mode"] {
		case nil, "materialize":
			specs = append(specs, MaterializeSpec(tag, sel))
		case "observe":
			specs = append(specs, ObserveSpec(tag, sel))
		default:
			t.Fatalf("%s: no capture mode %v", row.Where(), spec["mode"])
		}
	}
	return specs
}

func routeStage(t testing.TB, row *support.Row, input string) (any, *failed) {
	d := &deliveries{out: []any{}}
	router, f := NewRouter(capturesOf(t, row), limitsOf(t, row), duplicatesOf(t, row), NewMetrics(), d)
	if f != nil {
		return nil, &failed{fail: f}
	}
	if _, f := drive(t, row, input, router); f != nil {
		return nil, &failed{fail: f}
	}
	return d.out, nil
}

// tableLog records TableRows/1 in order.
type tableLog struct{ out []any }

func (l *tableLog) TableEvent(ev TableEvent) (Flow, *Fail) {
	switch ev.Kind {
	case TableSchema:
		labels := make([]any, len(ev.Columns))
		for i, c := range ev.Columns {
			labels[i] = c.Label
		}
		l.out = append(l.out, []any{"schema", labels})
	case TableRow:
		cells := make([]any, len(ev.Cells))
		for i, c := range ev.Cells {
			cells[i] = cellValue(c)
		}
		l.out = append(l.out, []any{"row", cells})
	case TableEnd:
		l.out = append(l.out, []any{"end"})
	}
	return Continue, nil
}

func bindingOf(t testing.TB, row *support.Row) TableBinding {
	cell, ok := jsonCell(t, row, "binding").(map[string]any)
	if !ok {
		t.Fatalf("%s: a table row has a binding", row.Where())
	}
	rows := selectorOf(t, cell["rows"])
	switch s := cell["schema"].(type) {
	case string:
		if s == "infer" {
			return TableBinding{Schema: InferSchema(), Rows: rows}
		}
	case map[string]any:
		if m, ok := s["metadata"]; ok {
			return TableBinding{Schema: MetadataSchema(selectorOf(t, m), ColumnFromMeta), Rows: rows}
		}
		if cols, ok := s["static"].([]any); ok {
			var bound []BoundColumn
			for _, c := range cols {
				col := c.(map[string]any)
				label, _ := col["label"].(string)
				bc := NewBoundColumn(label, segmentsOf(t, col["source"]))
				switch col["missing"] {
				case nil, "missing":
				case "null":
					bc.Missing = MissingNull
				case "error":
					bc.Missing = MissingError
				default:
					t.Fatalf("no missing policy %v", col["missing"])
				}
				bound = append(bound, bc)
			}
			return TableBinding{Schema: StaticSchema(bound...), Rows: rows}
		}
	}
	t.Fatalf("%s: not a schema: %v", row.Where(), cell["schema"])
	return TableBinding{}
}

func tableStage(t testing.TB, row *support.Row, input string) (any, *failed) {
	log := &tableLog{out: []any{}}
	tf, f := NewTableFromJSON(bindingOf(t, row), limitsOf(t, row), duplicatesOf(t, row), NewMetrics(), log)
	if f != nil {
		return nil, &failed{fail: f}
	}
	if _, f := drive(t, row, input, tf); f != nil {
		return nil, &failed{fail: f}
	}
	return log.out, nil
}

func stagedStage(t testing.TB, row *support.Row, input string) (any, *failed) {
	switch row.Named("stage") {
	case "events":
		return eventsStage(t, row, input)
	case "route":
		return routeStage(t, row, input)
	case "table":
		return tableStage(t, row, input)
	}
	t.Fatalf("%s: no stage %q", row.Where(), row.Named("stage"))
	return nil, nil
}

// scanItem is one scan.tsv operation's item.
type scanItem struct {
	add  int64
	emit []string
	fail Code
	kind int // 0 add, 1 emit, 2 fail
}

// scanStage drives a scan.tsv script through ScanEmit with a running
// sum.
func scanStage(t testing.TB, row *support.Row, script string) (any, *failed) {
	dec := json.NewDecoder(strings.NewReader(script))
	dec.UseNumber()
	var ops []any
	if err := dec.Decode(&ops); err != nil {
		t.Fatalf("%s: the script is not JSON: %v", row.Where(), err)
	}
	stopOn := row.Named("stop_on")
	out := []any{}
	scan := NewScanEmit(int64(0),
		func(sum int64, item scanItem) (Transition[int64, string], *Fail) {
			switch item.kind {
			case 0:
				return Emit(sum+item.add, fmt.Sprintf("+%d", item.add)), nil
			case 1:
				return Transition[int64, string]{State: sum, Outputs: item.emit}, nil
			}
			return Transition[int64, string]{}, NewFail(item.fail, "the step failed")
		},
		func(sum int64) ([]string, *Fail) { return []string{fmt.Sprintf("=%d", sum)}, nil },
		func(o string) (Flow, *Fail) {
			out = append(out, o)
			if o == stopOn {
				return Stop, nil
			}
			return Continue, nil
		})
	flows := []any{}
	for _, op := range ops {
		var flow Flow
		var f *Fail
		switch o := op.(type) {
		case string:
			if o != "finish" {
				t.Fatalf("%s: not a scan op: %v", row.Where(), op)
			}
			flow, f = scan.Finish()
		case json.Number:
			n, _ := o.Int64()
			flow, f = scan.Item(scanItem{add: n})
		case map[string]any:
			if e, ok := o["emit"].([]any); ok {
				strs := make([]string, len(e))
				for i, s := range e {
					strs[i], _ = s.(string)
				}
				flow, f = scan.Item(scanItem{kind: 1, emit: strs})
			} else if c, ok := o["fail"].(string); ok {
				code, ok := ParseCode(c)
				if !ok {
					t.Fatalf("%s: no code %q", row.Where(), c)
				}
				flow, f = scan.Item(scanItem{kind: 2, fail: code})
			} else {
				t.Fatalf("%s: not a scan op: %v", row.Where(), op)
			}
		default:
			t.Fatalf("%s: not a scan op: %v", row.Where(), op)
		}
		if f != nil {
			return nil, &failed{fail: f}
		}
		flows = append(flows, flow.String())
	}
	return map[string]any{"out": out, "flows": flows}, nil
}
