// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

// Ported from the Rust unit tests in rs/src/{matcher,route,table,
// table_from_json,scan}.rs and rs/src/source/guard.rs.

import (
	"fmt"
	"strings"
	"testing"
)

type pathHit struct {
	path string
	ids  string
}

func matchAll(t *testing.T, selectors []Selector, events []Event) []pathHit {
	t.Helper()
	m := NewMatcher(selectors)
	var out []pathHit
	for _, ev := range events {
		hit, f := m.Event(ev)
		if f != nil {
			t.Fatal(f)
		}
		if hit.Begins > 0 {
			out = append(out, pathHit{m.Path(hit.Depth).String(), fmt.Sprint(m.Begins())})
		}
	}
	if !m.Ended() {
		t.Fatal("not ended")
	}
	return out
}

func TestMatcherSelectors(t *testing.T) {
	cases := []struct {
		selectors []Selector
		doc       string
		want      string
	}{
		{[]Selector{Root().Property("a").Property("b")}, `{"a":{"b":[1,2],"c":3},"b":{"b":4}}`, "[{.a.b [0]}]"},
		{[]Selector{Root().Property("xs").EachIndex()}, `{"xs":[10,{"y":1},[2]],"ys":[9]}`, "[{.xs[0] [0]} {.xs[1] [0]} {.xs[2] [0]}]"},
		{[]Selector{Root().EachMember()}, `{"a":1,"odd key":{"z":2},"c":[3]}`, `[{.a [0]} {."odd key" [0]} {.c [0]}]`},
		{[]Selector{Root().Index(1)}, `[[0,1],[2,3],[4,5]]`, "[{[1] [0]}]"},
		{[]Selector{Root()}, `{"a":[1]}`, "[{. [0]}]"},
		{[]Selector{Root()}, `42`, "[{. [0]}]"},
		{[]Selector{Root().Property("response").Property("metadata"), Root().Property("response").Property("records").EachIndex()},
			`{"response":{"metadata":[1],"records":[{"id":1},{"id":2}]},"records":[0]}`,
			"[{.response.metadata [0]} {.response.records[0] [1]} {.response.records[1] [1]}]"},
		{[]Selector{Root().Property("a"), Root().EachMember()}, `{"a":1,"b":2}`, "[{.a [0 1]} {.b [1]}]"},
		{[]Selector{Root().Property("missing").EachIndex(), Root().Index(5)}, `{"a":[1,2,3]}`, "[]"},
		{[]Selector{Root().EachIndex().EachIndex()}, `[[1,2],[],[[3]]]`, "[{[0][0] [0]} {[0][1] [0]} {[2][0] [0]}]"},
		{[]Selector{Root().Index(0), Root().Property("0")}, `{"0":[7]}`, `[{."0" [1]}]`},
		{[]Selector{Root().EachIndex().Property("k")}, `[{"k":1,"other":2},{"other":3},{"k":4}]`, "[{[0].k [0]} {[2].k [0]}]"},
	}
	for _, c := range cases {
		if got := fmt.Sprint(matchAll(t, c.selectors, treeEvents(t, c.doc))); got != c.want {
			t.Errorf("%s: %s, want %s", c.doc, got, c.want)
		}
	}
}

func TestACloseReportsThePathOfTheContainerThatClosed(t *testing.T) {
	m := NewMatcher(nil)
	var closes []string
	for _, ev := range treeEvents(t, `{"a":[{"b":1},{"c":[]}]}`) {
		hit, _ := m.Event(ev)
		if hit.Kind == HitClose {
			closes = append(closes, m.Path(hit.Depth).String())
		}
	}
	if fmt.Sprint(closes) != "[.a[0] .a[1].c .a[1] .a .]" {
		t.Fatal(closes)
	}
	m = NewMatcher(nil)
	var seen []string
	for _, ev := range treeEvents(t, `[1,[2]]`) {
		hit, _ := m.Event(ev)
		seen = append(seen, fmt.Sprintf("%d/%d", hit.Kind, hit.Depth))
	}
	if fmt.Sprint(seen) != "[1/0 2/1 1/1 2/2 3/1 3/0 4/0]" {
		t.Fatal(seen)
	}
}

func TestMalformedStreamsAreProtocolErrors(t *testing.T) {
	one := EvNumber(1)
	os, oe, as, ae, k, end := EvObjectStart(), EvObjectEnd(), EvArrayStart(), EvArrayEnd(), EvKey, EvEnd()
	for _, c := range [][]Event{
		{k("a")}, {os, one}, {os, k("a"), k("b")}, {os, k("a"), oe}, {as, oe}, {os, ae},
		{ae}, {one, one}, {end}, {as, end}, {one, end, end}, {as, k("a")},
	} {
		m := NewMatcher([]Selector{Root()})
		var got *Fail
		for _, ev := range c {
			if _, f := m.Event(ev); f != nil {
				got = f
				break
			}
		}
		if got == nil || got.Code != CodeProtocolOrderError {
			t.Errorf("%v: %v", c, got)
		}
	}
}

// workedExample is the spec's worked example: metadata first, then
// records whose member order differs, one number with a lexeme.
func workedExample() []Event {
	k, s := EvKey, EvString
	n := EvNumberLexeme
	os, oe, as, ae := EvObjectStart(), EvObjectEnd(), EvArrayStart(), EvArrayEnd()
	return []Event{
		os, k("response"), os, k("metadata"), os, k("fields"), as,
		os, k("title"), s("Identifier"), k("path"), as, s("id"), ae, oe,
		os, k("title"), s("Full name"), k("path"), as, s("person"), s("name"), ae, oe,
		os, k("title"), s("Balance"), k("path"), as, s("account"), s("balance"), ae, oe,
		ae, oe,
		k("payload"), os, k("deep"), os, k("records"), as,
		os, k("id"), n(123, "123"), k("person"), os, k("name"), s("Alice"), oe,
		k("account"), os, k("balance"), n(50.25, "50.25"), oe, oe,
		os, k("account"), os, k("balance"), EvNumber(72), oe, k("id"), n(456, "456"),
		k("person"), os, k("name"), s("Bob"), oe, oe,
		ae, oe, oe, oe, oe, EvEnd(),
	}
}

func metadataSelector() Selector {
	return Root().Property("response").Property("metadata").Property("fields")
}

func recordsSelector() Selector {
	return Root().Property("response").Property("payload").Property("deep").Property("records").EachIndex()
}

func newTestRouter(t *testing.T, specs []CaptureSpec, rec RouteSink) (*Router, *Fail) {
	return NewRouter(specs, DefaultLimits(), Reject, NewMetrics(), rec)
}

func TestTheWorkedExampleDeliversMetadataThenEachRecordInOrder(t *testing.T) {
	var rec SelectedRecorder
	r, _ := newTestRouter(t, []CaptureSpec{MaterializeSpec("meta", metadataSelector()), MaterializeSpec("row", recordsSelector())}, &rec)
	if flow, f := Replay(workedExample(), r); f != nil || flow != Continue || !r.Ended() {
		t.Fatal(f)
	}
	var got []string
	for _, s := range rec.Matches {
		got = append(got, s.Tag+" "+s.Path.String()+" "+s.Value.String())
	}
	want := []string{
		`meta .response.metadata.fields [{"title":"Identifier","path":["id"]},{"title":"Full name","path":["person","name"]},{"title":"Balance","path":["account","balance"]}]`,
		`row .response.payload.deep.records[0] {"id":123,"person":{"name":"Alice"},"account":{"balance":50.25}}`,
		`row .response.payload.deep.records[1] {"account":{"balance":72},"id":456,"person":{"name":"Bob"}}`,
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") || rec.Matches[0].ID != 0 || rec.Matches[1].ID != 1 {
		t.Fatal(got)
	}
}

func TestOverlappingMaterializationsAreRejectedAtConstruction(t *testing.T) {
	rows := Root().Property("records").EachIndex()
	inner := Root().Property("records").Index(2).Property("x")
	for _, pair := range [][2]Selector{{rows, inner}, {inner, rows}} {
		if _, f := newTestRouter(t, []CaptureSpec{MaterializeSpec("a", pair[0]), MaterializeSpec("b", pair[1])}, &SelectedRecorder{}); f == nil || f.Code != CodeCaptureOverlapUnsupported {
			t.Fatal(f)
		}
	}
	if _, f := newTestRouter(t, []CaptureSpec{ObserveSpec("a", rows), MaterializeSpec("b", Root())}, &SelectedRecorder{}); f == nil {
		t.Fatal("observe over materialize")
	}
	if _, f := newTestRouter(t, []CaptureSpec{ObserveSpec("a", rows), ObserveSpec("b", Root())}, &SelectedRecorder{}); f != nil {
		t.Fatal(f)
	}
	if _, f := newTestRouter(t, []CaptureSpec{MaterializeSpec("a", rows), MaterializeSpec("b", Root().Property("meta"))}, &SelectedRecorder{}); f != nil {
		t.Fatal(f)
	}
}

func TestACaptureBeginningInsideAMaterializationFailsAtRunTime(t *testing.T) {
	specs := []CaptureSpec{MaterializeSpec("outer", Root().Property("a")), MaterializeSpec("inner", Root().Property("a").Property("b"))}
	r := newRouterUnchecked(specs, DefaultLimits(), Reject, NewMetrics(), &SelectedRecorder{})
	_, f := Replay([]Event{EvObjectStart(), EvKey("a"), EvObjectStart(), EvKey("b"), EvNull()}, r)
	if f == nil || f.Code != CodeCaptureOverlapUnsupported || f.Path != ".a.b" {
		t.Fatal(f)
	}
}

func TestACaptureOverItsBudgetNamesTheLimit(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxCaptureBytes = 40
	r, _ := NewRouter([]CaptureSpec{MaterializeSpec("row", recordsSelector())}, limits, Reject, NewMetrics(), &SelectedRecorder{})
	_, f := Replay(workedExample(), r)
	if f == nil || f.Limit == nil || f.Limit.Name != "max_capture_bytes" || f.Limit.Value != 40 || f.Path != ".response.payload.deep.records[0]" {
		t.Fatal(f)
	}
	r, _ = newTestRouter(t, []CaptureSpec{MaterializeSpec("meta", metadataSelector()).WithBudget(10, "max_metadata_bytes")}, &SelectedRecorder{})
	if _, f := Replay(workedExample(), r); f == nil || f.Limit.Name != "max_metadata_bytes" {
		t.Fatal(f)
	}
}

func TestRouterDepthOverTheLimitNamesMaxDepth(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxDepth = 3
	r, _ := NewRouter(nil, limits, Reject, NewMetrics(), &SelectedRecorder{})
	Replay([]Event{EvArrayStart(), EvArrayStart(), EvArrayStart()}, r)
	_, f := r.Event(EvArrayStart())
	if f == nil || f.Limit.Name != "max_depth" || f.Path != "[0][0][0]" {
		t.Fatal(f)
	}
}

func TestObserveDeliversPathsAtTheValueEndAndNests(t *testing.T) {
	var rec SelectedRecorder
	r, _ := newTestRouter(t, []CaptureSpec{
		ObserveSpec("row", recordsSelector()),
		ObserveSpec("records", Root().Property("response").Property("payload").Property("deep").Property("records")),
		ObserveSpec("title", metadataSelector().EachIndex().Property("title")),
	}, &rec)
	Replay(workedExample(), r)
	var got []string
	for _, s := range rec.Matches {
		if s.Value != nil {
			t.Fatal("observe retains nothing")
		}
		got = append(got, s.Tag+" "+s.Path.String())
	}
	want := "[title .response.metadata.fields[0].title title .response.metadata.fields[1].title " +
		"title .response.metadata.fields[2].title row .response.payload.deep.records[0] " +
		"row .response.payload.deep.records[1] records .response.payload.deep.records]"
	if fmt.Sprint(got) != want {
		t.Fatal(got)
	}
}

type takeOne struct{ n int }

func (t *takeOne) Selected(Selected) (Flow, *Fail) { t.n++; return Stop, nil }
func (t *takeOne) End() (Flow, *Fail)              { panic("end must not follow a stop") }

type ends struct{ n int }

func (e *ends) Selected(Selected) (Flow, *Fail) { panic("nothing is selected") }
func (e *ends) End() (Flow, *Fail)              { e.n++; return Continue, nil }

func TestAStopFromDownstreamStopsTheRouter(t *testing.T) {
	down := &takeOne{}
	r, _ := newTestRouter(t, []CaptureSpec{MaterializeSpec("row", recordsSelector())}, down)
	if flow, f := Replay(workedExample(), r); f != nil || flow != Stop || down.n != 1 || r.Ended() {
		t.Fatal(flow, f)
	}
}

func TestEndIsDeliveredExactlyOnce(t *testing.T) {
	down := &ends{}
	r, _ := newTestRouter(t, nil, down)
	Replay(workedExample(), r)
	if down.n != 1 || !r.Ended() {
		t.Fatal(down.n)
	}
	if _, f := r.Event(EvEnd()); f == nil || f.Code != CodeProtocolOrderError || down.n != 1 {
		t.Fatal(f)
	}
}

func TestMalformedStreamsFailBeforeAnythingIsDelivered(t *testing.T) {
	var rec SelectedRecorder
	r, _ := newTestRouter(t, []CaptureSpec{MaterializeSpec("all", Root())}, &rec)
	if _, f := Replay([]Event{EvObjectStart(), EvArrayEnd()}, r); f == nil || f.Code != CodeProtocolOrderError || len(rec.Matches) != 0 {
		t.Fatal(f)
	}
}

func TestDuplicatesFollowThePolicyInsideACapture(t *testing.T) {
	events := []Event{EvObjectStart(), EvKey("a"), EvNumber(1), EvKey("a"), EvNumber(2), EvObjectEnd(), EvEnd()}
	run := func(policy Duplicates) (string, *Fail) {
		var rec SelectedRecorder
		r, _ := NewRouter([]CaptureSpec{MaterializeSpec("all", Root())}, DefaultLimits(), policy, NewMetrics(), &rec)
		if _, f := Replay(events, r); f != nil {
			return "", f
		}
		return rec.Matches[0].Value.String(), nil
	}
	if _, f := run(Reject); f == nil || f.Code != CodeDuplicateMember {
		t.Fatal(f)
	}
	if v, _ := run(LastWins); v != `{"a":2}` {
		t.Fatal(v)
	}
	if v, _ := run(FirstWins); v != `{"a":1}` {
		t.Fatal(v)
	}
}

func TestTheRouterAccountsForCapturesAndLeavesTheSourceCountsAlone(t *testing.T) {
	m := NewMetrics()
	var rec SelectedRecorder
	r, _ := NewRouter([]CaptureSpec{MaterializeSpec("row", recordsSelector())}, DefaultLimits(), Reject, m, &rec)
	Replay(workedExample(), r)
	if m.Events.Load() != 0 || m.Keys.Load() != 0 || m.Scalars.Load() != 0 || m.CapturedBytes.Load() != 0 {
		t.Fatal("source counts")
	}
	biggest := 0
	for _, s := range rec.Matches {
		if n := s.Value.ByteSize(); n > biggest {
			biggest = n
		}
	}
	if m.CapturedBytesHigh.Load() != uint64(biggest) {
		t.Fatal(m.CapturedBytesHigh.Load(), biggest)
	}
}

func TestAScalarCaptureIsDeliveredWhole(t *testing.T) {
	var rec SelectedRecorder
	r, _ := newTestRouter(t, []CaptureSpec{MaterializeSpec("v", Root().Property("v"))}, &rec)
	Replay([]Event{EvObjectStart(), EvKey("v"), EvNumberLexeme(1.5, "1.50"), EvObjectEnd(), EvEnd()}, r)
	if len(rec.Matches) != 1 || !rec.Matches[0].Value.Equal(NumberDatumLexeme(1.5, "1.50")) {
		t.Fatal(rec.Matches)
	}
}

func TestColumnFromMetaReadsTitleAndPath(t *testing.T) {
	meta := mustDatum(t, `{"title": "Balance", "path": ["account", "balance"]}`)
	c, f := ColumnFromMeta(&meta)
	if f != nil || c.Label != "Balance" || fmt.Sprint(c.Source) != fmt.Sprint([]Segment{KeySegment("account"), KeySegment("balance")}) {
		t.Fatal(c, f)
	}
	meta = mustDatum(t, `{"title": "First", "path": ["tags", 0]}`)
	if c, _ := ColumnFromMeta(&meta); c.Source[1] != IndexSegment(0) {
		t.Fatal(c)
	}
	for _, bad := range []string{`{"title": "x", "path": ["a", -1]}`, `{"path": ["a"]}`, `[1]`, `{"title":"x"}`} {
		meta = mustDatum(t, bad)
		if _, f := ColumnFromMeta(&meta); f == nil || f.Code != CodeInputInvalid {
			t.Fatal(bad, f)
		}
	}
}

func TestCellsPrintAsJSON(t *testing.T) {
	d := mustDatum(t, "50.25")
	if CellFromDatum(&d).String() != "50.25" {
		t.Fatal("number")
	}
	d = mustDatum(t, "[1, 2]")
	if CellFromDatum(&d).String() != `"[1,2]"` {
		t.Fatal(CellFromDatum(&d).String())
	}
	if (Cell{Kind: CellMissing}).String() != "missing" {
		t.Fatal("missing")
	}
	null := NullDatum()
	if CellFromDatum(&null).Kind != CellNull {
		t.Fatal("null")
	}
}

func runTable(t *testing.T, binding TableBinding, limits Limits, events []Event) (*Table, *Fail) {
	t.Helper()
	tbl := &Table{}
	tf, f := NewTableFromJSON(binding, limits, Reject, NewMetrics(), tbl)
	if f != nil {
		return nil, f
	}
	if _, f := Replay(events, tf); f != nil {
		return nil, f
	}
	return tbl, nil
}

func fromMetadata() TableBinding {
	return TableBinding{Schema: MetadataSchema(metadataSelector(), ColumnFromMeta), Rows: recordsSelector()}
}

func labels(t *Table) string {
	var out []string
	for _, c := range t.Columns {
		out = append(out, c.Label)
	}
	return fmt.Sprint(out)
}

func rows(t *Table) string {
	var out []string
	for _, r := range t.Rows {
		var cells []string
		for _, c := range r {
			cells = append(cells, c.String())
		}
		out = append(out, strings.Join(cells, " "))
	}
	return strings.Join(out, " | ")
}

func TestTheWorkedExampleYieldsTheSpecTable(t *testing.T) {
	tbl, f := runTable(t, fromMetadata(), DefaultLimits(), workedExample())
	if f != nil || labels(tbl) != "[Identifier Full name Balance]" || rows(tbl) != `123 "Alice" 50.25 | 456 "Bob" 72` || !tbl.Ended {
		t.Fatal(f, labels(tbl), rows(tbl))
	}
	if !tbl.Rows[0][2].Equal(Cell{Kind: CellNumber, HasLexeme: true, Value: 50.25, Lexeme: "50.25"}) ||
		!tbl.Rows[1][2].Equal(Cell{Kind: CellNumber, Value: 72}) {
		t.Fatal(tbl.Rows)
	}
}

func TestTableOrderMissingAndInference(t *testing.T) {
	_, f := runTable(t, fromMetadata(), DefaultLimits(), treeEvents(t,
		`{"response":{"payload":{"deep":{"records":[{"id":1}]}},"metadata":{"fields":[{"title":"Id","path":["id"]}]}}}`))
	if f == nil || f.Code != CodeInputOrderViolation || f.Path != ".response.payload.deep.records[0]" {
		t.Fatal(f)
	}
	twice := TableBinding{Schema: MetadataSchema(Root().EachIndex().Property("response").Property("metadata").Property("fields"), ColumnFromMeta),
		Rows: Root().Property("rows").EachIndex()}
	_, f = runTable(t, twice, DefaultLimits(), treeEvents(t, `[{"response":{"metadata":{"fields":[]}}},{"response":{"metadata":{"fields":[]}}}]`))
	if f == nil || f.Code != CodeInputOrderViolation || f.Path != "[1].response.metadata.fields" {
		t.Fatal(f)
	}
	policy := func(p MissingPolicy) TableBinding {
		return TableBinding{Schema: StaticSchema(BoundColumn{Label: "A", Source: []Segment{KeySegment("a")}, Missing: p}), Rows: Root().Property("rows").EachIndex()}
	}
	events := treeEvents(t, `{"rows":[{"a":null},{}]}`)
	if tbl, _ := runTable(t, policy(MissingCell), DefaultLimits(), events); rows(tbl) != "null | missing" {
		t.Fatal(rows(tbl))
	}
	if tbl, _ := runTable(t, policy(MissingNull), DefaultLimits(), events); rows(tbl) != "null | null" {
		t.Fatal(rows(tbl))
	}
	if _, f := runTable(t, policy(MissingError), DefaultLimits(), events); f == nil || f.Code != CodeMissingValue || f.Path != ".rows[1].a" {
		t.Fatal(f)
	}
	tbl, _ := runTable(t, fromMetadata(), DefaultLimits(), treeEvents(t,
		`{"response":{"metadata":{"fields":[{"title":"Id","path":["id"]}]},"payload":{"deep":{"records":[]}}}}`))
	if labels(tbl) != "[Id]" || len(tbl.Rows) != 0 || !tbl.Ended {
		t.Fatal(tbl)
	}
	tbl, _ = runTable(t, TableBinding{Schema: InferSchema(), Rows: Root().EachIndex()}, DefaultLimits(), treeEvents(t, "[]"))
	if len(tbl.Columns) != 0 || len(tbl.Rows) != 0 || !tbl.Ended {
		t.Fatal(tbl)
	}
	if _, f := runTable(t, fromMetadata(), DefaultLimits(), treeEvents(t, `{"other":1}`)); f == nil || f.Code != CodeInputInvalid || f.Path != ".response.metadata.fields" {
		t.Fatal(f)
	}
	tbl, _ = runTable(t, TableBinding{Schema: InferSchema(), Rows: Root().EachIndex()}, DefaultLimits(),
		treeEvents(t, `[{"b":1,"a":"x"},{"a":"y","c":true},{"b":3}]`))
	if labels(tbl) != "[b a]" || rows(tbl) != `1 "x" | missing "y" | 3 missing` {
		t.Fatal(labels(tbl), rows(tbl))
	}
}

func TestInferLabelsAnArrayRowByPosition(t *testing.T) {
	infer := TableBinding{Schema: InferSchema(), Rows: Root().EachIndex()}
	tbl, f := runTable(t, infer, DefaultLimits(), treeEvents(t, `[[1,"x"],["y",true,3],[2]]`))
	if f != nil || labels(tbl) != "[0 1]" || rows(tbl) != `1 "x" | "y" true | 2 missing` {
		t.Fatal(f, labels(tbl), rows(tbl))
	}
	// An empty array row is a table of no columns, as no rows is.
	tbl, f = runTable(t, infer, DefaultLimits(), treeEvents(t, `[[],[1]]`))
	if f != nil || len(tbl.Columns) != 0 || len(tbl.Rows) != 2 || len(tbl.Rows[0]) != 0 || len(tbl.Rows[1]) != 0 || !tbl.Ended {
		t.Fatal(f, tbl)
	}
}

func TestInferGivesAScalarRowOneValueColumn(t *testing.T) {
	tbl, f := runTable(t, TableBinding{Schema: InferSchema(), Rows: Root().EachIndex()}, DefaultLimits(), treeEvents(t, `[1,"s",true,null]`))
	if f != nil || labels(tbl) != "[value]" || rows(tbl) != `1 | "s" | true | null` {
		t.Fatal(f, labels(tbl), rows(tbl))
	}
}

// A later row of another kind than the first projects through the first
// row's paths: a key path on an array or a scalar, and an index path on
// an object or a scalar, miss, so the cell is missing under the column's
// policy; the empty path of a "value" column finds every row, a container
// as its compact JSON text.
func TestInferProjectsALaterRowOfAnotherKindThroughTheFirstRowsPaths(t *testing.T) {
	infer := TableBinding{Schema: InferSchema(), Rows: Root().EachIndex()}
	tbl, f := runTable(t, infer, DefaultLimits(), treeEvents(t, `[{"a":1},[2],3]`))
	if f != nil || labels(tbl) != "[a]" || rows(tbl) != `1 | missing | missing` {
		t.Fatal(f, labels(tbl), rows(tbl))
	}
	tbl, f = runTable(t, infer, DefaultLimits(), treeEvents(t, `[[1],{"0":2},3]`))
	if f != nil || labels(tbl) != "[0]" || rows(tbl) != `1 | missing | missing` {
		t.Fatal(f, labels(tbl), rows(tbl))
	}
	tbl, f = runTable(t, infer, DefaultLimits(), treeEvents(t, `[1,{"a":2},[3]]`))
	if f != nil || labels(tbl) != "[value]" || rows(tbl) != `1 | "{\"a\":2}" | "[3]"` {
		t.Fatal(f, labels(tbl), rows(tbl))
	}
}

func TestAColumnWhosePathIsAnotherColumnsPrefixStillGetsItsValue(t *testing.T) {
	events := treeEvents(t, `{"rows":[{"p":{"n":"a"},"id":1},{"p":{"n":"b"},"id":1}]}`)
	overlapping := []BoundColumn{
		NewBoundColumn("P", []Segment{KeySegment("p")}),
		NewBoundColumn("N", []Segment{KeySegment("p"), KeySegment("n")}),
		NewBoundColumn("Id", []Segment{KeySegment("id")}),
		NewBoundColumn("Id again", []Segment{KeySegment("id")}),
	}
	if pathsAreDisjoint(overlapping) {
		t.Fatal("overlapping")
	}
	tbl, _ := runTable(t, TableBinding{Schema: StaticSchema(overlapping...), Rows: Root().Property("rows").EachIndex()}, DefaultLimits(), events)
	if rows(tbl) != `"{\"n\":\"a\"}" "a" 1 1 | "{\"n\":\"b\"}" "b" 1 1` {
		t.Fatal(rows(tbl))
	}
	disjoint := []BoundColumn{NewBoundColumn("N", []Segment{KeySegment("p"), KeySegment("n")}), NewBoundColumn("Id", []Segment{KeySegment("id")})}
	if !pathsAreDisjoint(disjoint) || !pathsAreDisjoint([]BoundColumn{NewBoundColumn("Row", nil)}) ||
		pathsAreDisjoint([]BoundColumn{NewBoundColumn("Row", nil), NewBoundColumn("Id", []Segment{KeySegment("id")})}) {
		t.Fatal("disjoint")
	}
	tbl, _ = runTable(t, TableBinding{Schema: StaticSchema(disjoint...), Rows: Root().Property("rows").EachIndex()}, DefaultLimits(), events)
	if rows(tbl) != `"a" 1 | "b" 1` {
		t.Fatal(rows(tbl))
	}
}

func TestTableLimitsAreEnforcedByName(t *testing.T) {
	with := func(f func(*Limits)) Limits {
		l := DefaultLimits()
		f(&l)
		return l
	}
	if _, f := runTable(t, fromMetadata(), with(func(l *Limits) { l.MaxColumns = 2 }), workedExample()); f == nil || f.Limit.Name != "max_columns" {
		t.Fatal(f)
	}
	if _, f := runTable(t, fromMetadata(), with(func(l *Limits) { l.MaxMetadataBytes = 64 }), workedExample()); f == nil || f.Limit.Name != "max_metadata_bytes" {
		t.Fatal(f)
	}
	if _, f := runTable(t, fromMetadata(), with(func(l *Limits) { l.MaxRecordBytes = 48 }), workedExample()); f == nil || f.Limit.Name != "max_record_bytes" || f.Path != ".response.payload.deep.records[0]" {
		t.Fatal(f)
	}
	infer := TableBinding{Schema: InferSchema(), Rows: Root().EachIndex()}
	if _, f := runTable(t, infer, with(func(l *Limits) { l.MaxColumns = 1 }), treeEvents(t, `[{"a":1,"b":2}]`)); f == nil || f.Limit.Name != "max_columns" {
		t.Fatal(f)
	}
	// Two one-byte names take 16 + 2 * (16 + 1) = 50 bytes.
	if _, f := runTable(t, infer, with(func(l *Limits) { l.MaxMetadataBytes = 49 }), treeEvents(t, `[{"a":1,"b":2}]`)); f == nil || f.Limit.Name != "max_metadata_bytes" || f.Path != "[0]" {
		t.Fatal(f)
	}
	if tbl, f := runTable(t, infer, with(func(l *Limits) { l.MaxMetadataBytes = 50 }), treeEvents(t, `[{"a":1,"b":2}]`)); f != nil || labels(tbl) != "[a b]" {
		t.Fatal(f)
	}
	// Positional labels and the "value" label are measured the same way:
	// "0" and "1" take 50 bytes too, and "value" 16 + 16 + 5 = 37.
	if _, f := runTable(t, infer, with(func(l *Limits) { l.MaxMetadataBytes = 49 }), treeEvents(t, `[[1,2]]`)); f == nil || f.Limit.Name != "max_metadata_bytes" || f.Path != "[0]" {
		t.Fatal(f)
	}
	if tbl, f := runTable(t, infer, with(func(l *Limits) { l.MaxMetadataBytes = 50 }), treeEvents(t, `[[1,2]]`)); f != nil || labels(tbl) != "[0 1]" {
		t.Fatal(f)
	}
	if _, f := runTable(t, infer, with(func(l *Limits) { l.MaxMetadataBytes = 36 }), treeEvents(t, `[1]`)); f == nil || f.Limit.Name != "max_metadata_bytes" {
		t.Fatal(f)
	}
	if tbl, f := runTable(t, infer, with(func(l *Limits) { l.MaxMetadataBytes = 37 }), treeEvents(t, `[1]`)); f != nil || labels(tbl) != "[value]" {
		t.Fatal(f)
	}
	if _, f := runTable(t, infer, with(func(l *Limits) { l.MaxColumns = 1 }), treeEvents(t, `[[1,2]]`)); f == nil || f.Limit.Name != "max_columns" {
		t.Fatal(f)
	}
}

func TestABadDescriptorNamesItsPosition(t *testing.T) {
	_, f := runTable(t, fromMetadata(), DefaultLimits(), treeEvents(t,
		`{"response":{"metadata":{"fields":[{"title":"Id","path":["id"]},{"path":["x"]}]},"payload":{"deep":{"records":[]}}}}`))
	if f == nil || f.Code != CodeInputInvalid || f.Path != ".response.metadata.fields[1]" {
		t.Fatal(f)
	}
	_, f = runTable(t, fromMetadata(), DefaultLimits(), treeEvents(t,
		`{"response":{"metadata":{"fields":{"title":"Id"}},"payload":{"deep":{"records":[]}}}}`))
	if f == nil || f.Code != CodeInputInvalid {
		t.Fatal(f)
	}
}

func TestTableEndArrivesOnlyWithTheDocumentsEndAndRowsCount(t *testing.T) {
	m := NewMetrics()
	tbl := &Table{}
	tf, _ := NewTableFromJSON(fromMetadata(), DefaultLimits(), Reject, m, tbl)
	events := workedExample()
	Replay(events[:len(events)-1], tf)
	if tf.Ended() || tbl.Ended || len(tbl.Rows) != 2 {
		t.Fatal("ended early")
	}
	tf.Event(EvEnd())
	if !tf.Ended() || !tbl.Ended || m.Rows.Load() != 2 {
		t.Fatal("not ended")
	}
}

type stopAtSchema struct{}

func (stopAtSchema) TableEvent(ev TableEvent) (Flow, *Fail) {
	if ev.Kind == TableSchema {
		return Stop, nil
	}
	panic("nothing follows a stop")
}

func TestAStopFromTheTableSinkStopsTheRun(t *testing.T) {
	tf, _ := NewTableFromJSON(fromMetadata(), DefaultLimits(), Reject, NewMetrics(), stopAtSchema{})
	if flow, f := Replay(workedExample(), tf); f != nil || flow != Stop {
		t.Fatal(flow, f)
	}
	_, f := NewTableFromJSON(TableBinding{Schema: MetadataSchema(Root().Property("a"), ColumnFromMeta), Rows: Root().Property("a").EachIndex()},
		DefaultLimits(), Reject, NewMetrics(), &Table{})
	if f == nil || f.Code != CodeCaptureOverlapUnsupported {
		t.Fatal(f)
	}
}

func TestScanEmitRunningSum(t *testing.T) {
	var seen []string
	scan := NewScanEmit(int64(0),
		func(sum, x int64) (Transition[int64, string], *Fail) { return Emit(sum+x, fmt.Sprintf("+%d", x)), nil },
		func(sum int64) ([]string, *Fail) { return []string{fmt.Sprintf("=%d", sum)}, nil },
		func(s string) (Flow, *Fail) { seen = append(seen, s); return Continue, nil })
	scan.Item(1)
	scan.Item(2)
	scan.Finish()
	if _, f := scan.Finish(); f == nil || f.Code != CodeProtocolOrderError {
		t.Fatal(f)
	}
	if fmt.Sprint(seen) != "[+1 +2 =3]" {
		t.Fatal(seen)
	}
	stop := NewScanEmit(struct{}{},
		func(s struct{}, x int) (Transition[struct{}, int], *Fail) { return Emit(s, x), nil },
		func(struct{}) ([]int, *Fail) { return nil, nil },
		func(x int) (Flow, *Fail) {
			if x == 2 {
				return Stop, nil
			}
			return Continue, nil
		})
	if f1, _ := stop.Item(1); f1 != Continue {
		t.Fatal(f1)
	}
	if f2, _ := stop.Item(2); f2 != Stop {
		t.Fatal(f2)
	}
	if s := Stay[int, string](3); s.State != 3 || len(s.Outputs) != 0 {
		t.Fatal(s)
	}
}

func TestGuardedCountsAreFlushedAtEnd(t *testing.T) {
	m := NewMetrics()
	var rec Recorder
	g := NewGuarded(&rec, DefaultLimits(), NewAbortFlag(), m)
	Replay([]Event{EvObjectStart(), EvKey("a"), EvNull(), EvKey("b"), EvString("x"), EvObjectEnd()}, g)
	if m.Events.Load() != 0 {
		t.Fatal("flushed early")
	}
	g.Event(EvEnd())
	if m.Events.Load() != 7 || m.Keys.Load() != 2 || m.Scalars.Load() != 2 {
		t.Fatal(m.Events.Load())
	}
	g.Event(EvNull())
	g.Flush()
	if m.Events.Load() != 8 || len(rec.Events) != 8 {
		t.Fatal(m.Events.Load())
	}
}

func TestEachSourceLimitFailsByName(t *testing.T) {
	guarded := func(f func(*Limits)) *Guarded {
		l := DefaultLimits()
		f(&l)
		return NewGuarded(&Recorder{}, l, NewAbortFlag(), NewMetrics())
	}
	g := guarded(func(l *Limits) { l.MaxDepth = 1 })
	g.Event(EvArrayStart())
	if _, f := g.Event(EvArrayStart()); f == nil || f.Limit.Name != "max_depth" {
		t.Fatal(f)
	}
	g = guarded(func(l *Limits) { l.MaxKeyBytes = 2 })
	g.Event(EvObjectStart())
	if _, f := g.Event(EvKey("abc")); f == nil || f.Limit.Name != "max_key_bytes" {
		t.Fatal(f)
	}
	g = guarded(func(l *Limits) { l.MaxScalarBytes = 2 })
	if _, f := g.Event(EvString("abc")); f == nil || f.Limit.Name != "max_scalar_bytes" {
		t.Fatal(f)
	}
	g = guarded(func(l *Limits) { l.MaxScalarBytes = 2 })
	if _, f := g.Event(EvNumberLexeme(1.5, "1.500")); f == nil {
		t.Fatal("lexeme")
	}
	abort := NewAbortFlag()
	g = NewGuarded(&Recorder{}, DefaultLimits(), abort, NewMetrics())
	g.Event(EvArrayStart())
	abort.Abort()
	if _, f := g.Event(EvNull()); f == nil || f.Code != CodeAborted {
		t.Fatal(f)
	}
}
