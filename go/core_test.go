// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

// Behaviour the shared fixtures do not reach, ported from the Rust unit
// tests beside the code (rs/src/{event,error,limits,datum,selector,sink}.rs).

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestEventClassification(t *testing.T) {
	if !EvArrayStart().IsStart() || !EvObjectEnd().IsEnd() || !EvNull().IsScalar() {
		t.Fatal("classification")
	}
	if EvKey("k").IsScalar() || EvEnd().IsScalar() {
		t.Fatal("a key and End are not scalars")
	}
	if got := EvNumberLexeme(1, "1.0").String(); got != "1.0" {
		t.Fatal(got)
	}
	if got := EvNumber(1e21).String(); got != "1000000000000000000000" {
		t.Fatal(got)
	}
}

func TestJSONNumberLexemesAreRecognized(t *testing.T) {
	for _, ok := range []string{"0", "-0", "12", "1.5", "50.25", "1e21", "1E+2", "-3.25e-7"} {
		if !isJSONNumber(ok) {
			t.Error(ok)
		}
	}
	for _, bad := range []string{"", "-", "01", "1.", ".5", "+1", "0x10", "1_000", "1e", "NaN", "Infinity", "1 "} {
		if isJSONNumber(bad) {
			t.Error(bad)
		}
	}
}

func TestCodesAreStableNames(t *testing.T) {
	if len(AllCodes) != 15 {
		t.Fatalf("%d codes", len(AllCodes))
	}
	for _, c := range AllCodes {
		back, ok := ParseCode(c.String())
		if !ok || back != c {
			t.Fatalf("%v", c)
		}
		for _, b := range c.String() {
			if !(b >= 'A' && b <= 'Z' || b == '_') {
				t.Fatalf("%v is not SCREAMING_SNAKE_CASE", c)
			}
		}
	}
	if _, ok := ParseCode("nope"); ok {
		t.Fatal("nope")
	}
	want := "DSL_PARSE_ERROR DSL_TYPE_ERROR STREAM_REUSED STREAMABILITY_UNKNOWN INPUT_ORDER_VIOLATION " +
		"CAPTURE_OVERLAP_UNSUPPORTED MISSING_VALUE DUPLICATE_MEMBER INVALID_NUMBER PROTOCOL_ORDER_ERROR " +
		"TARGET_VALUE_UNREPRESENTABLE RESOURCE_LIMIT_EXCEEDED INPUT_INVALID OUTPUT_FAILED ABORTED"
	var got []string
	for _, c := range AllCodes {
		got = append(got, c.String())
	}
	if strings.Join(got, " ") != want {
		t.Fatal(got)
	}
}

func TestAFailurePositionNamesItsFile(t *testing.T) {
	if got := NewFail(CodeDSLTypeError, "arity: one argument").At(2, 3).Error(); got != "DSL_TYPE_ERROR: arity: one argument (2:3)" {
		t.Fatal(got)
	}
	if got := NewFail(CodeDSLTypeError, "arity: one argument").At(2, 3).InFile("render.alc").Error(); got != "DSL_TYPE_ERROR: arity: one argument (render.alc:2:3)" {
		t.Fatal(got)
	}
	if got := NewFail(CodeDSLParseError, "bad_def: a name").InFile("lift.alc").Error(); got != "DSL_PARSE_ERROR: bad_def: a name (in lift.alc)" {
		t.Fatal(got)
	}
}

func TestAFailureAsJSON(t *testing.T) {
	f := LimitFail("max_record_bytes", 64, "a row of 65 bytes").AtPath(".rows[3]").Committed()
	raw, _ := json.Marshal(f)
	if string(raw) != `{"code":"RESOURCE_LIMIT_EXCEEDED","message":"a row of 65 bytes","path":".rows[3]","limit":{"name":"max_record_bytes","value":64},"output":"partial"}` {
		t.Fatal(string(raw))
	}
	if f.Error() != "RESOURCE_LIMIT_EXCEEDED: a row of 65 bytes at .rows[3] [max_record_bytes = 64]" {
		t.Fatal(f.Error())
	}
	raw, _ = json.Marshal(NewFail(CodeDSLTypeError, "x").At(2, 3).InFile("render.alc"))
	if string(raw) != `{"code":"DSL_TYPE_ERROR","message":"x","row":2,"col":3,"file":"render.alc","output":"none"}` {
		t.Fatal(string(raw))
	}
}

func TestCaptureTracksHighWater(t *testing.T) {
	m := NewMetrics()
	m.Capture(10)
	m.Capture(20)
	m.Release(10)
	m.Capture(5)
	if m.CapturedBytes.Load() != 25 || m.CapturedBytesHigh.Load() != 30 || m.RetainedBytesHigh.Load() != 30 {
		t.Fatal(m.CapturedBytes.Load(), m.CapturedBytesHigh.Load())
	}
	raw, _ := json.Marshal(m)
	if !strings.Contains(string(raw), `"captured_bytes_high":30`) {
		t.Fatal(string(raw))
	}
}

func TestAbortIsShared(t *testing.T) {
	a := NewAbortFlag()
	b := a
	if b.IsAborted() {
		t.Fatal("set")
	}
	a.Abort()
	if !b.IsAborted() {
		t.Fatal("not shared")
	}
}

func mustDatum(t testing.TB, text string) Datum {
	t.Helper()
	d, err := DatumFromJSON(text)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func build(events []Event, limit int, policy Duplicates) (Datum, *Fail) {
	b := NewDatumBuilder(limit, "max_capture_bytes", policy)
	for _, ev := range events {
		if f := b.Event(ev); f != nil {
			return Datum{}, f
		}
	}
	d, _ := b.Take()
	return d, nil
}

func walked(d Datum) []Event {
	var rec Recorder
	WalkDatum(&d, &rec)
	return rec.Events
}

func TestWalkAndBuildRoundTrip(t *testing.T) {
	d := mustDatum(t, `{"a": [1, "x", null, true], "b": {"c": 2.5}}`)
	back, f := build(walked(d), 1<<30, Reject)
	if f != nil || !back.Equal(d) {
		t.Fatal(f, back)
	}
	if back.String() != `{"a":[1,"x",null,true],"b":{"c":2.5}}` {
		t.Fatal(back.String())
	}
}

func TestLexemesSurvive(t *testing.T) {
	d := mustDatum(t, `{"n": 50.25}`)
	if v, _ := d.GetPath([]Segment{KeySegment("n")}); v.String() != "50.25" {
		t.Fatal(v)
	}
	big := NumberDatumLexeme(1.2345678901234568e29, "123456789012345678901234567890")
	if big.String() != "123456789012345678901234567890" {
		t.Fatal(big.String())
	}
	if NumberDatum(72).String() != "72" {
		t.Fatal(NumberDatum(72).String())
	}
}

// HasLexeme, not the text, says whether a number has a lexeme: the empty
// lexeme (Rust's Some("")) stays distinct from none (None) through every
// type that carries one, so a renderer can refuse it as INVALID_NUMBER
// rather than write the value.
func TestAnEmptyLexemeStaysDistinctFromNone(t *testing.T) {
	empty, none := EvNumberLexeme(0, ""), EvNumber(0)
	if !empty.HasLexeme || none.HasLexeme || empty == none || empty.WithoutLexeme() != none {
		t.Fatalf("empty %#v, none %#v", empty, none)
	}
	if empty.String() != "" || none.String() != "0" {
		t.Fatalf("%q %q", empty.String(), none.String())
	}
	for _, c := range []struct {
		ev   Event
		size int    // what the empty lexeme measures; none is a value's 8 bytes
		json string // the lexeme as it stands; none is the value's text
		enc  string // the fixture encoding
	}{
		{empty, NodeBytes, ``, `["number",0,""]`},
		{none, NodeBytes + 8, `0`, `["number",0,null]`},
	} {
		// The builder keeps it, charging the lexeme's bytes.
		b := NewDatumBuilder(1<<30, "max_capture_bytes", Reject)
		if f := b.Event(c.ev); f != nil || b.Bytes() != c.size {
			t.Fatalf("%#v: built with %v, charged %d", c.ev, f, b.Bytes())
		}
		d, _ := b.Take()
		if d.HasLexeme != c.ev.HasLexeme || d.ByteSize() != c.size || d.String() != c.json {
			t.Fatalf("%#v: the datum %#v measures %d", c.ev, d, d.ByteSize())
		}
		// The walk hands it back as it came.
		if back := walked(d); len(back) != 1 || back[0] != c.ev {
			t.Fatalf("%#v: walked back as %#v", c.ev, back)
		}
		// A cell keeps it.
		cell := CellFromDatum(&d)
		if cell.HasLexeme != c.ev.HasLexeme || cell.ByteSize() != c.size || cell.String() != c.json {
			t.Fatalf("%#v: the cell %#v", c.ev, cell)
		}
		// The harness encodes it as the fixtures spell it: "", never null.
		for _, v := range []any{eventValue(c.ev), datumValue(&d), cellValue(cell)} {
			if enc, _ := json.Marshal(v); string(enc) != c.enc {
				t.Fatalf("%#v: encoded as %s", c.ev, enc)
			}
		}
	}
	if NumberDatumLexeme(0, "").Equal(NumberDatum(0)) || NumberDatum(0).Equal(NumberDatumLexeme(0, "")) {
		t.Fatal("a datum with the empty lexeme equals one without")
	}
	if a, b := (Cell{Kind: CellNumber, HasLexeme: true}), (Cell{Kind: CellNumber}); a.Equal(b) || b.Equal(a) {
		t.Fatal("a cell with the empty lexeme equals one without")
	}
}

func TestStringsEscapeAsRFC8259(t *testing.T) {
	d := StringDatum("a\"b\\c\n\u0001\u007fé")
	if d.String() != "\"a\\\"b\\\\c\\n\\u0001\u007fé\"" {
		t.Fatal(d.String())
	}
	var back string
	if err := json.Unmarshal([]byte(d.String()), &back); err != nil || back != d.Text {
		t.Fatal(err, back)
	}
}

func TestDatumGetAndTakePath(t *testing.T) {
	d := mustDatum(t, `{"a": [{"b": 1}]}`)
	p := []Segment{KeySegment("a"), IndexSegment(0), KeySegment("b")}
	if v, ok := d.GetPath(p); !ok || v.String() != "1" {
		t.Fatal(v)
	}
	if _, ok := d.GetPath([]Segment{KeySegment("z")}); ok {
		t.Fatal("z")
	}
	if _, ok := d.GetPath([]Segment{IndexSegment(0)}); ok {
		t.Fatal("[0]")
	}
	if _, ok := d.GetPath(nil); !ok {
		t.Fatal("root")
	}
	d = mustDatum(t, `{"a": [{"b": "x"}], "c": 2}`)
	if v, ok := d.TakePath(p); !ok || !v.Equal(StringDatum("x")) {
		t.Fatal(v)
	}
	if v, _ := d.GetPath(p); v.Kind != DatumNull {
		t.Fatal(v)
	}
	if d.String() != `{"a":[{"b":null}],"c":2}` {
		t.Fatal(d.String())
	}
	if v, _ := d.TakePath(nil); v.String() != `{"a":[{"b":null}],"c":2}` || d.Kind != DatumNull {
		t.Fatal(v, d)
	}
}

func TestDatumSizeAndLimit(t *testing.T) {
	d := mustDatum(t, `["abcd", "ef"]`)
	if d.ByteSize() != NodeBytes*3+6 {
		t.Fatal(d.ByteSize())
	}
	_, f := build(walked(d), NodeBytes*2+5, Reject)
	if f == nil || f.Code != CodeResourceLimitExceeded || f.Limit.Name != "max_capture_bytes" {
		t.Fatal(f)
	}
}

func TestDuplicatesPolicy(t *testing.T) {
	events := []Event{EvObjectStart(), EvKey("a"), EvBool(true), EvKey("a"), EvBool(false), EvObjectEnd()}
	if _, f := build(events, 1<<30, Reject); f == nil || f.Code != CodeDuplicateMember {
		t.Fatal(f)
	}
	if d, _ := build(events, 1<<30, LastWins); d.String() != `{"a":false}` {
		t.Fatal(d)
	}
	if d, _ := build(events, 1<<30, FirstWins); d.String() != `{"a":true}` {
		t.Fatal(d)
	}
}

func TestProbingAnUnfinishedBuilderKeepsItsCharge(t *testing.T) {
	b := NewDatumBuilder(1<<30, "max_capture_bytes", Reject)
	b.Event(EvArrayStart())
	b.Event(EvString("abcd"))
	held := b.Bytes()
	if _, ok := b.Take(); ok || b.Bytes() != held {
		t.Fatal("a failed take releases nothing")
	}
	b.Event(EvArrayEnd())
	if _, ok := b.Take(); !ok || b.Bytes() != 0 {
		t.Fatal("take")
	}
}

func TestARepeatedMemberDoesNotGrowTheCharge(t *testing.T) {
	for _, policy := range []Duplicates{FirstWins, LastWins} {
		b := NewDatumBuilder(1<<30, "max_capture_bytes", policy)
		b.Event(EvObjectStart())
		b.Event(EvKey("k"))
		b.Event(EvString("first"))
		once := b.Bytes()
		for i := 0; i < 100; i++ {
			b.Event(EvKey("k"))
			b.Event(EvString("again"))
		}
		if b.Bytes() != once {
			t.Fatalf("%v: %d, %d", policy, b.Bytes(), once)
		}
		b.Event(EvObjectEnd())
		d, _ := b.Take()
		if d.ByteSize() != once {
			t.Fatalf("%v", policy)
		}
	}
	// Both values are held for a moment, and that moment is what the
	// limit protects.
	b := NewDatumBuilder(NodeBytes*2+1+5+3, "max_capture_bytes", LastWins)
	b.Event(EvObjectStart())
	b.Event(EvKey("k"))
	b.Event(EvString("first"))
	b.Event(EvKey("k"))
	if f := b.Event(EvString("second")); f == nil || f.Code != CodeResourceLimitExceeded {
		t.Fatal(f)
	}
}

func TestABuilderIndexesALargeObject(t *testing.T) {
	var events []Event
	events = append(events, EvObjectStart())
	for i := 0; i < 100; i++ {
		events = append(events, EvKey(string(rune('a'+i%26))+strings.Repeat("x", i/26)), EvNumber(float64(i)))
	}
	events = append(events, EvKey("a"), EvNumber(-1), EvObjectEnd())
	d, f := build(events, 1<<30, LastWins)
	if f != nil || len(d.Members) != 100 || d.Members[0].Value.Value != -1 {
		t.Fatal(f, d.Members[0])
	}
}

func TestBuilderProtocolErrors(t *testing.T) {
	for _, events := range [][]Event{
		{EvObjectEnd()},
		{EvObjectStart(), EvNull()},
		{EvArrayStart(), EvEnd()},
	} {
		if _, f := build(events, 1<<30, Reject); f == nil || f.Code != CodeProtocolOrderError {
			t.Fatal(events, f)
		}
	}
}

func TestDatumFromAnEngineValue(t *testing.T) {
	v, err := makeGrammar("json").Parse(`{"a":[1,2],"b":"x","c":null}`)
	if err != nil {
		t.Fatal(err)
	}
	if d := DatumFromValue(v); d.String() != `{"a":[1,2],"b":"x","c":null}` {
		t.Fatal(d)
	}
	// A plain Go map has no order: its members are walked sorted, numbers
	// within a key compared as numbers.
	plain := map[string]any{"field~10": 1.0, "b": true, "field~2": "x", "a": nil}
	if d := DatumFromValue(plain); d.String() != `{"a":null,"b":true,"field~2":"x","field~10":1}` {
		t.Fatal(d)
	}
}

func TestSelectorDisplayIsJq(t *testing.T) {
	s := Root().Property("response").Property("odd key").Index(3).EachIndex().EachMember()
	if s.String() != `.response."odd key"[3][*][]` || Root().String() != "." {
		t.Fatal(s.String())
	}
	p := Path{KeySegment("a"), IndexSegment(0), KeySegment("b-c")}
	if p.String() != `.a[0]."b-c"` || (Path{}).String() != "." {
		t.Fatal(p.String())
	}
	for key, want := range map[string]string{
		"a_1": ".a_1", "_x": "._x", "é": `."é"`, "1a": `."1a"`, "": `.""`, "a\"b": `."a\"b"`,
		"a\x01": `."a\u0001"`, "tab\t": `."tab\t"`,
	} {
		if got := (Path{KeySegment(key)}).String(); got != want {
			t.Errorf("%q: %s, want %s", key, got, want)
		}
	}
}

func TestSelectorBuildersDoNotAlias(t *testing.T) {
	base := Root().Property("a")
	x, y := base.Property("x"), base.Property("y")
	if x.String() != ".a.x" || y.String() != ".a.y" || base.String() != ".a" {
		t.Fatal(x, y, base)
	}
	s := FromSegments([]Segment{KeySegment("account"), KeySegment("balance")})
	if s.String() != ".account.balance" || s.IsMulti() || !Root().EachIndex().IsMulti() {
		t.Fatal(s)
	}
	if Root().Property("a").Compose(Root().EachIndex()).String() != ".a[*]" {
		t.Fatal("compose")
	}
}

func TestSelectorOverlap(t *testing.T) {
	rows := Root().Property("records").EachIndex()
	meta := Root().Property("metadata")
	inner := Root().Property("records").Index(2).Property("x")
	switch {
	case rows.MayOverlap(meta), !rows.MayOverlap(inner), !inner.MayOverlap(rows), !rows.MayOverlap(rows),
		!Root().MayOverlap(meta), !Root().EachMember().MayOverlap(meta), Root().EachIndex().MayOverlap(meta):
		t.Fatal("overlap")
	}
}

func TestARecorderReplays(t *testing.T) {
	var rec Recorder
	Replay([]Event{EvArrayStart(), EvBool(true), EvArrayEnd(), EvEnd()}, &rec)
	var count CountSink
	if flow, _ := Replay(rec.Events, &count); flow != Continue || count.Events != 4 {
		t.Fatal(count)
	}
	seen := 0
	stopper := FnSink(func(Event) (Flow, *Fail) {
		seen++
		if seen == 2 {
			return Stop, nil
		}
		return Continue, nil
	})
	if flow, _ := Replay(rec.Events, stopper); flow != Stop || seen != 2 {
		t.Fatal(seen)
	}
}

func treeEvents(t testing.TB, text string) []Event {
	d := mustDatum(t, text)
	return append(walked(d), EvEnd())
}

func TestATreesEventsPassTheTreeContractUnchanged(t *testing.T) {
	for _, doc := range []string{
		`{"a":{"b":1,"a":2},"b":[{"a":3},{"a":4},[],{}],"c":[],"d":{}}`,
		"1", `"x"`, "null", "[]", "[1,[2,[3]]]",
	} {
		var rec Recorder
		guard := NewTreeContract(&rec)
		if _, f := Replay(treeEvents(t, doc), guard); f != nil || !eventsEqualCore(rec.Events, treeEvents(t, doc)) {
			t.Fatal(doc, f)
		}
	}
}

func eventsEqualCore(a, b []Event) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestARepeatedKeyInOneObjectIsADuplicateMemberAtItsPath(t *testing.T) {
	stream := []Event{EvObjectStart(), EvKey("a"), EvNull(), EvKey("b"), EvArrayStart(), EvNull(),
		EvObjectStart(), EvKey("x"), EvNull(), EvKey("x")}
	var rec Recorder
	_, f := Replay(stream, NewTreeContract(&rec))
	if f == nil || f.Code != CodeDuplicateMember || !strings.HasPrefix(f.Message, `member "x"`) || f.Path != ".b[1].x" {
		t.Fatal(f)
	}
	if len(rec.Events) != len(stream)-1 {
		t.Fatal(len(rec.Events))
	}
	_, f = Replay([]Event{EvObjectStart(), EvKey("a b"), EvNull(), EvKey("a b")}, NewTreeContract(&Recorder{}))
	if f.Path != `."a b"` {
		t.Fatal(f.Path)
	}
}

func TestEventsNoTreeHasAreRefusedAsNotATree(t *testing.T) {
	os, oe, as, ae, k, null := EvObjectStart(), EvObjectEnd(), EvArrayStart(), EvArrayEnd(), EvKey, EvNull()
	cases := []struct {
		bad  []Event
		why  string
		path string
	}{
		{[]Event{os, null}, "a value where a key is due", "."},
		{[]Event{os, k("a"), os, as}, "a value where a key is due", ".a"},
		{[]Event{os, k("a"), k("b")}, "a key where a value is due", ".a"},
		{[]Event{as, k("a")}, "a key outside an object", "[0]"},
		{[]Event{k("a")}, "a key outside an object", "."},
		{[]Event{os, k("a"), oe}, "an object's end where a value is due", ".a"},
		{[]Event{as, null, oe}, "an object's end where none is due", "[1]"},
		{[]Event{oe}, "an object's end where none is due", "."},
		{[]Event{os, ae}, "an array's end where none is due", "."},
		{[]Event{null, null}, "a second root value", "."},
		{[]Event{as, ae, os}, "a second root value", "."},
		{[]Event{as, EvEnd()}, "its end inside an open container", "[0]"},
	}
	for _, c := range cases {
		var rec Recorder
		_, f := Replay(c.bad, NewTreeContract(&rec))
		if f == nil || f.Code != CodeStreamabilityUnknown || !strings.Contains(f.Message, c.why) || f.Path != c.path {
			t.Errorf("%v: %v", c.bad, f)
		}
		if len(rec.Events) != len(c.bad)-1 {
			t.Errorf("%v: %d passed", c.bad, len(rec.Events))
		}
	}
}
