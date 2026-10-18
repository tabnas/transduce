// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

// ParserEventSource and the native TransduceTree: the ports of the Rust
// unit tests in rs/src/source/parser_events.rs and rs/src/transduce_tree.rs
// that a TSV row cannot carry (a sink that stops or fails, the abort flag,
// the metrics, a pass built by hand).

import (
	"fmt"
	"strings"
	"testing"

	"github.com/tabnas/alchemy/go/shared"
	tabnasjson "github.com/tabnas/json/go"
)

func goPasses(t *testing.T, src string) ([]ParserEvent, *Fail) {
	t.Helper()
	rec := &passRecorder{}
	_, f := NewParserEventSource(tabnasjson.Make(), src).Run(rec)
	return rec.seen, f
}

// The open pass consumed the number, whose source text is its lexeme (the
// encoding writes a number as its lexeme); the close consumed the end of
// the source, which has no value, and carries the node's value. The groups
// are sorted.
func TestARootScalarIsItsValRulesTwoPassesThenEnd(t *testing.T) {
	seen, f := goPasses(t, "2.50")
	if f != nil {
		t.Fatal(f)
	}
	var got []string
	for _, ev := range seen {
		got = append(got, ev.JSON())
	}
	want := []string{
		`["open",{"name":"val","d":0,"i":1,"alt":{"p":"","r":"","b":0,"g":["json","val"]},"tokens":[["#NR",2.50,"2.50",1,1]],"forced":false,"n":{}}]`,
		`["close",{"name":"val","d":0,"i":1,"alt":{"p":"","r":"","b":0,"g":["end","json"]},"tokens":[["#ZZ",null,"",1,5]],"forced":false,"n":{},"value":2.50}]`,
		`["end"]`,
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("got\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	if tok := seen[0].Pass.Tokens[0].Val; !tok.HasLexeme || tok.Lexeme != "2.50" {
		t.Errorf("the number's lexeme: %+v", tok)
	}
}

// A source runs once: its parser carries the first run's subscriber and
// budget, so a second Run is STREAM_REUSED, and the first sink sees no
// more passes.
func TestASecondRunIsStreamReused(t *testing.T) {
	source := NewParserEventSource(tabnasjson.Make(), "[1]")
	first := &passRecorder{}
	if _, f := source.Run(first); f != nil {
		t.Fatal(f)
	}
	seen := len(first.seen)
	_, f := source.Run(&passRecorder{})
	if f == nil || f.Code != CodeStreamReused {
		t.Fatalf("a second run: %v", f)
	}
	if len(first.seen) != seen {
		t.Errorf("the first sink saw %d more passes", len(first.seen)-seen)
	}
}

func TestAParseErrorIsInvalidInputAfterThePassesBeforeIt(t *testing.T) {
	seen, f := goPasses(t, "[1,\n ]")
	if f == nil || f.Code != CodeInputInvalid || f.Row != 2 {
		t.Fatalf("%v", f)
	}
	if len(seen) == 0 || seen[len(seen)-1].Kind == ParserEnd {
		t.Errorf("%d passes, the last %v", len(seen), seen[len(seen)-1].Kind)
	}
}

func TestAHugeTokenIsRefusedByNameBeforeTheSinkSeesIt(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxScalarBytes = 3
	rec := &passRecorder{}
	_, f := NewParserEventSource(tabnasjson.Make(), `["abcd"]`).Limits(limits).Run(rec)
	if f == nil || f.Code != CodeResourceLimitExceeded || f.Limit == nil || f.Limit.Name != "max_scalar_bytes" {
		t.Fatalf("%v", f)
	}
	for _, ev := range rec.seen {
		if strings.Contains(ev.JSON(), "abcd") {
			t.Errorf("the sink saw %s", ev.JSON())
		}
	}
}

type stopAt struct{ at, n int }

func (s *stopAt) ParserEvent(ParserEvent) (Flow, *Fail) {
	s.n++
	if s.n == s.at {
		return Stop, nil
	}
	return Continue, nil
}

type refuse struct{}

func (refuse) ParserEvent(ParserEvent) (Flow, *Fail) { return Continue, OutputFail("disk full") }

func TestAStopStopsTheParseAndAFailureComesBack(t *testing.T) {
	s := &stopAt{at: 2}
	flow, f := NewParserEventSource(tabnasjson.Make(), "[1,2,3]").Run(s)
	if f != nil || flow != Stop || s.n != 2 {
		t.Errorf("%v %v after %d", flow, f, s.n)
	}
	if _, f := NewParserEventSource(tabnasjson.Make(), "[1]").Run(refuse{}); f == nil || f.Code != CodeOutputFailed {
		t.Errorf("%v", f)
	}
}

func TestAnAbortedFlagCancelsTheParseOfPassesAsAborted(t *testing.T) {
	abort := NewAbortFlag()
	abort.Abort()
	rec := &passRecorder{}
	if _, f := NewParserEventSource(tabnasjson.Make(), "[1]").Abort(abort).Run(rec); f == nil || f.Code != CodeAborted {
		t.Errorf("%v", f)
	}
	if len(rec.seen) != 0 {
		t.Errorf("%d passes", len(rec.seen))
	}
}

func TestMetricsCountTheEventsDelivered(t *testing.T) {
	metrics := NewMetrics()
	rec := &passRecorder{}
	if _, f := NewParserEventSource(tabnasjson.Make(), `{"a":[1]}`).Metrics(metrics).Run(rec); f != nil {
		t.Fatal(f)
	}
	if got := metrics.Events.Load(); got != uint64(len(rec.seen)) {
		t.Errorf("%d events counted, %d delivered", got, len(rec.seen))
	}
}

func goJSONSpec() TreeSpec {
	return TreeSpec{Rules: map[string]TreeRule{
		"map":  {Container: TreeObject},
		"list": {Container: TreeArray},
		"pair": {Key: TreeKey{Set: true, IsToken: true, Token: 0}},
		"val":  {Scalar: TreeScalar{Set: true, Token: 0}},
	}}
}

// The json part's spec over tabnas-json's own passes makes the document's
// events: those of the walk of its value, and each number with the lexeme
// it was written with.
func TestTheJSONSpecMakesTheDocumentsEvents(t *testing.T) {
	for _, src := range []string{
		`{"a":[1,2.50,"x",{"b":null}],"c":{},"d":[],"e":1e21,"f":true}`,
		"[]", "{}", "[[[]],[{}]]", `{"a":{"b":{"c":[1,[2,[3]]]}}}`, " 42 ", `"s"`, "null",
	} {
		var native Recorder
		if _, f := NewParserEventSource(tabnasjson.Make(), src).Run(NewTransduceTree(goJSONSpec(), &native)); f != nil {
			t.Fatalf("%s: %v", src, f)
		}
		var walk Recorder
		if _, f := NewParserSource(tabnasjson.Make(), src).Run(&walk); f != nil {
			t.Fatalf("%s: %v", src, f)
		}
		var lexemes []string
		for i := range native.Events {
			if native.Events[i].Kind == Number {
				lexemes = append(lexemes, native.Events[i].Lexeme)
				native.Events[i].HasLexeme, native.Events[i].Lexeme = false, ""
			}
		}
		if fmt.Sprint(native.Events) != fmt.Sprint(walk.Events) {
			t.Errorf("%s:\nnative %v\nwalk   %v", src, native.Events, walk.Events)
		}
		if src[0] == '{' && strings.Contains(src, "2.50") && fmt.Sprint(lexemes) != "[1 2.50 1e21]" {
			t.Errorf("%s: lexemes %v", src, lexemes)
		}
	}
}

func handPass(name string, r string, value *Datum, tokens ...Datum) *RulePass {
	p := &RulePass{Name: name, D: 1, I: 1, Alt: &PassAlt{R: r, G: []string{}}, Tokens: []PassToken{}, Value: value}
	for _, v := range tokens {
		p.Tokens = append(p.Tokens, PassToken{Name: "#T", Val: v, Row: 1, Col: 1})
	}
	return p
}

func runTree(spec TreeSpec, events ...ParserEvent) ([]Event, *Fail) {
	var rec Recorder
	native := NewTransduceTree(spec, &rec)
	for _, ev := range events {
		if _, f := native.ParserEvent(ev); f != nil {
			return rec.Events, f
		}
	}
	return rec.Events, nil
}

// A key named by the spec is announced as it stands; a scalar from a close
// pass's value; a rule the spec does not name yields nothing.
func TestANamedKeyACloseValueAndAnUnnamedRule(t *testing.T) {
	spec := TreeSpec{Rules: map[string]TreeRule{
		"doc": {Container: TreeObject},
		"v":   {Key: TreeKey{Set: true, Name: "k"}, Scalar: TreeScalar{Set: true, Value: true}},
	}}
	seven, eight := NumberDatum(7), NumberDatum(8)
	got, f := runTree(spec,
		ParserEvent{Kind: ParserOpen, Pass: handPass("doc", "", nil)},
		ParserEvent{Kind: ParserOpen, Pass: handPass("v", "", nil, NumberDatum(9))},
		ParserEvent{Kind: ParserOpen, Pass: handPass("other", "", nil, eight)},
		ParserEvent{Kind: ParserClose, Pass: handPass("other", "", &eight)},
		ParserEvent{Kind: ParserClose, Pass: handPass("v", "", &seven)},
		ParserEvent{Kind: ParserClose, Pass: handPass("doc", "", nil)},
		ParserEvent{Kind: ParserEnd},
	)
	want := []Event{EvObjectStart(), EvKey("k"), EvNumber(7), EvObjectEnd(), EvEnd()}
	if f != nil || fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("%v %v", got, f)
	}
}

// The library's refusals, with its codes and texts. A failing pass writes
// nothing, not even the key it would have announced.
func TestTheLibrarysRefusals(t *testing.T) {
	keyed := TreeSpec{Rules: map[string]TreeRule{
		"v": {Key: TreeKey{Set: true, Name: "k"}, Scalar: TreeScalar{Set: true, Token: 0}},
	}}
	got, f := runTree(keyed, ParserEvent{Kind: ParserOpen, Pass: handPass("v", "", nil, shared.ArrayDatum())})
	if f == nil || f.Code != CodeDSLTypeError || f.Message != "type_mismatch: scalar: the value must be null, a boolean, a number or a string, not a vector" || len(got) != 0 {
		t.Errorf("%v %v", got, f)
	}
	_, f = runTree(goJSONSpec(), ParserEvent{Kind: ParserOpen, Pass: handPass("pair", "", nil, BoolDatum(true))})
	if f == nil || f.Message != "type_mismatch: key: the name must be a string, not a boolean" {
		t.Errorf("%v", f)
	}
	_, f = runTree(goJSONSpec(), ParserEvent{Kind: ParserClose, Pass: handPass("map", "", nil)})
	if f == nil || f.Message != "type_mismatch: pop: the vector is empty; there is no last item to remove" {
		t.Errorf("%v", f)
	}
	_, f = runTree(goJSONSpec(), ParserEvent{Kind: ParserOpen, Pass: handPass("list", "", nil)}, ParserEvent{Kind: ParserEnd})
	if f == nil || f.Code != CodeProtocolOrderError || f.Message != "the rule passes ended inside a container, which a tree's never do" {
		t.Errorf("%v", f)
	}
	// A host's malformed event is refused, never read.
	if _, f = runTree(goJSONSpec(), ParserEvent{Kind: ParserOpen}); f == nil || f.Code != CodeProtocolOrderError {
		t.Errorf("an open without its pass: %v", f)
	}
}

// A close whose alternate replaces its rule with a container rule hands
// the container on: one start, one end, across the two rules.
func TestAReplacedContainerIsOneContainer(t *testing.T) {
	spec := TreeSpec{Rules: map[string]TreeRule{"a": {Container: TreeArray}, "b": {Container: TreeArray}}}
	got, f := runTree(spec,
		ParserEvent{Kind: ParserOpen, Pass: handPass("a", "", nil)},
		ParserEvent{Kind: ParserClose, Pass: handPass("a", "b", nil)},
		ParserEvent{Kind: ParserOpen, Pass: handPass("b", "", nil)},
		ParserEvent{Kind: ParserClose, Pass: handPass("b", "", nil)},
		ParserEvent{Kind: ParserEnd},
	)
	want := []Event{EvArrayStart(), EvArrayEnd(), EvEnd()}
	if f != nil || fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("%v %v", got, f)
	}
}

// spelledPass is a pass whose tokens are spelled srcs, each one's value
// its text.
func spelledPass(name string, r string, srcs ...string) *RulePass {
	p := handPass(name, r, nil)
	for _, src := range srcs {
		p.Tokens = append(p.Tokens, PassToken{Name: "#T", Val: StringDatum(src), Src: src, Row: 1, Col: 1})
	}
	return p
}

// A member is announced after what its rule begins, here a container one
// rule begins and a rule it replaces carries on; a :text rule's close
// yields first the text the passes between its open and it consumed, its
// own open's and close's tokens not among them, a rule the spec does not
// name writing its tokens too (rs/src/transduce_tree.rs's test).
func TestAMemberAfterTheStartAndARulesText(t *testing.T) {
	spec := TreeSpec{Rules: map[string]TreeRule{
		"row":  {Container: TreeObject, Member: TreeKey{Set: true, Name: "a"}, Text: true},
		"next": {Container: TreeObject, Member: TreeKey{Set: true, IsToken: true, Token: 0}, Text: true},
	}}
	got, f := runTree(spec,
		ParserEvent{Kind: ParserOpen, Pass: spelledPass("row", "", "<")},
		ParserEvent{Kind: ParserOpen, Pass: spelledPass("cell", "", "1", "2")},
		ParserEvent{Kind: ParserClose, Pass: spelledPass("cell", "", "3")},
		ParserEvent{Kind: ParserClose, Pass: spelledPass("row", "next", ",")},
		ParserEvent{Kind: ParserOpen, Pass: spelledPass("next", "", "b")},
		ParserEvent{Kind: ParserOpen, Pass: spelledPass("cell", "", "x")},
		ParserEvent{Kind: ParserClose, Pass: spelledPass("cell", "")},
		ParserEvent{Kind: ParserClose, Pass: spelledPass("next", "", ">")},
		ParserEvent{Kind: ParserEnd},
	)
	want := []Event{EvObjectStart(), EvKey("a"), EvString("123"), EvKey("b"), EvString("x"), EvObjectEnd(), EvEnd()}
	if f != nil || fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("%v %v", got, f)
	}
}

// A :text rule closed inside another joins its text to the one around it,
// its own open's tokens before it and its close's after.
func TestATextJoinsTheTextAroundIt(t *testing.T) {
	spec := TreeSpec{Rules: map[string]TreeRule{"outer": {Text: true}, "inner": {Text: true}}}
	got, f := runTree(spec,
		ParserEvent{Kind: ParserOpen, Pass: spelledPass("outer", "", "[")},
		ParserEvent{Kind: ParserOpen, Pass: spelledPass("x", "", "a")},
		ParserEvent{Kind: ParserClose, Pass: spelledPass("x", "")},
		ParserEvent{Kind: ParserOpen, Pass: spelledPass("inner", "", "(")},
		ParserEvent{Kind: ParserOpen, Pass: spelledPass("x", "", "b")},
		ParserEvent{Kind: ParserClose, Pass: spelledPass("x", "")},
		ParserEvent{Kind: ParserClose, Pass: spelledPass("inner", "", ")")},
		ParserEvent{Kind: ParserClose, Pass: spelledPass("outer", "", "]")},
		ParserEvent{Kind: ParserEnd},
	)
	want := []Event{EvString("b"), EvString("a(b)"), EvEnd()}
	if f != nil || fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("%v %v", got, f)
	}
}

// A text is held to the spec's bound at the library's joins, in its
// order, and refused as its `string-join` refuses: a pass's tokens
// together, then the innermost text with them, then a closed text with the
// text around it; a pass that fails writes nothing, and a text at the
// bound is not refused (rs/src/transduce_tree.rs).
func TestATextIsHeldToItsBoundAsTheLibraryHoldsIt(t *testing.T) {
	spec := TreeSpec{Rules: map[string]TreeRule{"outer": {Text: true}, "inner": {Text: true}}, MaxTextBytes: 3}
	open := func(name string, srcs ...string) ParserEvent {
		return ParserEvent{Kind: ParserOpen, Pass: spelledPass(name, "", srcs...)}
	}
	closed := func(name string) ParserEvent { return ParserEvent{Kind: ParserClose, Pass: spelledPass(name, "")} }
	refused := func(what string, n int, events ...ParserEvent) {
		t.Helper()
		got, f := runTree(spec, events...)
		want := shared.TreeTextRefusal(n, 3)
		if f == nil || f.Code != want.Code || f.Message != want.Message || f.Limit == nil || *f.Limit != *want.Limit || len(got) != 0 {
			t.Errorf("%s: %v %v", what, got, f)
		}
	}
	refused("a pass's tokens together", 4, open("outer"), open("x", "ab", "cd"))
	refused("the innermost text with a pass's tokens", 4, open("outer"), open("x", "ab"), closed("x"), open("x", "cd"))
	refused("a closed text with the text around it", 4,
		open("outer"), open("x", "ab"), closed("x"), open("inner"), open("x", "cd"), closed("x"), closed("inner"))
	got, f := runTree(spec,
		open("outer"), open("x", "a"), closed("x"), open("inner"), open("x", "bc"), closed("x"), closed("inner"), closed("outer"),
		ParserEvent{Kind: ParserEnd},
	)
	want := []Event{EvString("bc"), EvString("abc"), EvEnd()}
	if f != nil || fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("at the bound: %v %v", got, f)
	}

	// An open pass's key is refused before its tokens' text, and its
	// tokens' text before its member.
	fields := TreeSpec{Rules: map[string]TreeRule{
		"outer":  {Text: true},
		"keyed":  {Key: TreeKey{Set: true, IsToken: true, Token: 0}},
		"member": {Member: TreeKey{Set: true, IsToken: true, Token: 0}},
	}, MaxTextBytes: 3}
	wide := func(name string) ParserEvent {
		p := handPass(name, "", nil, BoolDatum(true))
		p.Tokens[0].Src = "true"
		return ParserEvent{Kind: ParserOpen, Pass: p}
	}
	if _, f := runTree(fields, open("outer"), wide("keyed")); f == nil || f.Code != CodeDSLTypeError {
		t.Errorf("keyed: %v", f)
	}
	if _, f := runTree(fields, open("outer"), wide("member")); f == nil || f.Message != shared.TreeTextRefusal(4, 3).Message {
		t.Errorf("member: %v", f)
	}
}

// The library's refusals of the two fields, with its codes and texts: a
// member whose token is not a string, before anything the pass would
// write, and a :text rule's close with no text open.
func TestTheLibrarysRefusalsOfAMemberAndAText(t *testing.T) {
	membered := TreeSpec{Rules: map[string]TreeRule{
		"row": {Container: TreeObject, Member: TreeKey{Set: true, IsToken: true, Token: 0}},
	}}
	got, f := runTree(membered, ParserEvent{Kind: ParserOpen, Pass: handPass("row", "", nil, BoolDatum(true))})
	if f == nil || f.Code != CodeDSLTypeError || f.Message != "type_mismatch: key: the name must be a string, not a boolean" || len(got) != 0 {
		t.Errorf("%v %v", got, f)
	}
	texted := TreeSpec{Rules: map[string]TreeRule{"t": {Text: true}}}
	_, f = runTree(texted, ParserEvent{Kind: ParserClose, Pass: handPass("t", "", nil)})
	if f == nil || f.Code != CodeDSLTypeError || f.Message != "type_mismatch: top: the vector is empty; there is no last item" {
		t.Errorf("%v", f)
	}
}
