// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

// ParserEventSource: the ports of the Rust unit tests in
// rs/src/source/parser_events.rs that a TSV row cannot carry (a sink that
// stops or fails, the abort flag, the metrics).

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
		`["open",{"name":"val","d":0,"i":1,"alt":{"p":"","r":"","b":0,"g":["json","val"]},"tokens":[["#NR",2.50,"2.50",1,1,0]],"forced":false,"n":{}}]`,
		`["close",{"name":"val","d":0,"i":1,"alt":{"p":"","r":"","b":0,"g":["end","json"]},"tokens":[["#ZZ",null,"",1,5,4]],"forced":false,"n":{},"value":2.50}]`,
		`["end"]`,
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("got\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	if tok := seen[0].Pass.Tokens[0].Val; !tok.HasLexeme || tok.Lexeme != "2.50" {
		t.Errorf("the number's lexeme: %+v", tok)
	}
}

// words is each event as a word: a pass by its rule, its phase and the byte
// offsets of its tokens, a token item by its name and offset.
func words(events []ParserEvent) string {
	var out []string
	for _, ev := range events {
		switch ev.Kind {
		case ParserEnd:
			out = append(out, "end")
		case ParserToken:
			out = append(out, fmt.Sprintf("%s@%d", ev.Token.Name, ev.Token.Offset))
		default:
			w := ev.Pass.Name + ":" + ev.Kind.String()
			for _, tok := range ev.Pass.Tokens {
				w += fmt.Sprintf("@%d", tok.Offset)
			}
			out = append(out, w)
		}
	}
	return strings.Join(out, " ")
}

// Each ignored token comes before the first pass that consumed a token
// after it: the leading space before the list's open pass, which consumed
// the bracket (the root's open consumed nothing, as its alternate gave the
// bracket back), the space after the comma before the second number's
// open, and the line break before the close that consumed the end of the
// source. Offsets count UTF-8 bytes: the string holds a two-byte
// character. rs/src/source/parser_events.rs's
// a_sink_that_asks_gets_the_ignored_tokens_in_source_order_with_byte_offsets.
func TestASinkThatAsksGetsTheIgnoredTokensInSourceOrderWithByteOffsets(t *testing.T) {
	src := " [\"é\", 1]\n"
	asks := &passRecorder{tokens: true}
	if _, f := NewParserEventSource(tabnasjson.Make(), src).Run(asks); f != nil {
		t.Fatal(f)
	}
	want := "val:open #SP@0 list:open@1 elem:open val:open@2 val:close elem:close@6 " +
		"elem:open #SP@7 val:open@8 val:close elem:close list:close@9 #LN@10 val:close@11 end"
	if got := words(asks.seen); got != want {
		t.Errorf("got\n%s\nwant\n%s", got, want)
	}
	// The same document to a sink that does not ask: the passes alone, their
	// offsets the same.
	plain, f := goPasses(t, src)
	if f != nil {
		t.Fatal(f)
	}
	bare := strings.NewReplacer("#SP@0 ", "", "#SP@7 ", "", "#LN@10 ", "").Replace(want)
	if got := words(plain); got != bare {
		t.Errorf("got\n%s\nwant\n%s", got, bare)
	}
	// A source told to send them sends them to any sink.
	told := &passRecorder{}
	if _, f := NewParserEventSource(tabnasjson.Make(), src).Tokens().Run(told); f != nil {
		t.Fatal(f)
	}
	if got := words(told.seen); got != want {
		t.Errorf("got\n%s\nwant\n%s", got, want)
	}
}

// DIVERGENCE.md 6: the engine's own space, line and comment tokens have no
// value in TypeScript and Go, and their text in Rust. A part reads a token
// item's text from its Src.
func TestASpaceTokenItemHasNoValue(t *testing.T) {
	asks := &passRecorder{tokens: true}
	if _, f := NewParserEventSource(tabnasjson.Make(), "  1").Run(asks); f != nil {
		t.Fatal(f)
	}
	for _, ev := range asks.seen {
		if ev.Kind == ParserToken {
			if ev.Token.Name != "#SP" || ev.Token.Val.Kind != shared.DatumNull || ev.Token.Src != "  " {
				t.Errorf("%+v", ev.Token)
			}
			return
		}
	}
	t.Fatal("no token item")
}

func TestATokenItemPastMaxScalarBytesIsRefusedByName(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxScalarBytes = 3
	asks := &passRecorder{tokens: true}
	_, f := NewParserEventSource(tabnasjson.Make(), "[1,    2]").Limits(limits).Run(asks)
	if f == nil || f.Code != CodeResourceLimitExceeded || f.Limit == nil || f.Limit.Name != "max_scalar_bytes" {
		t.Fatalf("%v", f)
	}
	for _, ev := range asks.seen {
		if ev.Kind == ParserToken {
			t.Errorf("a token item reached the sink: %+v", ev.Token)
		}
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
