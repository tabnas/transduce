// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

// ParserEventSource: the ports of the Rust unit tests in
// rs/src/source/parser_events.rs that a TSV row cannot carry (a sink that
// stops or fails, the abort flag, the metrics).

import (
	"strings"
	"testing"

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
