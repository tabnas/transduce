// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

// Ported from the Rust unit tests in rs/src/source/{mod,parser}.rs:
// ParserSource parses and walks. A source read from a reader is
// reader_test.go's, and pruning prune_test.go's.

import (
	"strings"
	"testing"
)

const parserDoc = `{"a":[1,2.50,"x",{"b":null}],"c":{},"d":[],"e":1e21,"f":true}`

func record(src string) (*Fail, []Event) {
	var rec Recorder
	_, f := NewParserSource(makeGrammar("json"), src).Run(&rec)
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

func TestARootScalarIsOneEventThenEnd(t *testing.T) {
	for _, c := range []struct {
		src  string
		want []Event
	}{
		{" 42 ", []Event{EvNumber(42), EvEnd()}},
		{`"s"`, []Event{EvString("s"), EvEnd()}},
	} {
		f, events := record(c.src)
		if f != nil || !eventsEqualCore(events, c.want) {
			t.Fatal(c.src, f, events)
		}
	}
}

func TestTheWalkCarriesNoNumberLexemes(t *testing.T) {
	f, events := record(parserDoc)
	if f != nil {
		t.Fatal(f)
	}
	var values []float64
	for _, ev := range events {
		if ev.Kind == Number {
			if ev.HasLexeme {
				t.Fatal("a lexeme on the walk", ev)
			}
			values = append(values, ev.Value)
		}
	}
	if len(values) != 3 || values[0] != 1 || values[1] != 2.5 || values[2] != 1e21 {
		t.Fatal(values)
	}
}

func TestAStopFromTheSinkStopsTheParse(t *testing.T) {
	seen := 0
	sink := FnSink(func(Event) (Flow, *Fail) {
		seen++
		if seen == 3 {
			return Stop, nil
		}
		return Continue, nil
	})
	flow, f := NewParserSource(makeGrammar("json"), parserDoc).Run(sink)
	if f != nil || flow != Stop || seen != 3 {
		t.Fatal(flow, f, seen)
	}
}

func TestASinkFailureComesBackUnchanged(t *testing.T) {
	sink := FnSink(func(ev Event) (Flow, *Fail) {
		if ev.Kind == Key && ev.Text == "c" {
			return Continue, OutputFail("disk full").AtPath(".c")
		}
		return Continue, nil
	})
	_, f := NewParserSource(makeGrammar("json"), parserDoc).Run(sink)
	if f == nil || f.Code != CodeOutputFailed || f.Path != ".c" {
		t.Fatal(f)
	}
}

func TestAnAbortedFlagCancelsTheParseAsAborted(t *testing.T) {
	abort := NewAbortFlag()
	abort.Abort()
	_, f := NewParserSource(makeGrammar("json"), parserDoc).Abort(abort).Run(&Recorder{})
	if f == nil || f.Code != CodeAborted {
		t.Fatal(f)
	}
}

func TestAParseErrorIsInvalidInputWithItsPosition(t *testing.T) {
	f, _ := record("{\"a\": 1,\n \"b\": }")
	if f == nil || f.Code != CodeInputInvalid || !strings.HasPrefix(f.Message, "unexpected") || f.Row != 2 || f.Column != 7 {
		t.Fatal(f)
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

func TestSourceLimitsApplyByName(t *testing.T) {
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
		_, f := NewParserSource(makeGrammar("json"), c.src).Limits(limits).Run(&Recorder{})
		if f == nil || f.Code != CodeResourceLimitExceeded || f.Limit.Name != c.limit {
			t.Fatal(c.limit, f)
		}
	}
}

func TestMetricsCountTheSourceEvents(t *testing.T) {
	m := NewMetrics()
	var rec Recorder
	if _, f := NewParserSource(makeGrammar("json"), parserDoc).Metrics(m).Run(&rec); f != nil {
		t.Fatal(f)
	}
	if m.Events.Load() != uint64(len(rec.Events)) || m.Keys.Load() != 6 || m.Scalars.Load() != 6 {
		t.Fatal(m.Events.Load(), m.Keys.Load(), m.Scalars.Load())
	}
}
