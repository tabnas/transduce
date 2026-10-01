// Copyright (c) 2026 tabnas, MIT License

//go:build tabnas_nodecell

package tabnastransduce

// The incremental source's own tests, ported from rs/src/source/parser.rs
// and rs/src/source/rule_events.rs.

import (
	"fmt"
	"testing"
)

func TestIncrementalEventsEqualTheWalkAndCarryLexemes(t *testing.T) {
	f1, inc := record(IncrementalMode(Prune{}), parserDoc)
	f2, mat := record(MaterializeMode(), parserDoc)
	if f1 != nil || f2 != nil || !eventsEqual(withoutLexemes(inc), mat) || inc[len(inc)-1].Kind != End {
		t.Fatal(f1, f2, inc, mat)
	}
	var lexemes []string
	for _, ev := range inc {
		if ev.Kind == Number {
			lexemes = append(lexemes, ev.Lexeme)
		}
	}
	if fmt.Sprint(lexemes) != "[1 2.50 1e21]" {
		t.Fatal(lexemes)
	}
	for _, ev := range mat {
		if ev.Kind == Number && ev.HasLexeme {
			t.Fatal("the walk has no lexemes")
		}
	}
}

func TestARootScalarIsOneEventThenEnd(t *testing.T) {
	if f, inc := record(IncrementalMode(Prune{}), " 42 "); f != nil || !eventsEqual(inc, []Event{EvNumberLexeme(42, "42"), EvEnd()}) {
		t.Fatal(f, inc)
	}
	if f, inc := record(IncrementalMode(Prune{}), `"s"`); f != nil || !eventsEqual(inc, []Event{EvString("s"), EvEnd()}) {
		t.Fatal(f, inc)
	}
}

func TestPruningLeavesTheEventsUntouched(t *testing.T) {
	_, plain := record(IncrementalMode(Prune{}), parserDoc)
	for _, prune := range []Prune{
		{Kind: PruneAllArrays},
		PruneUnderSelector(Root().Property("a").EachIndex()),
		PruneUnderSelector(Root().Property("a")),
	} {
		f, pruned := record(IncrementalMode(prune), parserDoc)
		if f != nil || !eventsEqual(pruned, plain) {
			t.Fatal(prune, f, pruned)
		}
	}
}

// pruned runs a parse through the adapter and gives back the engine's
// value, to see what pruning did to it.
func pruned(t *testing.T, src string, prune Prune) (string, []Event) {
	var rec Recorder
	_, f, value := NewParserSource(makeGrammar("json"), src).Grammar("json").Mode(IncrementalMode(prune)).RunWithValue(&rec)
	if f != nil {
		t.Fatal(f)
	}
	return DatumFromValue(value).String(), rec.Events
}

func TestPruningEmptiesTheStreamedArraysInTheEnginesValueOnly(t *testing.T) {
	src := `{"rows":[{"a":1},{"a":2}],"keep":[1,2,3],"n":{"rows":[[1],[2]]}}`
	whole, events := pruned(t, src, Prune{})
	if whole != src {
		t.Fatal(whole)
	}
	got, same := pruned(t, src, PruneUnderSelector(Root().Property("rows").EachIndex()))
	if !eventsEqual(same, events) || got != `{"rows":[],"keep":[1,2,3],"n":{"rows":[[1],[2]]}}` {
		t.Fatal(got)
	}
	got, same = pruned(t, src, Prune{Kind: PruneAllArrays})
	if !eventsEqual(same, events) || got != `{"rows":[],"keep":[],"n":{"rows":[]}}` {
		t.Fatal(got)
	}
	got, _ = pruned(t, src, PruneUnderSelector(Root().Property("n").Property("rows")))
	if got != `{"rows":[{"a":1},{"a":2}],"keep":[1,2,3],"n":{"rows":[]}}` {
		t.Fatal(got)
	}
}

func TestTheLineSourcesGrammarIsVerified(t *testing.T) {
	if !Incremental("json") {
		t.Fatal("the JSON Lines source installs the adapter on tabnas-json without the gate")
	}
}
