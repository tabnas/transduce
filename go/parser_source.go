// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

import (
	"fmt"

	tabnas "github.com/tabnas/parser/go"
)

// ParserSource is JsonEvents/1 from a tabnas parse of one text.
//
// Two modes. ModeMaterialize parses, then walks the value: always sound,
// and it retains the whole value. ModeIncremental installs the
// rule-event adapter so events leave the parse as containers open and
// entries land, and optionally prunes streamed array elements from the
// engine's tree. It is sound for the grammars IncrementalGrammars lists,
// which the differential suite verifies, and for no other: an imperative
// grammar's rule events give a well-formed stream of the wrong shape or
// a malformed one, and the run would still succeed. So the incremental
// path is gated on the list, by the grammar's name, which the source
// cannot learn from the parser and so must be told (Grammar); without a
// listed name it fails with STREAMABILITY_UNKNOWN before the parse,
// having emitted nothing. Unverified lifts the gate for the differential
// suite that maintains the list.
//
// The source takes the parser over: it installs its parse budget (and
// in ModeIncremental a rule-done subscriber) on it, so a parser is given
// to one source.
//
// Failure mapping, in this order: a sink failure is returned as it was;
// a sink that stopped is (Stop, nil); a parse cancelled through the
// caller's AbortFlag is ABORTED; any other engine error is INPUT_INVALID
// with the engine's code and position; a grammar's own guard cancelling
// the parse is INPUT_INVALID too, with a message that names the
// grammar's guard.
type ParserSource struct {
	parser     *tabnas.Tabnas
	text       string
	mode       SourceMode
	limits     Limits
	abort      *AbortFlag
	metrics    *Metrics
	grammar    string
	unverified bool
}

// NewParserSource is a source in ModeMaterialize with default limits,
// its own abort flag and fresh metrics; the builder methods change each.
func NewParserSource(parser *tabnas.Tabnas, text string) *ParserSource {
	return &ParserSource{
		parser:  parser,
		text:    text,
		mode:    MaterializeMode(),
		limits:  DefaultLimits(),
		abort:   NewAbortFlag(),
		metrics: NewMetrics(),
	}
}

// Mode sets how the source produces its events.
func (p *ParserSource) Mode(mode SourceMode) *ParserSource {
	p.mode = mode
	return p
}

// Grammar names the grammar the parser implements, by the name its
// module uses (json for github.com/tabnas/json/go). ModeIncremental runs
// only for a name IncrementalGrammars lists; ModeMaterialize needs none.
func (p *ParserSource) Grammar(name string) *ParserSource {
	p.grammar = name
	return p
}

// Unverified runs ModeIncremental whatever the verified list says. It
// exists for the differential suite that maintains the list and for
// nothing else: on a grammar the suite has not verified, the events may
// be a well-formed stream of the wrong shape, or malformed, and the run
// still succeeds. A build without the adapter refuses all the same.
func (p *ParserSource) Unverified() *ParserSource {
	p.unverified = true
	return p
}

// Limits sets the run's limits.
func (p *ParserSource) Limits(limits Limits) *ParserSource {
	p.limits = limits
	return p
}

// Abort sets the abort flag the run polls.
func (p *ParserSource) Abort(abort *AbortFlag) *ParserSource {
	p.abort = abort
	return p
}

// Metrics sets the metrics the run counts into.
func (p *ParserSource) Metrics(metrics *Metrics) *ParserSource {
	p.metrics = metrics
	return p
}

// gate is why the incremental path may not run, when it may not.
func (p *ParserSource) gate() *Fail {
	if !adapterBuilt {
		return NewFail(CodeStreamabilityUnknown,
			"this build has no incremental adapter (it needs the engine's node-cell identity, built "+
				"with the tabnas_nodecell tag), so no grammar is verified for SourceMode Incremental; "+
				"run it with ModeMaterialize")
	}
	if p.unverified {
		return nil
	}
	switch {
	case p.grammar == "":
		return NewFail(CodeStreamabilityUnknown,
			"ModeIncremental needs the grammar's name (ParserSource.Grammar) to check "+
				"IncrementalGrammars; without one, run ModeMaterialize")
	case !Incremental(p.grammar):
		return NewFail(CodeStreamabilityUnknown, fmt.Sprintf(
			"grammar %q is not in IncrementalGrammars: the differential suite has not verified that "+
				"its rule events stream as the walk does; run it with ModeMaterialize", p.grammar))
	}
	return nil
}

// Run drives sink in the configured mode.
func (p *ParserSource) Run(sink Sink) (Flow, *Fail) {
	flow, f, _ := p.RunWithValue(sink)
	return flow, f
}

// RunWithValue is Run, also handing back the value the engine returned,
// when the parse returned one. In ModeMaterialize that is the grammar's
// value. In ModeIncremental it is the engine's tree AFTER pruning, which
// is neither the grammar's value nor the run's result (the events are):
// it exists so a test can measure what pruning left in the tree, and
// nothing else should read it.
func (p *ParserSource) RunWithValue(sink Sink) (Flow, *Fail, any) {
	if p.mode.Kind == ModeIncremental {
		if f := p.gate(); f != nil {
			return Continue, f, nil
		}
		return runIncremental(p, sink)
	}
	guarded := NewGuarded(sink, p.limits, p.abort, p.metrics)
	flow, f, value := materialize(p.parser, p.text, p.abort, guarded)
	guarded.Flush()
	return flow, f, value
}

// materialize parses, then walks; the grammar's value comes back beside
// the outcome.
func materialize(parser *tabnas.Tabnas, text string, abort *AbortFlag, guarded *Guarded) (Flow, *Fail, any) {
	guard := installGuard(parser, func() bool { return !abort.IsAborted() })
	value, err := parser.Parse(text)
	if err != nil {
		return Continue, engineFailure(err, abort), nil
	}
	flow, f := walkValueOrdered(value, guarded, guard.fieldOrder())
	if f != nil || flow == Stop {
		return flow, f, value
	}
	flow, f = guarded.Event(EvEnd())
	return flow, f, value
}
