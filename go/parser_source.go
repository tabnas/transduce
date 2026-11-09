// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

import (
	tabnas "github.com/tabnas/parser/go"
)

// ParserSource is JsonEvents/1 from a tabnas parse of one text
// (rs/src/source/parser.rs).
//
// It parses, then walks the value: always sound, for every grammar, and
// it retains the whole value. A format whose package ships a transducer
// part streams through the part instead, over ParserEventSource; the
// rule-event inference that streamed the grammars the differential suite
// had verified, before the parts, was retired with alchemy's streaming
// plan's step 13, in Rust first and then in this port.
//
// The source takes the parser over: it installs its parse budget on it,
// so a parser is given to one source.
//
// Failure mapping, in this order: a sink failure is returned as it was;
// a sink that stopped is (Stop, nil); a parse cancelled through the
// caller's AbortFlag is ABORTED; any other engine error is INPUT_INVALID
// with the engine's code and position; a grammar's own guard cancelling
// the parse is INPUT_INVALID too, with a message that names the
// grammar's guard.
type ParserSource struct {
	parser  *tabnas.Tabnas
	text    string
	limits  Limits
	abort   *AbortFlag
	metrics *Metrics
}

// NewParserSource is a source with default limits, its own abort flag
// and fresh metrics; the builder methods change each.
func NewParserSource(parser *tabnas.Tabnas, text string) *ParserSource {
	return &ParserSource{
		parser:  parser,
		text:    text,
		limits:  DefaultLimits(),
		abort:   NewAbortFlag(),
		metrics: NewMetrics(),
	}
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

// Run parses, then walks, into sink.
func (p *ParserSource) Run(sink Sink) (Flow, *Fail) {
	flow, f, _ := p.RunWithValue(sink)
	return flow, f
}

// RunWithValue is Run, also handing back the grammar's value, when the
// parse returned one.
func (p *ParserSource) RunWithValue(sink Sink) (Flow, *Fail, any) {
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
