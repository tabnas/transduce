// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

import (
	"fmt"
	"math"
	"sort"
	"strconv"

	"github.com/tabnas/alchemy/go/shared"
	tabnas "github.com/tabnas/parser/go"
)

// The parser's rule passes (alchemy's shared/parser_event.go), under this
// package's names.
type (
	ParserEvent     = shared.ParserEvent
	ParserEventKind = shared.ParserEventKind
	ParserSink      = shared.ParserSink
	RulePass        = shared.RulePass
	PassAlt         = shared.PassAlt
	PassToken       = shared.PassToken
	PassCounter     = shared.PassCounter
)

// The kinds of ParserEvent.
const (
	ParserOpen  = shared.ParserOpen
	ParserClose = shared.ParserClose
	ParserEnd   = shared.ParserEnd
)

// ParserEventSource is ParserEvents/1 from a tabnas parse of one text, the
// input a transducer part reads (rs/src/source/parser_events.rs).
//
// The source installs one rule-done subscriber, which relays each rule
// pass the engine completes as a ParserEvent, on the goroutine that
// parses, into a ParserSink: an Open when a rule's open pass has run, a
// Close when its close pass has. It knows no grammar. When the engine has
// returned the parse's value it sends one End, so a document is complete
// only once it validated, as every source's is.
//
// What each pass carries is alchemy's RulePass:
//
//   - the rule's Name, its depth D and instance I (the engine's, which
//     numbers rules from 1, the start rule's, in the order it makes them),
//     the alternate that matched (Alt: P, R, B, and its groups G, sorted,
//     since they are tags whose order the runtimes' engines keep
//     differently), whether recovery forced the close, and its named
//     counters N, sorted by name;
//   - the Tokens the pass consumed: those its alternate matched (the
//     engine's O[:ON] or C[:CN]), less the B it gave back from their end,
//     which are the next rule's to consume, and none when B is more than it
//     recorded, since a close alternate can back up a token it did not
//     record. A token is its name, its value, its source text and its
//     1-based row and column; a token with no value (the end of the
//     source, or a fixed token such as `[`) has null, and a number
//     carries its source text as its lexeme when that text is a JSON
//     number spelling the value (RFC 8259), the rule the incremental
//     source keeps lexemes by;
//   - on a close pass, the rule's node when that is a scalar (Value), a
//     number with the lexeme of the rule's first open token as above, and
//     nil as null; nothing on an open pass, for a container, nor for the
//     engine's Undefined, a rule that has no value, as in Rust and
//     TypeScript.
//
// A pass made while a probe is open is speculative: the grammar parses
// ahead and rewinds past it (tabnas-bnf's probe, the engine's own for a
// serialized grammar's pd_ keys), so it is never the document's. The
// engine flags it (RuleDone.Probe), and the source drops it before any
// sink sees it.
//
// The source holds its limits as it builds each pass, before the sink
// sees it: a token's value or source text, or a pass's value, longer than
// max_scalar_bytes is refused with that limit's name, as a JSON-events
// source refuses a scalar. max_depth and max_key_bytes hold where a part's
// outputs become JsonEvents/1, on the events it writes. The abort flag is
// polled per pass, and Metrics.Events counts the events delivered.
//
// Failure mapping is ParserSource's: a sink failure is returned as it
// was; a sink that stopped is (Stop, nil); a parse cancelled through the
// caller's AbortFlag is ABORTED; any other engine error is INPUT_INVALID
// with the engine's code and position. The source takes the parser over,
// as ParserSource does: it installs its subscriber and parse budget on
// it, so a parser is given to one source, and a source runs once (a second
// Run is STREAM_REUSED, as in TypeScript; Rust's run consumes the source).
//
// The engine builds its value as it parses, so a long document's tree
// holds every record, though nothing reads the value the parse returns.
// Prune empties each list behind the stream: once the sink has seen a
// close pass whose rule's node is a list, the source truncates that list
// in place, in every rule that builds into it (tabnas.Rule.SetNode), so
// the tree holds the elements of the passes since the last close on it.
// That alters the value the engine returns, which this source never
// reads, and it is sound for a grammar whose passes do not depend on what
// its lists hold, so it is a verified capability, as the incremental
// source is: prune_test.go runs every document of every grammar the tests
// depend on pruned and not, and PrunableGrammars lists the grammars whose
// passes and outcomes never differed (rs/src/source/parser_events.rs has
// the contract's one difference, a token whose value is a container of
// the tree). A run that prunes is gated on the list, by the grammar's
// name (Grammar), and without a listed name it fails with
// STREAMABILITY_UNKNOWN before the parse, having sent nothing; Unverified
// lifts the gate for the suite.
type ParserEventSource struct {
	parser     *tabnas.Tabnas
	text       string
	limits     Limits
	abort      *AbortFlag
	metrics    *Metrics
	grammar    string
	prune      bool
	unverified bool
	used       bool
}

// NewParserEventSource is a source with default limits, its own abort flag
// and fresh metrics, that prunes nothing; the builder methods change each.
func NewParserEventSource(parser *tabnas.Tabnas, text string) *ParserEventSource {
	return &ParserEventSource{
		parser:  parser,
		text:    text,
		limits:  DefaultLimits(),
		abort:   NewAbortFlag(),
		metrics: NewMetrics(),
	}
}

// Grammar names the grammar the parser implements, by the name of the
// format it reads, as ParserSource.Grammar takes it. A run that prunes
// needs a name PrunableGrammars lists; one that does not prune needs no
// name.
func (p *ParserEventSource) Grammar(name string) *ParserEventSource {
	p.grammar = name
	return p
}

// Prune empties each list in the engine's tree once the sink has seen a
// close pass on it, so the tree does not hold the whole document. The
// passes are unchanged for a grammar PrunableGrammars lists, and Run
// refuses to prune any other, with STREAMABILITY_UNKNOWN before the parse.
func (p *ParserEventSource) Prune() *ParserEventSource {
	p.prune = true
	return p
}

// Unverified prunes whatever the verified list says. It exists for the
// differential suite that maintains the list and for nothing else: a
// grammar that reads its lists back may make other passes once they are
// emptied, and the run still succeeds.
func (p *ParserEventSource) Unverified() *ParserEventSource {
	p.unverified = true
	return p
}

// gate is why the run may not prune, when it may not.
func (p *ParserEventSource) gate() *Fail {
	if !p.prune || p.unverified {
		return nil
	}
	switch {
	case p.grammar == "":
		return NewFail(CodeStreamabilityUnknown,
			"ParserEventSource.Prune needs the grammar's name (ParserEventSource.Grammar) to check "+
				"PrunableGrammars; without one, run it without pruning")
	case !Prunable(p.grammar):
		return NewFail(CodeStreamabilityUnknown, fmt.Sprintf(
			"grammar %q is not in PrunableGrammars: the differential suite has not verified that its "+
				"passes are the same when its lists are emptied behind the stream; run it without "+
				"ParserEventSource.Prune", p.grammar))
	}
	return nil
}

// Limits sets the run's limits.
func (p *ParserEventSource) Limits(limits Limits) *ParserEventSource {
	p.limits = limits
	return p
}

// Abort sets the abort flag the run polls.
func (p *ParserEventSource) Abort(abort *AbortFlag) *ParserEventSource {
	p.abort = abort
	return p
}

// Metrics sets the metrics the run counts into.
func (p *ParserEventSource) Metrics(metrics *Metrics) *ParserEventSource {
	p.metrics = metrics
	return p
}

// relayStatus is how the run stands, as seen from inside the subscriber.
type relayStatus uint8

const (
	relayRunning relayStatus = iota
	relayStopped
	relayFailed
)

// relay is the subscriber's state: the sink and how the run stands.
type relay struct {
	sink     ParserSink
	status   relayStatus
	fail     *Fail
	maxBytes int
	abort    *AbortFlag
	stop     *AbortFlag
	events   uint64
	// prune empties the list a close pass's rule builds, once the sink has
	// seen it.
	prune bool
}

func (r *relay) send(ev ParserEvent) (Flow, *Fail) {
	r.events++
	return r.sink.ParserEvent(ev)
}

func (r *relay) failWith(f *Fail) {
	r.status, r.fail = relayFailed, f
	r.stop.Abort()
}

// onDone is one pass the engine completed.
func (r *relay) onDone(rule *tabnas.Rule, ctx *tabnas.Context, done tabnas.RuleDone) {
	if done.Probe || r.status != relayRunning {
		return
	}
	if r.abort.IsAborted() {
		r.failWith(AbortedFail())
		return
	}
	open := done.State == tabnas.OPEN
	pass, f := rulePass(rule, ctx, done, open, r.maxBytes)
	if f != nil {
		r.failWith(f)
		return
	}
	kind := ParserOpen
	if !open {
		kind = ParserClose
	}
	flow, f := r.send(ParserEvent{Kind: kind, Pass: pass})
	switch {
	case f != nil:
		r.failWith(f)
	case flow == Stop:
		r.status = relayStopped
		r.stop.Abort()
	case r.prune && !open:
		emptyList(rule)
	}
}

// emptyList truncates the list rule's node belongs to, in place, in the
// rules that build into it, when it is a list holding anything. Every
// element in it was built by passes the sink has seen, the last of them
// the close pass just sent, and what those passes carry is the sink's, so
// nothing sent so far changes. Truncating in place keeps the list's
// identity, so the rules still building into it go on doing so. The
// elements are cleared first: a truncated slice keeps its backing array,
// and that would otherwise keep every element it held reachable until a
// later append wrote over it.
func emptyList(rule *tabnas.Rule) {
	switch v := rule.NodeCell().Node.(type) {
	case []any:
		if len(v) > 0 {
			clear(v)
			rule.SetNode(v[:0])
		}
	case tabnas.ListRef:
		if len(v.Val) > 0 {
			clear(v.Val)
			v.Val = v.Val[:0]
			rule.SetNode(v)
		}
	}
}

// Run parses, relaying each pass into sink, then End. A source runs once:
// a second Run is STREAM_REUSED, before anything is installed on the
// parser, and a run that prunes an unverified grammar is refused before
// anything is installed either.
func (p *ParserEventSource) Run(sink ParserSink) (Flow, *Fail) {
	flow, f, _ := p.RunWithValue(sink)
	return flow, f
}

// RunWithValue is Run, also handing back the value the engine returned,
// when the parse returned one: the engine's tree after pruning, when the
// run prunes. It is not the run's result (the passes are): it exists so a
// test can measure what pruning left in the tree, and nothing else should
// read it.
func (p *ParserEventSource) RunWithValue(sink ParserSink) (Flow, *Fail, any) {
	if p.used {
		return Continue, NewFail(CodeStreamReused,
			"a ParserEventSource runs once: its parser carries the run's subscriber and budget; "+
				"make a new source, with a new parser, for another run"), nil
	}
	p.used = true
	if f := p.gate(); f != nil {
		return Continue, f, nil
	}
	r := &relay{sink: sink, maxBytes: p.limits.MaxScalarBytes, abort: p.abort, stop: NewAbortFlag(), prune: p.prune}
	p.parser.SubRuleDone(func(rule *tabnas.Rule, ctx *tabnas.Context, done tabnas.RuleDone) {
		r.onDone(rule, ctx, done)
	})
	installGuard(p.parser, func() bool { return !p.abort.IsAborted() && !r.stop.IsAborted() })
	value, err := p.parser.Parse(p.text)
	defer func() { p.metrics.Events.Add(r.events) }()
	switch r.status {
	case relayFailed:
		return Continue, r.fail, nil
	case relayStopped:
		return Stop, nil, nil
	}
	if err != nil {
		return Continue, engineFailure(err, p.abort), nil
	}
	flow, f := r.send(ParserEvent{Kind: ParserEnd})
	return flow, f, value
}

// rulePass is the pass as alchemy's RulePass, its sizes held to
// max_scalar_bytes.
func rulePass(rule *tabnas.Rule, ctx *tabnas.Context, done tabnas.RuleDone, open bool, max int) (*RulePass, *Fail) {
	recorded := rule.C[:rule.CN]
	if open {
		recorded = rule.O[:rule.ON]
	}
	b := 0
	if done.Alt != nil {
		b = done.Alt.B
	}
	keep := len(recorded) - b
	if keep < 0 {
		keep = 0
	}
	pass := &RulePass{Name: rule.Name, D: uint64(rule.D), I: uint64(rule.I), Forced: done.Forced, Tokens: []PassToken{}}
	for _, tok := range recorded[:keep] {
		if f := scalarHeld(len(tok.Src), "a token's source text", max); f != nil {
			return nil, f
		}
		val := scalarOf(tok.ResolveVal(rule, ctx), tok.Src, true)
		if val.Kind == shared.DatumString {
			if f := scalarHeld(len(val.Text), "a token's value", max); f != nil {
				return nil, f
			}
		}
		pass.Tokens = append(pass.Tokens, PassToken{Name: tok.Name, Val: val, Src: tok.Src, Row: uint64(tok.RI), Col: uint64(tok.CI)})
	}
	if done.Alt != nil {
		g := append([]string(nil), done.Alt.G...)
		sort.Strings(g)
		if g == nil {
			g = []string{}
		}
		pass.Alt = &PassAlt{P: done.Alt.P, R: done.Alt.R, B: uint64(done.Alt.B), G: g}
	}
	names := make([]string, 0, len(rule.N))
	for k := range rule.N {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		pass.N = append(pass.N, PassCounter{Name: k, Value: int64(rule.N[k])})
	}
	if !open && !tabnas.IsUndefined(rule.Node) {
		if _, scalar := scalarEvent(rule.Node); scalar {
			lexeme, has := "", false
			if rule.ON > 0 {
				lexeme, has = rule.O[0].Src, true
			}
			v := scalarOf(rule.Node, lexeme, has)
			if v.Kind == shared.DatumString {
				if f := scalarHeld(len(v.Text), "a rule's value", max); f != nil {
					return nil, f
				}
			}
			pass.Value = &v
		}
	}
	return pass, nil
}

// scalarOf is an engine value as a pass carries it: undefined as null,
// and a number with lexeme when has is set and that text is a JSON number
// spelling it.
func scalarOf(value any, lexeme string, has bool) Datum {
	ev, scalar := scalarEvent(value)
	if !scalar {
		return engineDatum(value)
	}
	switch ev.Kind {
	case Null:
		return NullDatum()
	case Bool:
		return BoolDatum(ev.Bool)
	case String:
		return StringDatum(ev.Text)
	}
	if has && isJSONNumber(lexeme) && parsesTo(lexeme, ev.Value) {
		return shared.NumberDatumLexeme(ev.Value, lexeme)
	}
	return NumberDatum(ev.Value)
}

// parsesTo says whether a JSON number's text reads as value.
func parsesTo(text string, value float64) bool {
	f, err := strconv.ParseFloat(text, 64)
	return err == nil && f == value
}

// engineDatum is a container engine value as a retained value, walked as
// the source walks one.
func engineDatum(value any) Datum {
	b := shared.NewDatumBuilder(math.MaxInt, "max_scalar_bytes", shared.LastWins)
	if _, f := WalkValue(value, datumSink{b}); f != nil {
		return NullDatum()
	}
	d, ok := b.Take()
	if !ok {
		return NullDatum()
	}
	return d
}

// datumSink feeds a value's events to a builder.
type datumSink struct{ b *shared.DatumBuilder }

func (s datumSink) Event(ev Event) (Flow, *Fail) {
	if ev.Kind == End {
		return Continue, nil
	}
	return Continue, s.b.Event(ev)
}

// scalarHeld is max_scalar_bytes held on one text a pass carries.
func scalarHeld(bytes int, what string, max int) *Fail {
	if bytes > max {
		return LimitFail("max_scalar_bytes", uint64(max), fmt.Sprintf("%s of %d bytes is larger than %d", what, bytes, max))
	}
	return nil
}
