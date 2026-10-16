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
//     counts from 0 here and in Rust and from 1 in TypeScript:
//     DIVERGENCE.md 6), the alternate that matched (Alt: P, R, B, and its
//     groups G, sorted, since they are tags whose order the runtimes'
//     engines keep differently), whether recovery forced the close, and
//     its named counters N, sorted by name;
//   - the Tokens the pass consumed: those its alternate matched (the
//     engine's O[:ON] or C[:CN]), less the B it gave back from their end,
//     which are the next rule's to consume, and none when B is more than it
//     recorded, since a close alternate can back up a token it did not
//     record. A token is its name, its value, its source text and its
//     1-based row and column; a token with no value (the end of the
//     source, a fixed token such as `[`, which Rust's engine gives its
//     text: DIVERGENCE.md 7) has null, and a number carries its source text
//     as its lexeme when that text is a JSON number spelling the value (RFC
//     8259), the rule the incremental source keeps lexemes by;
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
type ParserEventSource struct {
	parser  *tabnas.Tabnas
	text    string
	limits  Limits
	abort   *AbortFlag
	metrics *Metrics
	used    bool
}

// NewParserEventSource is a source with default limits, its own abort flag
// and fresh metrics; the builder methods change each.
func NewParserEventSource(parser *tabnas.Tabnas, text string) *ParserEventSource {
	return &ParserEventSource{
		parser:  parser,
		text:    text,
		limits:  DefaultLimits(),
		abort:   NewAbortFlag(),
		metrics: NewMetrics(),
	}
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
	}
}

// Run parses, relaying each pass into sink, then End. A source runs once:
// a second Run is STREAM_REUSED, before anything is installed on the
// parser.
func (p *ParserEventSource) Run(sink ParserSink) (Flow, *Fail) {
	if p.used {
		return Continue, NewFail(CodeStreamReused,
			"a ParserEventSource runs once: its parser carries the run's subscriber and budget; "+
				"make a new source, with a new parser, for another run")
	}
	p.used = true
	r := &relay{sink: sink, maxBytes: p.limits.MaxScalarBytes, abort: p.abort, stop: NewAbortFlag()}
	p.parser.SubRuleDone(func(rule *tabnas.Rule, ctx *tabnas.Context, done tabnas.RuleDone) {
		r.onDone(rule, ctx, done)
	})
	installGuard(p.parser, func() bool { return !p.abort.IsAborted() && !r.stop.IsAborted() })
	_, err := p.parser.Parse(p.text)
	defer func() { p.metrics.Events.Add(r.events) }()
	switch r.status {
	case relayFailed:
		return Continue, r.fail
	case relayStopped:
		return Stop, nil
	}
	if err != nil {
		return Continue, engineFailure(err, p.abort)
	}
	return r.send(ParserEvent{Kind: ParserEnd})
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
