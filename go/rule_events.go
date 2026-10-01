// Copyright (c) 2026 tabnas, MIT License

//go:build tabnas_nodecell

package tabnastransduce

// The rule-event adapter: JsonEvents/1 from a live tabnas parse.
//
// The engine tells a rule-done subscriber about every rule pass, with
// the rule. The adapter watches the containers the rules build into, by
// their node cell (tabnas.Rule.NodeCell: the rule that holds the
// container's authoritative copy, the Go counterpart of Rust's shared
// Rc cell), and nothing grammar-specific, which is what lets one adapter
// serve every grammar. The algorithm is the Rust crate's
// (rs/src/source/rule_events.rs), kept exactly:
//
//   - A rule whose node is a container at the end of its OPEN pass, in a
//     cell not already on the frame stack, STARTS a container. Pushed and
//     replaced rules share their parent's cell, so a pair or an element
//     rule starts nothing.
//   - At the end of a CLOSE pass whose node cell is the top frame's, a
//     longer container means new entries: the tail of a list, the last
//     members of a map (insertion ordered). A new entry that is the
//     container which completed last was already streamed; any other is
//     walked whole, late.
//   - A key is announced early when the rule stashed it in U["key"] at
//     its OPEN pass; a key the adapter only learns at insertion is
//     announced then.
//   - A repeated member name does not grow the map: the announcing
//     rule's close pass HOLDS what the map has under the name until the
//     adapter's next event shows the pass stood (a failed pass is the last
//     the engine reports). A held scalar is streamed, so the stream reads
//     `Key a, 1, Key a, 2` and a router's Duplicates policy decides; a
//     container the adapter just streamed is complete already; any other
//     container is a merge whose first half has left: DUPLICATE_MEMBER.
//   - A map rewritten after streaming (a YAML merge key) is caught by an
//     order-independent hash of the names streamed into it, compared
//     when its frame ends: STREAMABILITY_UNKNOWN.
//   - The top frame ENDS when a rule at the frame's depth whose node is
//     the frame's cell closes, or the rule that started it closes, unless
//     that close REPLACES the rule (alt.r).
//   - A root scalar is emitted at the depth-0 rule's close, unless that
//     close replaces the rule; a frame that starts at the root after a
//     root value completed, and a root rule that closes for good over a
//     value other than the one streamed, are refused with
//     STREAMABILITY_UNKNOWN.
//
// Number lexemes are best effort: at the close of a rule whose node is a
// number, the first open token's source text is kept when it is a JSON
// number that reads as the same value, and attached to that number when
// it is inserted next.
//
// Pruning drops the elements of a list frame through the cell
// (tabnas.Rule.SetNode) after they were emitted, so a document's rows do
// not pile up in the engine's tree while the transducer streams them.

import (
	"fmt"
	"hash/maphash"
	"reflect"
	"strconv"
	"strings"
	"unsafe"

	tabnasjson "github.com/tabnas/json/go"
	tabnas "github.com/tabnas/parser/go"
)

type ruleFrame struct {
	cell  *tabnas.Rule
	array bool
	len   int
	ruleI int
	depth int
	// key is the last member name streamed; the one announced early for
	// the next member, when hasKey.
	key    string
	hasKey bool
	prune  bool
	// names is the wrapping sum of the hashes of the distinct names
	// streamed into this map.
	names uint64
}

// held is a member announced early whose rule closed without growing
// the map, held until the adapter's next event shows that the close
// stood. hasValue is false when the grammar announced a member it never
// stored.
type held struct {
	key      string
	value    any
	hasValue bool
}

type adapterStatus uint8

const (
	statusRunning adapterStatus = iota
	statusStopped
	statusFailed
)

type adapter struct {
	frames []ruleFrame
	open   int
	// lastCompleted is the container that completed last: the next entry
	// inserted into its parent is this one, already streamed.
	lastCompleted    any
	hasLastCompleted bool
	held             *held
	lexeme           string
	lexemeValue      float64
	lexemeReady      bool
	sink             *Guarded
	status           adapterStatus
	failure          *Fail
	stop             *AbortFlag
	pruneKind        PruneKind
	pruneMatcher     *Matcher
	pruneHit         bool
	rootDone         bool
	emitted          bool
	seed             maphash.Seed
}

func newAdapter(sink Sink, limits Limits, abort *AbortFlag, metrics *Metrics, prune Prune, stop *AbortFlag) *adapter {
	a := &adapter{
		sink:      NewGuarded(sink, limits, abort, metrics),
		stop:      stop,
		pruneKind: prune.Kind,
		seed:      maphash.MakeSeed(),
	}
	if prune.Kind == PruneUnder {
		// A selector that names the elements names the array one step
		// up; one that names the array is taken as is.
		steps := prune.Selector.Steps()
		array := prune.Selector
		if n := len(steps); n > 0 && steps[n-1].Kind == StepEachIndex {
			array = SelectorOf(steps[:n-1]...)
		}
		a.pruneMatcher = NewMatcher([]Selector{array})
	}
	return a
}

// install puts the subscriber and the parse budget on parser. The budget
// cancels the parse when the caller's flag or the adapter's own stop
// flag is set.
func (a *adapter) install(parser *tabnas.Tabnas, abort *AbortFlag) {
	parser.SubRuleDone(func(rule *tabnas.Rule, _ *tabnas.Context, done tabnas.RuleDone) {
		a.onDone(rule, done)
	})
	installGuard(parser, func() bool { return !abort.IsAborted() && !a.stop.IsAborted() })
}

// reset readies the adapter for another document into the same sink:
// the line sources parse one value per line with one parser.
func (a *adapter) reset() {
	a.open = 0
	a.lastCompleted, a.hasLastCompleted = nil, false
	a.held = nil
	a.lexemeReady = false
	a.pruneHit = false
	a.rootDone = false
	a.emitted = false
}

// complete reports whether one whole root value was emitted and every
// frame closed.
func (a *adapter) complete() bool { return a.rootDone && a.open == 0 }

// idle reports whether nothing at all was emitted for the current
// document, so the value the engine returned can be walked in its place.
func (a *adapter) idle() bool { return !a.emitted }

// walkWhole emits a whole value through the sink, outside the parse.
func (a *adapter) walkWhole(value any) (Flow, *Fail) {
	a.emitted = true
	return WalkValue(value, a.sink)
}

// send sends one event straight to the sink, outside the parse.
func (a *adapter) send(ev Event) (Flow, *Fail) { return a.sink.Event(ev) }

func (a *adapter) fail(f *Fail) {
	a.status = statusFailed
	a.failure = f
	a.stop.Abort()
}

// emit emits one event; false means stop: the sink stopped or failed,
// and the parse has been told to cancel.
func (a *adapter) emit(ev Event) bool {
	a.emitted = true
	if a.pruneMatcher != nil {
		hit, f := a.pruneMatcher.Event(ev)
		if f != nil {
			a.fail(f)
			return false
		}
		a.pruneHit = hit.Kind == HitStart && hit.Begins > 0
	}
	flow, f := a.sink.Event(ev)
	switch {
	case f != nil:
		a.fail(f)
		return false
	case flow == Stop:
		a.status = statusStopped
		a.stop.Abort()
		return false
	}
	return true
}

// scalar emits a scalar value, with the remembered lexeme when it is
// this number's.
func (a *adapter) scalar(value any) bool {
	ev, ok := scalarEvent(value)
	if !ok {
		return true
	}
	if ev.Kind == Number {
		if a.lexemeReady && a.lexemeValue == ev.Value {
			a.lexemeReady = false
			ev.Lexeme, ev.HasLexeme = a.lexeme, true
		}
	}
	return a.emit(ev)
}

// walk emits a whole value the adapter did not see being built (late).
func (a *adapter) walk(value any) bool {
	a.lexemeReady = false
	if !isContainerValue(value) {
		return a.scalar(value)
	}
	if items, ok := listItems(value); ok {
		if !a.emit(EvArrayStart()) {
			return false
		}
		for _, item := range items {
			if !a.walk(item) {
				return false
			}
		}
		return a.emit(EvArrayEnd())
	}
	if !a.emit(EvObjectStart()) {
		return false
	}
	for _, k := range memberNames(value) {
		v, _ := memberOf(value, k)
		if !a.emit(EvKey(k)) || !a.walk(v) {
			return false
		}
	}
	return a.emit(EvObjectEnd())
}

// pushFrame opens a frame. Its len starts at zero whatever the container
// holds: entries present when the adapter first sees a cell were never
// streamed (jsonic's promoted first value), and the next close pass
// emits them before the new ones.
func (a *adapter) pushFrame(cell *tabnas.Rule, array bool, rule *tabnas.Rule, prune bool) {
	f := ruleFrame{cell: cell, array: array, ruleI: rule.I, depth: rule.D, prune: prune}
	if a.open == len(a.frames) {
		a.frames = append(a.frames, f)
	} else {
		a.frames[a.open] = f
	}
	a.open++
}

// onDone is the subscriber's body.
func (a *adapter) onDone(rule *tabnas.Rule, done tabnas.RuleDone) {
	if a.status != statusRunning {
		return
	}
	// Any further event means the pass that held a member stood.
	if !a.flushHeld() {
		return
	}
	if done.Alt != nil && done.Alt.Err != nil {
		return
	}
	cell := rule.NodeCell()
	if done.State == tabnas.OPEN {
		a.opened(rule, cell)
		return
	}
	replaces := done.Alt != nil && done.Alt.R != ""
	a.closed(rule, cell, replaces)
}

func (a *adapter) onStack(cell *tabnas.Rule) bool {
	for i := 0; i < a.open; i++ {
		if a.frames[i].cell == cell {
			return true
		}
	}
	return false
}

func (a *adapter) opened(rule *tabnas.Rule, cell *tabnas.Rule) {
	node := cell.Node
	array, length, ok := containerLen(node)
	if !ok {
		return
	}
	if !a.onStack(cell) {
		if a.open == 0 && a.rootDone {
			a.fail(wrappedRoot())
			return
		}
		if a.hasLastCompleted {
			// The container that completed last was never stored in the
			// one around it, and the grammar is opening another: what
			// left cannot be taken back.
			a.fail(unstoredContainer())
			return
		}
		if a.open > 0 {
			if top := &a.frames[a.open-1]; !top.array && !top.hasKey {
				// A container opening in a map whose next member has no
				// key yet: its events would leave before the key.
				a.fail(valueBeforeKey())
				return
			}
		}
		a.lexemeReady = false
		ev := EvObjectStart()
		if array {
			ev = EvArrayStart()
		}
		if !a.emit(ev) {
			return
		}
		prune := false
		switch a.pruneKind {
		case PruneAllArrays:
			prune = array
		case PruneUnder:
			prune = array && a.pruneHit
		}
		a.pushFrame(cell, array, rule, prune)
		return
	}
	if array {
		return
	}
	top := &a.frames[a.open-1]
	if top.cell == cell && !top.hasKey && top.len == length {
		if k, ok := rule.U["key"].(string); ok {
			if !a.emit(EvKey(k)) {
				return
			}
			top = &a.frames[a.open-1]
			top.key = k
			top.hasKey = true
		}
	}
}

func (a *adapter) closed(rule *tabnas.Rule, cell *tabnas.Rule, replaces bool) {
	// A root scalar's lexeme has to be known before it is emitted below.
	a.rememberLexeme(rule, cell)
	// Whether a whole root value had left before this pass: the pass that
	// completes the root is exempt from the check at the end.
	wasDone := a.rootDone

	pruneFrom := -1
	if a.open > 0 && a.frames[a.open-1].cell == cell {
		topI := a.open - 1
		node := cell.Node
		if array, length, ok := containerLen(node); ok {
			old := a.frames[topI].len
			if !array && a.frames[topI].hasKey && length == old {
				// The announced member did not grow the map, and the rule
				// that announced it is closing: it replaced or merged an
				// earlier member of the same name, or the pass failed the
				// parse before storing it. Hold what the map has under the
				// name; the next event decides.
				if k, ok := rule.U["key"].(string); ok && k == a.frames[topI].key {
					a.holdPending(node, topI)
				}
			}
			if length > old {
				for i := old; i < length; i++ {
					key, value, ok := entryAt(node, i)
					if !ok {
						break
					}
					if !array {
						top := &a.frames[topI]
						if top.hasKey && top.key != key {
							// A member landed ahead of the one announced;
							// that one keeps its place in the stream.
							if !a.settlePending(node, topI) {
								return
							}
						}
						top = &a.frames[topI]
						announced := top.hasKey
						top.hasKey = false
						top.names += maphash.String(a.seed, key)
						if !announced {
							top.key = key
							if !a.emit(EvKey(key)) {
								return
							}
						}
					}
					if !a.entry(value) {
						return
					}
				}
				a.frames[topI].len = length
				a.lexemeReady = false
				if array && a.frames[topI].prune {
					pruneFrom = old
				}
			}
		}
	}
	if pruneFrom >= 0 {
		switch v := cell.Node.(type) {
		case []any:
			rule.SetNode(v[:pruneFrom])
		case tabnas.ListRef:
			v.Val = v.Val[:pruneFrom]
			rule.SetNode(v)
		}
		a.frames[a.open-1].len = pruneFrom
	}

	if replaces {
		return
	}

	if a.open > 0 {
		topI := a.open - 1
		top := &a.frames[topI]
		if top.ruleI == rule.I || (top.cell == cell && top.depth == rule.D) {
			array := top.array
			// A member held by this very pass goes before the frame ends.
			if !a.flushHeld() {
				return
			}
			if a.frames[topI].hasKey {
				// The announcing rule never closed on this cell: the
				// member is whatever the map holds under the name now.
				if !a.settlePending(cell.Node, topI) {
					return
				}
			}
			if a.hasLastCompleted {
				// A container built inside this one was streamed and never
				// stored: the frame cannot end as the walk's would.
				a.fail(unstoredContainer())
				return
			}
			node := cell.Node
			if !array {
				var names uint64
				for _, k := range memberNames(node) {
					names += maphash.String(a.seed, k)
				}
				if names != a.frames[topI].names {
					a.fail(rewrittenMap())
					return
				}
			}
			a.open--
			a.lexemeReady = false
			ev := EvObjectEnd()
			if array {
				ev = EvArrayEnd()
			}
			if !a.emit(ev) {
				return
			}
			if isContainerValue(node) {
				a.lastCompleted, a.hasLastCompleted = node, true
			} else {
				a.lastCompleted, a.hasLastCompleted = nil, false
			}
			if a.open == 0 {
				a.rootDone = true
			}
		}
	}

	if rule.D == 0 && a.open == 0 && !a.rootDone {
		if node := cell.Node; !isContainerValue(node) {
			if !a.scalar(node) {
				return
			}
			a.rootDone = true
		}
	}

	// The parse root closing for good after the root value left: the cell
	// must still hold what was streamed.
	if wasDone && rule.D == 0 && a.open == 0 {
		node := cell.Node
		streamed := false
		switch {
		case a.hasLastCompleted && isContainerValue(node):
			streamed = sameContainer(node, a.lastCompleted)
		case !a.hasLastCompleted && !isContainerValue(node):
			// A root scalar was streamed, and the cell holds a scalar.
			streamed = true
		}
		if !streamed {
			a.fail(rewrittenRoot())
		}
	}
}

// entry emits one entry that just landed in the top frame's container: a
// scalar as itself, the container that completed last as nothing (it
// has been streamed), any other container whole, late.
func (a *adapter) entry(value any) bool {
	if a.hasLastCompleted {
		if isContainerValue(value) && sameContainer(value, a.lastCompleted) {
			a.lastCompleted, a.hasLastCompleted = nil, false
			return true
		}
		// Something else landed: the container streamed last was never
		// stored, and its events cannot be taken back.
		a.fail(unstoredContainer())
		return false
	}
	if !isContainerValue(value) {
		return a.scalar(value)
	}
	return a.walk(value)
}

// settlePending streams what the map holds now under the member
// announced on frame topI, which did not grow its map.
func (a *adapter) settlePending(node any, topI int) bool {
	top := &a.frames[topI]
	top.hasKey = false
	value, ok := memberOf(node, top.key)
	return a.settle(top.key, value, ok)
}

// holdPending is settlePending, but the member is held rather than
// streamed: the announcing rule's close pass may be one the engine
// reports although a lifecycle action failed the parse in it.
func (a *adapter) holdPending(node any, topI int) {
	top := &a.frames[topI]
	top.hasKey = false
	value, ok := memberOf(node, top.key)
	a.held = &held{key: top.key, value: value, hasValue: ok}
}

// flushHeld streams the member held at the last pass, if any. false
// means stop.
func (a *adapter) flushHeld() bool {
	h := a.held
	if h == nil {
		return true
	}
	a.held = nil
	return a.settle(h.key, h.value, h.hasValue)
}

// settle streams what a map holds under a repeated member's name.
func (a *adapter) settle(key string, value any, ok bool) bool {
	if !ok {
		a.fail(NewFail(CodeStreamabilityUnknown, fmt.Sprintf(
			"the grammar announced member %s and never stored it, which the incremental source "+
				"cannot follow; run it with ModeMaterialize", strconv.Quote(key))))
		return false
	}
	if !isContainerValue(value) {
		return a.scalar(value)
	}
	if a.hasLastCompleted && sameContainer(value, a.lastCompleted) {
		a.lastCompleted, a.hasLastCompleted = nil, false
		return true
	}
	a.fail(mergedMember(key))
	return false
}

// rememberLexeme keeps the source text of a number rule's first token
// when it is a JSON number spelling the node's value.
func (a *adapter) rememberLexeme(rule *tabnas.Rule, cell *tabnas.Rule) {
	n, ok := cell.Node.(float64)
	if !ok {
		return
	}
	if rule.O0 == nil || rule.O0 == tabnas.NoToken {
		return
	}
	src := rule.O0.Src
	if v, err := strconv.ParseFloat(src, 64); err == nil && isJSONNumber(src) && v == n {
		a.lexeme = src
		a.lexemeValue = n
		a.lexemeReady = true
	} else {
		a.lexemeReady = false
	}
}

// runIncremental is ParserSource's incremental path.
func runIncremental(p *ParserSource, sink Sink) (Flow, *Fail, any) {
	stop := NewAbortFlag()
	a := newAdapter(sink, p.limits, p.abort, p.metrics, p.mode.Prune, stop)
	a.install(p.parser, p.abort)
	value, err := p.parser.Parse(p.text)
	flow, f := Continue, (*Fail)(nil)
	if a.status == statusRunning {
		switch {
		case err == nil && a.complete():
			flow, f = a.send(EvEnd())
		case err == nil && a.idle():
			flow, f = a.walkWhole(value)
			if f == nil && flow == Continue {
				flow, f = a.send(EvEnd())
			}
		case err == nil:
			f = notStreamable()
		default:
			f = engineFailure(err, p.abort)
		}
	}
	a.sink.Flush()
	switch a.status {
	case statusFailed:
		return Continue, a.failure, value
	case statusStopped:
		return Stop, nil, value
	}
	return flow, f, value
}

// jsonlIncremental is JSON Lines with the adapter: one parser, one
// subscriber, the adapter reset before each line.
func jsonlIncremental(l *LinesSource, sink Sink) (Flow, *Fail) {
	stop := NewAbortFlag()
	a := newAdapter(sink, l.limits, l.abort, l.metrics, Prune{}, stop)
	parser := tabnasjson.Make()
	a.install(parser, l.abort)
	lines := newLines(l.reader, l.limits.MaxRecordBytes)
	flow, f := a.send(EvArrayStart())
	if f == nil && flow == Continue {
	loop:
		for {
			number, line, ok, lf := lines.nextLine()
			if lf != nil {
				f = lf
				break
			}
			if !ok {
				break
			}
			if strings.TrimSpace(line) == "" {
				continue
			}
			value, err := parser.Parse(line)
			switch a.status {
			case statusStopped:
				flow = Stop
				break loop
			case statusFailed:
				break loop
			}
			switch {
			case err == nil && a.complete():
				a.reset()
			case err == nil && a.idle():
				fl, wf := a.walkWhole(value)
				if wf != nil {
					f = wf
					break loop
				}
				if fl == Stop {
					flow = Stop
					break loop
				}
				a.reset()
			case err == nil:
				f = notStreamable()
				break loop
			default:
				f = lineFailure(err, number, l.abort)
				break loop
			}
		}
	}
	if f == nil && flow == Continue && a.status == statusRunning {
		flow, f = a.send(EvArrayEnd())
		if f == nil && flow == Continue {
			flow, f = a.send(EvEnd())
		}
	}
	a.sink.Flush()
	switch a.status {
	case statusFailed:
		return Continue, a.failure
	case statusStopped:
		return Stop, nil
	}
	return flow, f
}

// The failures the adapter refuses with.

func notStreamable() *Fail {
	return NewFail(CodeStreamabilityUnknown,
		"the grammar did not build its value through rule events the incremental source can follow "+
			"(the events did not amount to one whole document); run it with ModeMaterialize")
}

func wrappedRoot() *Fail {
	return NewFail(CodeStreamabilityUnknown,
		"the grammar wrapped a value already streamed as the document's root in a list (a YAML "+
			"stream of several documents, a jsonic top-level implicit list); the incremental source "+
			"cannot take the root back, so run it with ModeMaterialize")
}

func rewrittenRoot() *Fail {
	return NewFail(CodeStreamabilityUnknown,
		"the grammar replaced the document's root after the incremental source streamed it (a YAML "+
			"stream of several documents is wrapped in a list when the source ends); the incremental "+
			"source cannot take the root back, so run it with ModeMaterialize")
}

func unstoredContainer() *Fail {
	return NewFail(CodeStreamabilityUnknown,
		"the grammar built a container the incremental source streamed and then never stored it in "+
			"the container around it (jsonic drops a pair inside a list when list.pair is off), so the "+
			"stream would not be the document's; the incremental source cannot follow a grammar that "+
			"builds a container so: run it with ModeMaterialize")
}

func valueBeforeKey() *Fail {
	return NewFail(CodeStreamabilityUnknown,
		"the grammar opened a container inside a map before announcing the member's key (it builds "+
			"the value, or a key that is itself a container, in a rule of its own and names the member "+
			"only when the pair closes); its events would leave before the key, so the stream would "+
			"not be the document's; the incremental source cannot follow a grammar that builds a "+
			"member so: run it with ModeMaterialize")
}

func rewrittenMap() *Fail {
	return NewFail(CodeStreamabilityUnknown,
		"the grammar rewrote the members of a map after the incremental source streamed them (a YAML "+
			"merge key does), so the stream would not be the document's; run it with ModeMaterialize")
}

func mergedMember(key string) *Fail {
	return NewFail(CodeDuplicateMember, fmt.Sprintf(
		"member %s appears twice and the grammar merged the two values; the first was already "+
			"streamed, so the incremental source cannot emit the merged member: run it with "+
			"ModeMaterialize", strconv.Quote(key)))
}

// Engine values, as the adapter reads them.

// containerLen is whether a value is a list (array) and how many entries
// it holds; ok is false for a value that is not a container.
func containerLen(v any) (array bool, length int, ok bool) {
	switch c := v.(type) {
	case []any:
		return true, len(c), true
	case tabnas.ListRef:
		return true, len(c.Val), true
	case *tabnas.OrderedMap:
		if c == nil {
			return false, 0, false
		}
		return false, len(c.Keys), true
	case map[string]any:
		return false, len(c), true
	case tabnas.MapRef:
		return false, len(c.Val), true
	}
	return false, 0, false
}

func listItems(v any) ([]any, bool) {
	switch c := v.(type) {
	case []any:
		return c, true
	case tabnas.ListRef:
		return c.Val, true
	}
	return nil, false
}

// memberNames is the member names of a map, in its order (a plain Go
// map's sorted).
func memberNames(v any) []string {
	switch c := v.(type) {
	case *tabnas.OrderedMap:
		if c != nil {
			return c.Keys
		}
	case map[string]any:
		return plainKeys(c, nil)
	case tabnas.MapRef:
		return plainKeys(c.Val, nil)
	}
	return nil
}

// memberOf is the member of a map under key.
func memberOf(v any, key string) (any, bool) {
	switch c := v.(type) {
	case *tabnas.OrderedMap:
		if c != nil {
			return c.Get(key)
		}
	case map[string]any:
		x, ok := c[key]
		return x, ok
	case tabnas.MapRef:
		x, ok := c.Val[key]
		return x, ok
	}
	return nil, false
}

// entryAt is the i-th entry of a container: its key ("" in a list) and
// its value.
func entryAt(v any, i int) (string, any, bool) {
	if items, ok := listItems(v); ok {
		if i < len(items) {
			return "", items[i], true
		}
		return "", nil, false
	}
	names := memberNames(v)
	if i < len(names) {
		x, _ := memberOf(v, names[i])
		return names[i], x, true
	}
	return "", nil, false
}

// sameContainer reports whether two values are the same container: maps
// by pointer, lists by backing array. A list with no backing array of
// its own (an empty one: every empty slice shares one) matches another
// such list of the same type, which is the container that completed
// last in every grammar the suite runs.
func sameContainer(a, b any) bool {
	switch x := a.(type) {
	case *tabnas.OrderedMap:
		y, ok := b.(*tabnas.OrderedMap)
		return ok && x != nil && x == y
	case map[string]any:
		y, ok := b.(map[string]any)
		return ok && x != nil && y != nil && reflect.ValueOf(x).Pointer() == reflect.ValueOf(y).Pointer()
	case tabnas.MapRef:
		y, ok := b.(tabnas.MapRef)
		return ok && x.Val != nil && y.Val != nil &&
			reflect.ValueOf(x.Val).Pointer() == reflect.ValueOf(y.Val).Pointer()
	case []any:
		y, ok := b.([]any)
		return ok && sameBacking(x, y)
	case tabnas.ListRef:
		y, ok := b.(tabnas.ListRef)
		return ok && sameBacking(x.Val, y.Val)
	}
	return false
}

func sameBacking(x, y []any) bool {
	if cap(x) > 0 && cap(y) > 0 {
		return unsafe.SliceData(x) == unsafe.SliceData(y)
	}
	return cap(x) == 0 && cap(y) == 0 && len(x) == 0 && len(y) == 0
}
