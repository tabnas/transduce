// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	tabnas "github.com/tabnas/parser/go"
)

// Source drives a sink with one document's events: to completion, until
// the sink stops (Stop, nil: the document was not validated past that
// point), or until a failure.
type Source interface {
	Run(sink Sink) (Flow, *Fail)
}

// ModeKind names how ParserSource produces its events.
type ModeKind uint8

const (
	// ModeMaterialize parses the whole text, then walks the value. Sound
	// for every grammar.
	ModeMaterialize ModeKind = iota
	// ModeIncremental emits from the engine's rule events as the parse
	// proceeds. Sound for the grammars IncrementalGrammars lists.
	ModeIncremental
)

// SourceMode is how ParserSource produces its events, and for
// ModeIncremental which arrays it prunes.
type SourceMode struct {
	Kind  ModeKind
	Prune Prune
}

// MaterializeMode is SourceMode{Kind: ModeMaterialize}.
func MaterializeMode() SourceMode { return SourceMode{Kind: ModeMaterialize} }

// IncrementalMode is ModeIncremental with the given pruning.
func IncrementalMode(prune Prune) SourceMode { return SourceMode{Kind: ModeIncremental, Prune: prune} }

// PruneKind names which arrays the incremental source empties.
type PruneKind uint8

const (
	// PruneNever leaves the tree alone.
	PruneNever PruneKind = iota
	// PruneUnder empties the array whose elements the selector names (a
	// trailing EachIndex names the elements; without one the selector
	// names the array).
	PruneUnder
	// PruneAllArrays empties every array.
	PruneAllArrays
)

// Prune is which arrays the incremental source empties as it streams
// them. Pruning alters the value the engine returns, which the
// incremental source discards; it is never applied in ModeMaterialize.
type Prune struct {
	Kind     PruneKind
	Selector Selector
}

// PruneUnderSelector is PruneUnder with its selector.
func PruneUnderSelector(s Selector) Prune { return Prune{Kind: PruneUnder, Selector: s} }

// cancelCode is the engine's cancel code: what a parse budget that
// answered false reports, whether the budget is this package's abort or
// the grammar's own guard.
const cancelCode = "cancel"

// engineFailure maps an engine error to a failure. A cancel while the
// caller's flag is set is ABORTED. A cancel otherwise is a guard the
// GRAMMAR installed, so the message says so instead of "parse
// cancelled", which would read as the caller's doing; it stays
// INPUT_INVALID, because the document is what the grammar refused. Any
// other error is the input's, with the engine's code and position.
func engineFailure(err error, abort *AbortFlag) *Fail {
	te, ok := err.(*tabnas.TabnasError)
	if !ok {
		return InputFail(err.Error())
	}
	if te.Code != cancelCode {
		return FailFromTabnas(te)
	}
	if abort.IsAborted() {
		return AbortedFail()
	}
	f := FailFromTabnas(te)
	f.Message = fmt.Sprintf(
		"the grammar stopped the parse with a guard of its own (%s: %s); a grammar may refuse "+
			"nesting or size below this crate's Limits", te.Code, strings.TrimRight(te.Detail, " \t\r\n"))
	return f
}

// parseGuard is what this package installs on a parser: the engine's
// parse budget, checked at every rule step, cancelling when ok answers
// false, and chained to any budget the grammar installed so its own
// guard keeps working. The last context the engine handed it is kept,
// for what a grammar leaves in ctx.Meta.
type parseGuard struct {
	ctx *tabnas.Context
}

func installGuard(parser *tabnas.Tabnas, ok func() bool) *parseGuard {
	g := &parseGuard{}
	cfg := parser.Config()
	prevN, prev := cfg.ParseBudgetN, cfg.ParseBudgetCheck
	cfg.ParseBudgetN = 1
	cfg.ParseBudgetCheck = func(ctx *tabnas.Context) bool {
		g.ctx = ctx
		if !ok() {
			return false
		}
		if prev != nil && prevN > 0 && ctx.KI%prevN == 0 {
			return prev(ctx)
		}
		return true
	}
	return g
}

// fieldOrder is the member order a grammar that builds its records as
// plain Go maps (which have none) left behind: tabnas-csv keeps the
// header's cells in ctx.Meta["fields"]. Nil when there is none.
func (g *parseGuard) fieldOrder() []any {
	if g == nil || g.ctx == nil || g.ctx.Meta == nil {
		return nil
	}
	fields, _ := g.ctx.Meta["fields"].([]any)
	return fields
}

// ValueSource emits a parsed engine value as events, ending with End.
// It applies no limits.
type ValueSource struct {
	Value any
}

// Run walks the value into sink, then sends End.
func (v ValueSource) Run(sink Sink) (Flow, *Fail) {
	flow, f := WalkValue(v.Value, sink)
	if f != nil || flow == Stop {
		return flow, f
	}
	return sink.Event(EvEnd())
}

// WalkValue emits one engine value's events, without End.
//
// Undefined and nil are null; the metadata wrappers (Text, MapRef,
// ListRef) unwrap to their plain forms; an *OrderedMap gives its members
// in its order. A plain Go map has no order, so its members are given in
// sorted key order (numbers in a key compared as numbers).
func WalkValue(value any, sink Sink) (Flow, *Fail) {
	return walkValueOrdered(value, sink, nil)
}

// walkValueOrdered is WalkValue with the field order a grammar left
// behind, for its plain maps.
func walkValueOrdered(value any, sink Sink, fields []any) (Flow, *Fail) {
	w := walker{sink: sink, fields: fields}
	if !w.walk(value) {
		if w.fail != nil {
			return Continue, w.fail
		}
		return Stop, nil
	}
	return Continue, nil
}

type walker struct {
	sink   Sink
	fields []any
	fail   *Fail
}

func (w *walker) send(ev Event) bool {
	flow, f := w.sink.Event(ev)
	if f != nil {
		w.fail = f
		return false
	}
	return flow == Continue
}

func (w *walker) walk(value any) bool {
	if ev, ok := scalarEvent(value); ok {
		return w.send(ev)
	}
	switch v := value.(type) {
	case []any:
		return w.array(v)
	case tabnas.ListRef:
		return w.array(v.Val)
	case *tabnas.ListRef:
		return w.array(v.Val)
	case *tabnas.OrderedMap:
		if v == nil {
			return w.send(EvNull())
		}
		return w.object(v.Keys, func(k string) any { return v.Vals[k] })
	case tabnas.OrderedMap:
		return w.object(v.Keys, func(k string) any { return v.Vals[k] })
	case map[string]any:
		return w.object(plainKeys(v, w.fields), func(k string) any { return v[k] })
	case tabnas.MapRef:
		return w.object(plainKeys(v.Val, w.fields), func(k string) any { return v.Val[k] })
	case *tabnas.MapRef:
		return w.object(plainKeys(v.Val, w.fields), func(k string) any { return v.Val[k] })
	}
	// Anything else a grammar may build: a typed slice or a string-keyed
	// map by reflection, and otherwise its text.
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		items := make([]any, rv.Len())
		for i := range items {
			items[i] = rv.Index(i).Interface()
		}
		return w.array(items)
	case reflect.Map:
		if rv.Type().Key().Kind() == reflect.String {
			m := make(map[string]any, rv.Len())
			for _, k := range rv.MapKeys() {
				m[k.String()] = rv.MapIndex(k).Interface()
			}
			return w.object(plainKeys(m, nil), func(k string) any { return m[k] })
		}
	case reflect.Pointer, reflect.Interface:
		if rv.IsNil() {
			return w.send(EvNull())
		}
		return w.walk(rv.Elem().Interface())
	}
	return w.send(EvString(fmt.Sprint(value)))
}

func (w *walker) array(items []any) bool {
	if !w.send(EvArrayStart()) {
		return false
	}
	for _, item := range items {
		if !w.walk(item) {
			return false
		}
	}
	return w.send(EvArrayEnd())
}

func (w *walker) object(keys []string, get func(string) any) bool {
	if !w.send(EvObjectStart()) {
		return false
	}
	for _, k := range keys {
		if !w.send(EvKey(k)) || !w.walk(get(k)) {
			return false
		}
	}
	return w.send(EvObjectEnd())
}

// scalarEvent is the event for a scalar engine value, and false for a
// container (or a value it does not know).
func scalarEvent(value any) (Event, bool) {
	if value == nil || tabnas.IsUndefined(value) {
		return EvNull(), true
	}
	switch v := value.(type) {
	case bool:
		return EvBool(v), true
	case float64:
		return EvNumber(v), true
	case float32:
		return EvNumber(float64(v)), true
	case int:
		return EvNumber(float64(v)), true
	case int64:
		return EvNumber(float64(v)), true
	case int32:
		return EvNumber(float64(v)), true
	case uint64:
		return EvNumber(float64(v)), true
	case uint32:
		return EvNumber(float64(v)), true
	case string:
		return EvString(v), true
	case tabnas.Text:
		return EvString(v.Str), true
	case *tabnas.Text:
		if v == nil {
			return EvNull(), true
		}
		return EvString(v.Str), true
	}
	return Event{}, false
}

// isContainerValue reports whether an engine value is a list or a map.
func isContainerValue(value any) bool {
	switch v := value.(type) {
	case []any, tabnas.ListRef, map[string]any, tabnas.MapRef:
		return true
	case *tabnas.OrderedMap:
		return v != nil
	}
	return false
}

// plainKeys is the order a plain Go map's members are walked in: the
// names the grammar's field order gives, as far as the map holds them,
// then the rest in sorted order, numbers within a key compared as
// numbers (field~2 before field~10).
func plainKeys(m map[string]any, fields []any) []string {
	keys := make([]string, 0, len(m))
	var placed map[string]bool
	if len(fields) > 0 {
		placed = make(map[string]bool, len(fields))
		for _, f := range fields {
			name, ok := f.(string)
			if !ok {
				continue
			}
			if _, in := m[name]; in && !placed[name] {
				placed[name] = true
				keys = append(keys, name)
			}
		}
	}
	rest := make([]string, 0, len(m)-len(keys))
	for k := range m {
		if !placed[k] {
			rest = append(rest, k)
		}
	}
	sort.Slice(rest, func(i, j int) bool { return naturalLess(rest[i], rest[j]) })
	return append(keys, rest...)
}

// naturalLess orders two keys by text, with a trailing run of digits
// compared as a number.
func naturalLess(a, b string) bool {
	pa, na, oka := splitNumber(a)
	pb, nb, okb := splitNumber(b)
	if oka && okb && pa == pb && na != nb {
		return na < nb
	}
	return a < b
}

func splitNumber(s string) (string, int, bool) {
	i := len(s)
	for i > 0 && s[i-1] >= '0' && s[i-1] <= '9' {
		i--
	}
	if i == len(s) || len(s)-i > 9 {
		return s, 0, false
	}
	n, _ := strconv.Atoi(s[i:])
	return s[:i], n, true
}
