// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

import (
	"math"
	"strings"

	"github.com/tabnas/alchemy/go/shared"
)

// The spec of the library's transduce-tree (alchemy's shared/tree.go),
// under this package's names.
type (
	TreeSpec      = shared.TreeSpec
	TreeRule      = shared.TreeRule
	TreeKey       = shared.TreeKey
	TreeScalar    = shared.TreeScalar
	TreeContainer = shared.TreeContainer
)

// The containers a rule's passes begin and end.
const (
	TreeNoContainer = shared.TreeNoContainer
	TreeObject      = shared.TreeObject
	TreeArray       = shared.TreeArray
)

// TransduceTree is alchemy's transduce-tree run natively, the parser's
// rule passes (ParserEvents/1) in and a tree's JsonEvents/1 out, as a
// TreeSpec keyed by rule name says (rs/src/transduce_tree.rs).
//
// The library's text (stdlib/transduce.alc) is the specification, and
// alchemy's TreeSpec documents what a native must do: each pass is looked
// up by its rule's name, one lookup per pass whatever the spec's size; an
// open pass announces its key, then begins its container or yields its
// scalar from one of its tokens, then announces its member; a close pass
// yields its text, when its rule is a :text rule, then ends its
// container, or yields its own value; and a container whose rule is
// replaced by a rule the spec names a container is one container, so that
// close ends nothing and the replacing open begins nothing. The state is
// the count of open containers, whether a close handed its container on,
// and the text each open :text rule has gathered, to which every pass
// writes its tokens' source text, so the native retains the document's
// nesting and an open rule's text, and nothing else of its width or
// length, as the library's step does. alchemy-cli's differential test
// holds the two to the same bytes.
//
// A text is held to the spec's MaxTextBytes as it grows, at the joins the
// library's `string-join` makes and in their order: a pass's tokens
// together, then the innermost text with them, and a closed text with the
// text around it, each refused past the bound with alchemy's
// TreeTextRefusal. The routers say this native reads :member and :text
// (routers.go), which is what lets the lowering hand it a spec that uses
// either.
//
// A pass's events are worked out before the first of them is written, as
// the library's step returns them together: a pass that fails writes
// none. The failures are the library's, with its codes and texts, and in
// its order: an open pass's key, then what it begins or yields, then its
// tokens' text, then its member; a close pass's text, then what it ends
// or yields.
type TransduceTree struct {
	spec  TreeSpec
	open  int
	carry bool
	// texts is the text each open :text rule has gathered, the innermost
	// last.
	texts []*strings.Builder
	down  Sink
}

// NewTransduceTree is the library's transduce-tree, natively, writing to
// down.
func NewTransduceTree(spec TreeSpec, down Sink) *TransduceTree {
	return &TransduceTree{spec: spec, down: down}
}

// ParserEvent takes one pass, or the end of the passes.
func (t *TransduceTree) ParserEvent(ev ParserEvent) (Flow, *Fail) {
	switch {
	case ev.Kind == ParserEnd:
		if t.open > 0 {
			return Continue, NewFail(CodeProtocolOrderError, "the rule passes ended inside a container, which a tree's never do")
		}
		return t.down.Event(EvEnd())
	case ev.Pass == nil:
		return Continue, shared.ProtocolFail("a parser event's open or close came without its pass")
	case ev.Kind == ParserOpen:
		return t.openPass(ev.Pass)
	case ev.Kind == ParserClose:
		return t.closePass(ev.Pass)
	}
	return Continue, shared.ProtocolFail("a parser event is an open, a close or the end, not " + ev.Kind.String())
}

// max is the most bytes a text may hold: the spec's MaxTextBytes, which
// zero leaves unbounded.
func (t *TransduceTree) max() int {
	if t.spec.MaxTextBytes == 0 {
		return math.MaxInt
	}
	return t.spec.MaxTextBytes
}

// spell writes the pass's tokens to the innermost open text, if one is and
// the pass consumed any, as the library's `transduce-spell` writes them:
// the tokens' text joined, then written to the innermost, each join held
// to the bound.
func (t *TransduceTree) spell(pass *RulePass) *Fail {
	if len(t.texts) == 0 || len(pass.Tokens) == 0 {
		return nil
	}
	more := 0
	for _, token := range pass.Tokens {
		more += len(token.Src)
	}
	if max := t.max(); more > max {
		return shared.TreeTextRefusal(more, max)
	}
	text := t.texts[len(t.texts)-1]
	if f := t.fits(text, more); f != nil {
		return f
	}
	for _, token := range pass.Tokens {
		text.WriteString(token.Src)
	}
	return nil
}

// fits refuses, as the library's `transduce-write` would, a join of more
// bytes to text past the bound.
func (t *TransduceTree) fits(text *strings.Builder, more int) *Fail {
	if max := t.max(); text.Len()+more > max {
		return shared.TreeTextRefusal(text.Len()+more, max)
	}
	return nil
}

func (t *TransduceTree) openPass(pass *RulePass) (Flow, *Fail) {
	rule, ok := t.spec.Rule(pass.Name)
	if !ok {
		return Continue, t.spell(pass)
	}
	key, hasKey, f := named(rule.Key, pass)
	if f != nil {
		return Continue, f
	}
	var then Event
	hasThen, begins := false, false
	switch rule.Container {
	case TreeObject, TreeArray:
		if !t.carry {
			begins = true
			then, hasThen = EvArrayStart(), true
			if rule.Container == TreeObject {
				then = EvObjectStart()
			}
		}
	default:
		if rule.Scalar.Set && !rule.Scalar.Value && rule.Scalar.Token < len(pass.Tokens) {
			ev, f := scalar(pass.Tokens[rule.Scalar.Token].Val)
			if f != nil {
				return Continue, f
			}
			then, hasThen = ev, true
		}
	}
	if f := t.spell(pass); f != nil {
		return Continue, f
	}
	member, hasMember, f := named(rule.Member, pass)
	if f != nil {
		return Continue, f
	}
	if rule.Container != TreeNoContainer {
		if begins {
			t.open++
		} else {
			t.carry = false
		}
	}
	if rule.Text {
		t.texts = append(t.texts, &strings.Builder{})
	}
	if hasKey {
		if flow, f := t.down.Event(EvKey(key)); f != nil || flow == Stop {
			return flow, f
		}
	}
	if hasThen {
		if flow, f := t.down.Event(then); f != nil || flow == Stop {
			return flow, f
		}
	}
	if hasMember {
		return t.down.Event(EvKey(member))
	}
	return Continue, nil
}

func (t *TransduceTree) closePass(pass *RulePass) (Flow, *Fail) {
	rule, ok := t.spec.Rule(pass.Name)
	if !ok {
		return Continue, t.spell(pass)
	}
	// A :text rule's text closes first, and joins the one around it before
	// this pass's own tokens do; it is yielded before anything else, once
	// the rest of the pass is known not to fail.
	text, hasText := "", false
	if rule.Text {
		if len(t.texts) == 0 {
			return Continue, typeError("top: the vector is empty; there is no last item")
		}
		text, hasText = t.texts[len(t.texts)-1].String(), true
		t.texts = t.texts[:len(t.texts)-1]
		if len(t.texts) > 0 {
			around := t.texts[len(t.texts)-1]
			if f := t.fits(around, len(text)); f != nil {
				return Continue, f
			}
			around.WriteString(text)
		}
	}
	if f := t.spell(pass); f != nil {
		return Continue, f
	}
	var then Event
	hasThen := false
	switch {
	case rule.Container == TreeNoContainer:
		if rule.Scalar.Set && rule.Scalar.Value && pass.Value != nil {
			ev, f := scalar(*pass.Value)
			if f != nil {
				return Continue, f
			}
			then, hasThen = ev, true
		}
	case pass.Alt != nil && t.spec.IsContainer(pass.Alt.R):
		t.carry = true
	default:
		if t.open == 0 {
			return Continue, typeError("pop: the vector is empty; there is no last item to remove")
		}
		t.open--
		t.carry = false
		then, hasThen = EvArrayEnd(), true
		if rule.Container == TreeObject {
			then = EvObjectEnd()
		}
	}
	if hasText {
		if flow, f := t.down.Event(EvString(text)); f != nil || flow == Stop {
			return flow, f
		}
	}
	if !hasThen {
		return Continue, nil
	}
	return t.down.Event(then)
}

// named is the member name a field of a rule's entry names for a pass: the
// string, or the :val of the pass's token at that index, nothing when the
// pass consumed no such token; the library's `key` refuses a name that is
// not a string.
func named(field TreeKey, pass *RulePass) (string, bool, *Fail) {
	if !field.Set {
		return "", false, nil
	}
	if !field.IsToken {
		return field.Name, true, nil
	}
	if field.Token >= len(pass.Tokens) {
		return "", false, nil
	}
	val := pass.Tokens[field.Token].Val
	if val.Kind != shared.DatumString {
		return "", false, typeError("key: the name must be a string, not " + kind(val))
	}
	return val.Text, true, nil
}

// scalar is a value as the library's `scalar` takes it, or its refusal.
func scalar(value Datum) (Event, *Fail) {
	switch value.Kind {
	case shared.DatumNull:
		return EvNull(), nil
	case shared.DatumBool:
		return EvBool(value.Bool), nil
	case shared.DatumNumber:
		if value.HasLexeme {
			return shared.EvNumberLexeme(value.Value, value.Lexeme), nil
		}
		return EvNumber(value.Value), nil
	case shared.DatumString:
		return EvString(value.Text), nil
	}
	return Event{}, typeError("scalar: the value must be null, a boolean, a number or a string, not " + kind(value))
}

// kind is what a value is, in the runtime's words.
func kind(value Datum) string {
	switch value.Kind {
	case shared.DatumNull:
		return "null"
	case shared.DatumBool:
		return "a boolean"
	case shared.DatumNumber:
		return "a number"
	case shared.DatumString:
		return "a string"
	case shared.DatumArray:
		return "a vector"
	}
	return "a record"
}

// typeError is the runtime's type error: DSL_TYPE_ERROR, the
// type_mismatch word first.
func typeError(message string) *Fail {
	return NewFail(CodeDSLTypeError, "type_mismatch: "+message)
}
