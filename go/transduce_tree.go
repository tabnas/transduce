// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

import "github.com/tabnas/alchemy/go/shared"

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
// scalar from one of its tokens; a close pass ends its container, or
// yields its own value; and a container whose rule is replaced by a rule
// the spec names a container is one container, so that close ends nothing
// and the replacing open begins nothing. The state is the count of open
// containers and whether a close handed its container on, so the native
// retains the document's nesting and nothing of its width or length, as
// the library's step does. alchemy-cli's differential test holds the two
// to the same bytes.
//
// A pass's events are worked out before the first of them is written, as
// the library's step returns them together: a pass that fails writes
// none. The failures are the library's, with its codes and texts.
type TransduceTree struct {
	spec  TreeSpec
	open  int
	carry bool
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

func (t *TransduceTree) openPass(pass *RulePass) (Flow, *Fail) {
	rule, ok := t.spec.Rule(pass.Name)
	if !ok {
		return Continue, nil
	}
	key, hasKey := "", false
	if rule.Key.Set {
		if !rule.Key.IsToken {
			key, hasKey = rule.Key.Name, true
		} else if rule.Key.Token < len(pass.Tokens) {
			val := pass.Tokens[rule.Key.Token].Val
			if val.Kind != shared.DatumString {
				return Continue, typeError("key: the name must be a string, not " + kind(val))
			}
			key, hasKey = val.Text, true
		}
	}
	var then Event
	hasThen := false
	switch rule.Container {
	case TreeObject, TreeArray:
		if t.carry {
			t.carry = false
		} else {
			t.open++
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
	if hasKey {
		if flow, f := t.down.Event(EvKey(key)); f != nil || flow == Stop {
			return flow, f
		}
	}
	if !hasThen {
		return Continue, nil
	}
	return t.down.Event(then)
}

func (t *TransduceTree) closePass(pass *RulePass) (Flow, *Fail) {
	rule, ok := t.spec.Rule(pass.Name)
	if !ok {
		return Continue, nil
	}
	if rule.Container == TreeNoContainer {
		if rule.Scalar.Set && rule.Scalar.Value && pass.Value != nil {
			ev, f := scalar(*pass.Value)
			if f != nil {
				return Continue, f
			}
			return t.down.Event(ev)
		}
		return Continue, nil
	}
	if pass.Alt != nil && t.spec.IsContainer(pass.Alt.R) {
		t.carry = true
		return Continue, nil
	}
	if t.open == 0 {
		return Continue, typeError("pop: the vector is empty; there is no last item to remove")
	}
	t.open--
	t.carry = false
	if rule.Container == TreeObject {
		return t.down.Event(EvObjectEnd())
	}
	return t.down.Event(EvArrayEnd())
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
