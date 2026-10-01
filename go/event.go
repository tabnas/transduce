// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

import (
	"strconv"
)

// EventKind names one kind of JsonEvents/1 event.
type EventKind uint8

// The JsonEvents/1 event kinds.
const (
	ObjectStart EventKind = iota
	ObjectEnd
	ArrayStart
	ArrayEnd
	// Key is the name of the member whose value follows, inside an object.
	Key
	Null
	Bool
	Number
	String
	// End: the document is complete. Exactly one, after the root value,
	// and only after the whole source has been validated.
	End
)

var eventKindNames = [...]string{
	"object_start", "object_end", "array_start", "array_end", "key",
	"null", "bool", "number", "string", "end",
}

// String is the kind's name as the shared fixtures spell it.
func (k EventKind) String() string {
	if int(k) < len(eventKindNames) {
		return eventKindNames[k]
	}
	return "event(" + strconv.Itoa(int(k)) + ")"
}

// Event is one event of JsonEvents/1.
//
// Text is a Key's name or a String's text. Value is a Number's value and
// Lexeme its source text when the source could hand it over (the
// rule-event adapter reads it off the token), else "": a JSON number is
// never empty, so the empty string means "no lexeme" and a renderer falls
// back to the shortest round-trip form of Value. Keeping both is how
// `50.25` stays `50.25` and a number beyond float64's exact range keeps
// its digits.
//
// Go strings are immutable, so one type serves as both the event a
// source hands a sink and the recording a recorder keeps.
type Event struct {
	Kind   EventKind
	Text   string
	Bool   bool
	Value  float64
	Lexeme string
}

// Constructors, for sources, recorders and tests.

func EvObjectStart() Event     { return Event{Kind: ObjectStart} }
func EvObjectEnd() Event       { return Event{Kind: ObjectEnd} }
func EvArrayStart() Event      { return Event{Kind: ArrayStart} }
func EvArrayEnd() Event        { return Event{Kind: ArrayEnd} }
func EvKey(name string) Event  { return Event{Kind: Key, Text: name} }
func EvNull() Event            { return Event{Kind: Null} }
func EvBool(b bool) Event      { return Event{Kind: Bool, Bool: b} }
func EvNumber(v float64) Event { return Event{Kind: Number, Value: v} }
func EvString(s string) Event  { return Event{Kind: String, Text: s} }
func EvEnd() Event             { return Event{Kind: End} }

// EvNumberLexeme is a number with the source text it was read from.
func EvNumberLexeme(v float64, lexeme string) Event {
	return Event{Kind: Number, Value: v, Lexeme: lexeme}
}

// IsStart reports whether the event opens a container.
func (e Event) IsStart() bool { return e.Kind == ObjectStart || e.Kind == ArrayStart }

// IsEnd reports whether the event closes a container.
func (e Event) IsEnd() bool { return e.Kind == ObjectEnd || e.Kind == ArrayEnd }

// IsScalar reports whether the event is a whole scalar value.
func (e Event) IsScalar() bool {
	switch e.Kind {
	case Null, Bool, Number, String:
		return true
	}
	return false
}

// WithoutLexeme is the event with any number lexeme dropped.
func (e Event) WithoutLexeme() Event {
	e.Lexeme = ""
	return e
}

// String is a short human form, as the Rust recording's Display has it.
func (e Event) String() string {
	switch e.Kind {
	case ObjectStart:
		return "{"
	case ObjectEnd:
		return "}"
	case ArrayStart:
		return "["
	case ArrayEnd:
		return "]"
	case Key:
		return "key " + strconv.Quote(e.Text)
	case Null:
		return "null"
	case Bool:
		return strconv.FormatBool(e.Bool)
	case Number:
		if e.Lexeme != "" {
			return e.Lexeme
		}
		return formatFloat(e.Value)
	case String:
		return strconv.Quote(e.Text)
	case End:
		return "end"
	}
	return e.Kind.String()
}

// formatFloat writes a float64 the way Rust's f64 Display does: the
// shortest digits that read back as the same value, never an exponent.
func formatFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// isJSONNumber reports whether s is a number by RFC 8259's grammar:
// -?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?. Only such a lexeme is
// kept, so a renderer can write it as it stands.
func isJSONNumber(s string) bool {
	i, n := 0, len(s)
	digits := func() int {
		start := i
		for i < n && s[i] >= '0' && s[i] <= '9' {
			i++
		}
		return i - start
	}
	if i < n && s[i] == '-' {
		i++
	}
	switch {
	case i < n && s[i] == '0':
		i++
	case i < n && s[i] >= '1' && s[i] <= '9':
		digits()
	default:
		return false
	}
	if i < n && s[i] == '.' {
		i++
		if digits() == 0 {
			return false
		}
	}
	if i < n && (s[i] == 'e' || s[i] == 'E') {
		i++
		if i < n && (s[i] == '+' || s[i] == '-') {
			i++
		}
		if digits() == 0 {
			return false
		}
	}
	return i == n
}
