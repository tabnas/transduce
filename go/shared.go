// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

// shared.go: the protocol types, under this package's names.
//
// The protocols this package produces and consumes (JsonEvents/1 and its
// Sink, TableRows/1, selectors and paths, retained values, limits,
// metrics and the abort flag, the failure codes, captures, and the
// scan-emit step's result) are declared in alchemy's shared package,
// github.com/tabnas/alchemy/go/shared, which this package, render and
// alchemy all build on. Each is declared here again under its own name:
// a type as an alias, a constant or a variable as itself, a function as a
// call. So this package's API is the one it had, and a value of any of
// these types is the same value in every package that names it.

import (
	"strings"

	"github.com/tabnas/alchemy/go/shared"
)

// JsonEvents/1 (shared/event.go).
type (
	EventKind = shared.EventKind
	Event     = shared.Event
)

// The JsonEvents/1 event kinds.
const (
	ObjectStart = shared.ObjectStart
	ObjectEnd   = shared.ObjectEnd
	ArrayStart  = shared.ArrayStart
	ArrayEnd    = shared.ArrayEnd
	Key         = shared.Key
	Null        = shared.Null
	Bool        = shared.Bool
	Number      = shared.Number
	String      = shared.String
	End         = shared.End
)

// Event constructors.

func EvObjectStart() Event     { return shared.EvObjectStart() }
func EvObjectEnd() Event       { return shared.EvObjectEnd() }
func EvArrayStart() Event      { return shared.EvArrayStart() }
func EvArrayEnd() Event        { return shared.EvArrayEnd() }
func EvKey(name string) Event  { return shared.EvKey(name) }
func EvNull() Event            { return shared.EvNull() }
func EvBool(b bool) Event      { return shared.EvBool(b) }
func EvNumber(v float64) Event { return shared.EvNumber(v) }
func EvString(s string) Event  { return shared.EvString(s) }
func EvEnd() Event             { return shared.EvEnd() }

// EvNumberLexeme is a number with the source text it was read from.
func EvNumberLexeme(v float64, lexeme string) Event { return shared.EvNumberLexeme(v, lexeme) }

// The push boundary, its recorders and adapters (shared/sink.go).
type (
	Flow         = shared.Flow
	Sink         = shared.Sink
	Recorder     = shared.Recorder
	FnSink       = shared.FnSink
	CountSink    = shared.CountSink
	TreeContract = shared.TreeContract
)

// What a stage wants next.
const (
	Continue = shared.Continue
	Stop     = shared.Stop
)

// Replay feeds a recording back into a sink, stopping where the sink
// stops.
func Replay(events []Event, sink Sink) (Flow, *Fail) { return shared.Replay(events, sink) }

// NewTreeContract wraps inner.
func NewTreeContract(inner Sink) *TreeContract { return shared.NewTreeContract(inner) }

// TableRows/1, bindings and the standard column mapping (shared/table.go).
type (
	CellKind       = shared.CellKind
	Cell           = shared.Cell
	PublicColumn   = shared.PublicColumn
	TableEventKind = shared.TableEventKind
	TableEvent     = shared.TableEvent
	TableSink      = shared.TableSink
	Table          = shared.Table
	MissingPolicy  = shared.MissingPolicy
	BoundColumn    = shared.BoundColumn
	ColumnMapper   = shared.ColumnMapper
	SchemaKind     = shared.SchemaKind
	Schema         = shared.Schema
	TableBinding   = shared.TableBinding
)

// The cell kinds, the table event kinds, the missing policies and the
// schema kinds.
const (
	CellNull           = shared.CellNull
	CellBool           = shared.CellBool
	CellNumber         = shared.CellNumber
	CellString         = shared.CellString
	CellMissing        = shared.CellMissing
	TableSchema        = shared.TableSchema
	TableRow           = shared.TableRow
	TableEnd           = shared.TableEnd
	MissingCell        = shared.MissingCell
	MissingNull        = shared.MissingNull
	MissingError       = shared.MissingError
	SchemaStatic       = shared.SchemaStatic
	SchemaFromMetadata = shared.SchemaFromMetadata
	SchemaInfer        = shared.SchemaInfer
)

// CellFromDatum is the cell for a retained value.
func CellFromDatum(d *Datum) Cell { return shared.CellFromDatum(d) }

// NewBoundColumn is a column with the default missing policy.
func NewBoundColumn(label string, source []Segment) BoundColumn {
	return shared.NewBoundColumn(label, source)
}

// StaticSchema is a schema the caller declares.
func StaticSchema(columns ...BoundColumn) Schema { return shared.StaticSchema(columns...) }

// MetadataSchema is a schema read from the descriptors the selector names,
// each mapped by column.
func MetadataSchema(columns Selector, column ColumnMapper) Schema {
	return shared.MetadataSchema(columns, column)
}

// InferSchema is a schema taken from the first row, by its kind: an
// object's member names, an array's positions ("0", "1", ...) or, for a
// scalar, the one column "value".
func InferSchema() Schema { return shared.InferSchema() }

// ColumnFromMeta is the standard mapping from a metadata descriptor to a
// column, the spec's `column-from-meta`.
func ColumnFromMeta(meta *Datum) (BoundColumn, *Fail) { return shared.ColumnFromMeta(meta) }

// The stable failure codes and the failure (shared/error.go).
type (
	Code  = shared.Code
	Limit = shared.Limit
	Fail  = shared.Fail
)

// The stable failure codes, in the Rust crate's declaration order.
const (
	CodeDSLParseError              = shared.CodeDSLParseError
	CodeDSLTypeError               = shared.CodeDSLTypeError
	CodeStreamReused               = shared.CodeStreamReused
	CodeStreamabilityUnknown       = shared.CodeStreamabilityUnknown
	CodeInputOrderViolation        = shared.CodeInputOrderViolation
	CodeCaptureOverlapUnsupported  = shared.CodeCaptureOverlapUnsupported
	CodeMissingValue               = shared.CodeMissingValue
	CodeDuplicateMember            = shared.CodeDuplicateMember
	CodeInvalidNumber              = shared.CodeInvalidNumber
	CodeProtocolOrderError         = shared.CodeProtocolOrderError
	CodeTargetValueUnrepresentable = shared.CodeTargetValueUnrepresentable
	CodeResourceLimitExceeded      = shared.CodeResourceLimitExceeded
	CodeInputInvalid               = shared.CodeInputInvalid
	CodeOutputFailed               = shared.CodeOutputFailed
	CodeAborted                    = shared.CodeAborted
)

// AllCodes is every code, in declaration order (Rust's Code::ALL).
var AllCodes = shared.AllCodes

// ParseCode finds a code by its written form.
func ParseCode(text string) (Code, bool) { return shared.ParseCode(text) }

// NewFail is a failure with a code and a message.
func NewFail(code Code, message string) *Fail { return shared.NewFail(code, message) }

// LimitFail is a limit failure, named after the Limits field that was
// passed.
func LimitFail(name string, value uint64, message string) *Fail {
	return shared.LimitFail(name, value, message)
}

// ProtocolFail is a PROTOCOL_ORDER_ERROR.
func ProtocolFail(message string) *Fail { return shared.ProtocolFail(message) }

// InputFail is an INPUT_INVALID.
func InputFail(message string) *Fail { return shared.InputFail(message) }

// OutputFail is an OUTPUT_FAILED.
func OutputFail(message string) *Fail { return shared.OutputFail(message) }

// AbortedFail is the failure for a cancelled run.
func AbortedFail() *Fail { return shared.AbortedFail() }

// Limits, metrics and the abort flag (shared/limits.go).
type (
	Limits    = shared.Limits
	Metrics   = shared.Metrics
	AbortFlag = shared.AbortFlag
)

// NodeBytes is the fixed allowance counted for every retained node.
const NodeBytes = shared.NodeBytes

// DefaultLimits is 256 levels, 64 KiB keys, 16 MiB scalars and metadata,
// 10 000 columns, 64 MiB records and captures, and no output limit.
func DefaultLimits() Limits { return shared.DefaultLimits() }

// UnlimitedLimits is no limit on anything that can be unlimited, and the
// largest values otherwise.
func UnlimitedLimits() Limits { return shared.UnlimitedLimits() }

// NewMetrics is a fresh set of metrics.
func NewMetrics() *Metrics { return shared.NewMetrics() }

// NewAbortFlag is a flag not yet set.
func NewAbortFlag() *AbortFlag { return shared.NewAbortFlag() }

// Selectors and paths (shared/selector.go).
type (
	Segment  = shared.Segment
	Path     = shared.Path
	StepKind = shared.StepKind
	Step     = shared.Step
	Selector = shared.Selector
)

// The selector step kinds.
const (
	StepProperty   = shared.StepProperty
	StepIndex      = shared.StepIndex
	StepEachIndex  = shared.StepEachIndex
	StepEachMember = shared.StepEachMember
)

// KeySegment is a member step.
func KeySegment(key string) Segment { return shared.KeySegment(key) }

// IndexSegment is an element step.
func IndexSegment(i int) Segment { return shared.IndexSegment(i) }

// Root is the document itself.
func Root() Selector { return shared.Root() }

// SelectorOf is a selector of the given steps.
func SelectorOf(steps ...Step) Selector { return shared.SelectorOf(steps...) }

// FromSegments is a selector naming exactly one location.
func FromSegments(segments []Segment) Selector { return shared.FromSegments(segments) }

// The retained value, its builder and the JSON writer (shared/datum.go).
type (
	DatumKind    = shared.DatumKind
	Member       = shared.Member
	Datum        = shared.Datum
	Duplicates   = shared.Duplicates
	DatumBuilder = shared.DatumBuilder
)

// The kinds of Datum, and the policies for a repeated member name.
const (
	DatumNull   = shared.DatumNull
	DatumBool   = shared.DatumBool
	DatumNumber = shared.DatumNumber
	DatumString = shared.DatumString
	DatumArray  = shared.DatumArray
	DatumObject = shared.DatumObject
	Reject      = shared.Reject
	LastWins    = shared.LastWins
	FirstWins   = shared.FirstWins
)

// Datum constructors.

func NullDatum() Datum            { return shared.NullDatum() }
func BoolDatum(b bool) Datum      { return shared.BoolDatum(b) }
func NumberDatum(v float64) Datum { return shared.NumberDatum(v) }
func StringDatum(s string) Datum  { return shared.StringDatum(s) }

// NumberDatumLexeme is a number with the source text it was read from.
func NumberDatumLexeme(v float64, lexeme string) Datum { return shared.NumberDatumLexeme(v, lexeme) }

// ArrayDatum is an array of items.
func ArrayDatum(items ...Datum) Datum { return shared.ArrayDatum(items...) }

// ObjectDatum is an object of members, in the order given.
func ObjectDatum(members ...Member) Datum { return shared.ObjectDatum(members...) }

// DatumFromJSON reads one JSON text into a datum, keeping member order and
// each number's text as its lexeme.
func DatumFromJSON(text string) (Datum, error) { return shared.DatumFromJSON(text) }

// WriteJSONString appends s as an RFC 8259 JSON string literal.
func WriteJSONString(s string, out *strings.Builder) { shared.WriteJSONString(s, out) }

// WriteJSONNumber appends a number: its lexeme when it has one, else the
// shortest text that reads back as the same float64.
func WriteJSONNumber(value float64, lexeme string, hasLexeme bool, out *strings.Builder) {
	shared.WriteJSONNumber(value, lexeme, hasLexeme, out)
}

// WriteJSON appends a datum as compact JSON.
func WriteJSON(d *Datum, out *strings.Builder) { shared.WriteJSON(d, out) }

// WalkDatum emits a datum as JsonEvents/1, without the final End.
func WalkDatum(d *Datum, sink Sink) (Flow, *Fail) { return shared.WalkDatum(d, sink) }

// NewDatumBuilder is a builder whose limit failure names limitName.
func NewDatumBuilder(limit int, limitName string, duplicates Duplicates) *DatumBuilder {
	return shared.NewDatumBuilder(limit, limitName, duplicates)
}

// Captures and the route sink they are delivered to (shared/route.go).
type (
	CaptureMode   = shared.CaptureMode
	Budget        = shared.Budget
	CaptureSpec   = shared.CaptureSpec
	Selected      = shared.Selected
	RouteSink     = shared.RouteSink
	RouteBeginner = shared.RouteBeginner
)

// What a capture keeps of its match.
const (
	Materialize = shared.Materialize
	Observe     = shared.Observe
)

// MaterializeSpec is a Materialize capture.
func MaterializeSpec(tag string, selector Selector) CaptureSpec {
	return shared.MaterializeSpec(tag, selector)
}

// ObserveSpec is an Observe capture.
func ObserveSpec(tag string, selector Selector) CaptureSpec {
	return shared.ObserveSpec(tag, selector)
}

// Transition is the result of one scan-emit step (shared/scan.go).
type Transition[S, O any] = shared.Transition[S, O]

// CaptureID is which selector matched (shared/matcher.go).
type CaptureID = shared.CaptureID
