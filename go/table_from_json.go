// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

import (
	"fmt"
	"strconv"
	"strings"
)

// tableCore is the route sink behind the transducer: it owns the schema
// state, the projection buffer and the table sink.
type tableCore struct {
	sink  TableSink
	rowID CaptureID
	// metaID is the metadata capture's id, -1 when there is none.
	metaID int
	schema Schema
	// bound is the columns once known; boundSet says they are.
	bound      []BoundColumn
	boundSet   bool
	public     []PublicColumn
	schemaSent bool
	cells      []Cell
	// disjoint: no column's path is another's prefix, so each cell can
	// be moved out of the row instead of copied.
	disjoint         bool
	maxColumns       int
	maxMetadataBytes int
	metrics          *Metrics
}

func (c *tableCore) bind(columns []BoundColumn, from string) *Fail {
	if len(columns) > c.maxColumns {
		return LimitFail("max_columns", uint64(c.maxColumns),
			fmt.Sprintf("%s declares %d columns, more than %d", from, len(columns), c.maxColumns))
	}
	c.public = make([]PublicColumn, len(columns))
	for i := range columns {
		c.public[i] = columns[i].Public()
	}
	c.disjoint = pathsAreDisjoint(columns)
	c.bound = columns
	c.boundSet = true
	return nil
}

func (c *tableCore) sendSchema() (Flow, *Fail) {
	c.schemaSent = true
	return c.sink.TableEvent(TableEvent{Kind: TableSchema, Columns: c.public})
}

func (c *tableCore) metadata(s Selected) *Fail {
	if c.schema.Kind != SchemaFromMetadata {
		return ProtocolFail("metadata was delivered to a static schema")
	}
	if c.boundSet {
		return NewFail(CodeInputOrderViolation, fmt.Sprintf(
			"the column metadata at %s was selected twice; a table has one schema", s.Path)).AtPath(s.Path.String())
	}
	value := NullDatum()
	if s.Value != nil {
		value = *s.Value
	}
	if value.Kind != DatumArray {
		return InputFail(fmt.Sprintf("the column metadata at %s is not an array", s.Path)).AtPath(s.Path.String())
	}
	if len(value.Items) > c.maxColumns {
		return LimitFail("max_columns", uint64(c.maxColumns), fmt.Sprintf(
			"the metadata at %s declares %d columns, more than %d", s.Path, len(value.Items), c.maxColumns)).
			AtPath(s.Path.String())
	}
	columns := make([]BoundColumn, 0, len(value.Items))
	for i := range value.Items {
		col, f := c.schema.Column(&value.Items[i])
		if f != nil {
			if f.Path == "" {
				f.Path = append(s.Path.Clone(), IndexSegment(i)).String()
			}
			return f
		}
		columns = append(columns, col)
	}
	return c.bind(columns, "the metadata")
}

func (c *tableCore) row(s Selected) (Flow, *Fail) {
	row := NullDatum()
	if s.Value != nil {
		row = *s.Value
	}
	if !c.schemaSent {
		if c.schema.Kind == SchemaInfer && !c.boundSet {
			columns, f := inferColumns(&row, c.maxColumns, c.maxMetadataBytes)
			if f != nil {
				return Continue, f.AtPath(s.Path.String())
			}
			if f := c.bind(columns, "the first row"); f != nil {
				return Continue, f
			}
		}
		flow, f := c.sendSchema()
		if f != nil || flow == Stop {
			return flow, f
		}
	}
	if !c.boundSet {
		return Continue, ProtocolFail("a row was projected before its schema was bound")
	}
	c.cells = c.cells[:0]
	for _, col := range c.bound {
		var cell Cell
		found := false
		if c.disjoint {
			if d, ok := row.TakePath(col.Source); ok {
				cell, found = CellFromDatum(&d), true
			}
		} else if d, ok := row.GetPath(col.Source); ok {
			cell, found = CellFromDatum(d), true
		}
		if !found {
			switch col.Missing {
			case MissingNull:
				cell = Cell{Kind: CellNull}
			case MissingError:
				at := append(s.Path.Clone(), col.Source...)
				return Continue, NewFail(CodeMissingValue, fmt.Sprintf(
					"column %q has no value at %s", col.Label, at)).AtPath(at.String())
			default:
				cell = Cell{Kind: CellMissing}
			}
		}
		c.cells = append(c.cells, cell)
	}
	c.metrics.Rows.Add(1)
	return c.sink.TableEvent(TableEvent{Kind: TableRow, Cells: c.cells})
}

// Began refuses a row that begins before the metadata completed.
func (c *tableCore) Began(id CaptureID, _ string) *Fail {
	if id == c.rowID && c.schema.Kind == SchemaFromMetadata && !c.boundSet {
		return NewFail(CodeInputOrderViolation, fmt.Sprintf(
			"a row began before the column metadata at %s had completed; rows must follow their metadata",
			c.schema.Columns))
	}
	return nil
}

// Selected takes a completed metadata or row capture.
func (c *tableCore) Selected(s Selected) (Flow, *Fail) {
	if s.ID == c.metaID {
		return Continue, c.metadata(s)
	}
	return c.row(s)
}

// End completes the table.
func (c *tableCore) End() (Flow, *Fail) {
	if !c.schemaSent {
		switch {
		case c.schema.Kind == SchemaFromMetadata && !c.boundSet:
			return Continue, InputFail(fmt.Sprintf(
				"the document has no column metadata at %s", c.schema.Columns)).AtPath(c.schema.Columns.String())
		case c.schema.Kind == SchemaInfer && !c.boundSet:
			// No rows: nothing to infer from, so the table is empty.
			if f := c.bind(nil, "the binding"); f != nil {
				return Continue, f
			}
		}
		flow, f := c.sendSchema()
		if f != nil || flow == Stop {
			return flow, f
		}
	}
	return c.sink.TableEvent(TableEvent{Kind: TableEnd})
}

// TableFromJSON is the metadata-first table transducer: a Sink for one
// document's JsonEvents/1, with TableRows/1 going to the wrapped
// TableSink as the rows arrive. It is built on a Router with at most two
// captures: the column metadata, when the schema comes from the
// document, and the rows. The schema must be known before the first row
// is emitted, and the transducer holds one row at a time: metadata that
// has not completed when a row BEGINS is INPUT_ORDER_VIOLATION, raised at
// the row's start before a byte of the row is retained; metadata that
// arrives twice is the same failure; a document with no rows is a valid
// empty table. Rows are projected into schema order by path, so the
// order of members inside a row never matters, and a number keeps the
// lexeme the source events carried. An inferred schema is the first row's,
// by its kind: an object's member names, an array's positions, or the one
// column "value" of a scalar; a later row of another kind projects through
// those paths and lands empty where they miss.
type TableFromJSON struct {
	router *Router
	core   *tableCore
}

// NewTableFromJSON builds the transducer. Rows are materialized under
// max_record_bytes, metadata under max_metadata_bytes; a rows selector
// that may overlap the metadata selector is refused as the router
// refuses any overlapping materializations.
func NewTableFromJSON(binding TableBinding, limits Limits, duplicates Duplicates, metrics *Metrics, sink TableSink) (*TableFromJSON, *Fail) {
	if metrics == nil {
		metrics = NewMetrics()
	}
	rowSpec := MaterializeSpec("row", binding.Rows).WithBudget(limits.MaxRecordBytes, "max_record_bytes")
	specs := []CaptureSpec{rowSpec}
	metaID := -1
	if binding.Schema.Kind == SchemaFromMetadata {
		metaSpec := MaterializeSpec("metadata", binding.Schema.Columns).
			WithBudget(limits.MaxMetadataBytes, "max_metadata_bytes")
		specs = []CaptureSpec{metaSpec, rowSpec}
		metaID = 0
	}
	core := &tableCore{
		sink:             sink,
		rowID:            len(specs) - 1,
		metaID:           metaID,
		schema:           binding.Schema,
		maxColumns:       limits.MaxColumns,
		maxMetadataBytes: limits.MaxMetadataBytes,
		metrics:          metrics,
	}
	if binding.Schema.Kind == SchemaStatic {
		if f := core.bind(append([]BoundColumn(nil), binding.Schema.Static...), "the binding"); f != nil {
			return nil, f
		}
	}
	router, f := NewRouter(specs, limits, duplicates, metrics, core)
	if f != nil {
		return nil, f
	}
	return &TableFromJSON{router: router, core: core}, nil
}

// Sink is the table sink.
func (t *TableFromJSON) Sink() TableSink { return t.core.sink }

// Ended reports whether the table's End has been emitted.
func (t *TableFromJSON) Ended() bool { return t.router.Ended() }

// Event takes one source event.
func (t *TableFromJSON) Event(ev Event) (Flow, *Fail) { return t.router.Event(ev) }

// inferColumns is the columns the first row implies, by its kind: an
// object's member names, in its order, each sourced at its key; an
// array's positions, labelled "0", "1", ... up to its length, each
// sourced at its index; and for a scalar one column, "value", sourced at
// the row itself. A later row of any kind projects through these paths,
// and lands empty where they miss.
func inferColumns(row *Datum, maxColumns, maxMetadataBytes int) ([]BoundColumn, *Fail) {
	// The bounds are checked before a column is built: the labels are the
	// table's metadata for as long as it lasts, so they are held to the
	// bound a metadata capture is, measured as the array of their strings
	// would be, and counted from the row itself, so a first row wider than
	// either bound costs a walk of what it already holds and allocates
	// nothing.
	count, labels := 1, len("value")
	switch row.Kind {
	case DatumObject:
		count, labels = len(row.Members), 0
		for _, m := range row.Members {
			labels += len(m.Key)
		}
	case DatumArray:
		count, labels = len(row.Items), 0
		for i := 0; i < count; i++ {
			labels += decimalDigits(i)
		}
	}
	bytes := (count+1)*NodeBytes + labels
	if bytes > maxMetadataBytes {
		return nil, LimitFail("max_metadata_bytes", uint64(maxMetadataBytes), fmt.Sprintf(
			"the first row's %d column labels take %d bytes as the table's metadata, more than %d",
			count, bytes, maxMetadataBytes))
	}
	if count > maxColumns {
		return nil, LimitFail("max_columns", uint64(maxColumns), fmt.Sprintf(
			"the first row declares %d columns, more than %d", count, maxColumns))
	}
	return buildInferredColumns(row), nil
}

// decimalDigits is the length of i written in decimal: the bytes of an
// array row's label.
func decimalDigits(i int) int {
	digits := 1
	for n := i; n >= 10; n /= 10 {
		digits++
	}
	return digits
}

// buildInferredColumns is the columns inferColumns has bounded.
func buildInferredColumns(row *Datum) []BoundColumn {
	switch row.Kind {
	case DatumObject:
		columns := make([]BoundColumn, len(row.Members))
		for i, m := range row.Members {
			columns[i] = NewBoundColumn(m.Key, []Segment{KeySegment(m.Key)})
		}
		return columns
	case DatumArray:
		columns := make([]BoundColumn, len(row.Items))
		for i := range row.Items {
			columns[i] = NewBoundColumn(strconv.Itoa(i), []Segment{IndexSegment(i)})
		}
		return columns
	default:
		return []BoundColumn{NewBoundColumn("value", nil)}
	}
}

// pathsAreDisjoint reports whether every column's path can be moved out
// of a row without robbing another column: no path is another's prefix,
// and none repeats.
func pathsAreDisjoint(columns []BoundColumn) bool {
	seen := make(map[string]struct{}, len(columns))
	for _, c := range columns {
		k := pathKey(c.Source)
		if _, dup := seen[k]; dup {
			return false
		}
		seen[k] = struct{}{}
	}
	for _, c := range columns {
		for n := 0; n < len(c.Source); n++ {
			if _, ok := seen[pathKey(c.Source[:n])]; ok {
				return false
			}
		}
	}
	return true
}

// pathKey encodes a path so that two paths have the same key exactly
// when they are equal.
func pathKey(path []Segment) string {
	var b strings.Builder
	for _, seg := range path {
		if seg.IsIndex {
			b.WriteByte('i')
			b.WriteString(strconv.Itoa(seg.Index))
		} else {
			b.WriteByte('k')
			b.WriteString(strconv.Itoa(len(seg.Key)))
			b.WriteByte(':')
			b.WriteString(seg.Key)
		}
		b.WriteByte(';')
	}
	return b.String()
}
