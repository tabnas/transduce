// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

// Retention does not grow with the number of rows (rs/tests/retention_test.rs).
//
// The table transducer holds one row at a time and the router one
// capture at a time, so the bytes retained at their peak depend on the
// largest row, never on how many rows there are. Ten times the rows of
// the same size must leave captured_bytes_high (and so
// retained_bytes_high) exactly where it was. With the adapter, the test
// also looks where pruning acts: the engine's tree after the run must
// hold no row for 200 rows and none for 2000, while the same run without
// pruning holds every row.

import (
	"fmt"
	"strings"
	"testing"
)

// sameRows is the worked example with rows copies of one record.
func sameRows(rows int) string {
	record := workedRecord(123_456)
	var b strings.Builder
	b.WriteString(`{"response":{"metadata":` + workedMetadata + `,"payload":{"deep":{"records":[`)
	for i := 0; i < rows; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(record)
	}
	b.WriteString("]}}}}")
	return b.String()
}

type left struct {
	capturedHigh, retainedHigh uint64
	rows, treeRows, treeBytes  int
}

func retentionRun(t *testing.T, rows int, mode SourceMode) left {
	t.Helper()
	m := NewMetrics()
	tbl := &Table{}
	tf, f := NewTableFromJSON(TableBinding{Schema: MetadataSchema(metadataSelector(), ColumnFromMeta), Rows: recordsSelector()},
		DefaultLimits(), Reject, m, tbl)
	if f != nil {
		t.Fatal(f)
	}
	_, f, value := NewParserSource(makeGrammar("json"), sameRows(rows)).Grammar("json").Mode(mode).Metrics(m).RunWithValue(tf)
	if f != nil || !tbl.Ended {
		t.Fatal(f)
	}
	tree := DatumFromValue(value)
	records, ok := tree.GetPath([]Segment{KeySegment("response"), KeySegment("payload"), KeySegment("deep"), KeySegment("records")})
	if !ok || records.Kind != DatumArray {
		t.Fatal("the records array is in the tree")
	}
	return left{m.CapturedBytesHigh.Load(), m.RetainedBytesHigh.Load(), len(tbl.Rows), len(records.Items), tree.ByteSize()}
}

func TestTenTimesTheRowsLeaveTheRetainedHighWaterFlat(t *testing.T) {
	modes := []SourceMode{MaterializeMode()}
	if AdapterBuilt() {
		modes = append(modes, IncrementalMode(PruneUnderSelector(recordsSelector())))
	}
	for _, mode := range modes {
		one := retentionRun(t, 200, mode)
		ten := retentionRun(t, 2000, mode)
		fmt.Printf("retention (mode %d): 200 rows, high-water %d bytes, tree %d bytes; 2000 rows, high-water %d bytes, tree %d bytes\n",
			mode.Kind, one.capturedHigh, one.treeBytes, ten.capturedHigh, ten.treeBytes)
		if one.rows != 200 || ten.rows != 2000 || one.capturedHigh == 0 {
			t.Fatal(one, ten)
		}
		if ten.capturedHigh != one.capturedHigh || ten.retainedHigh != one.retainedHigh {
			t.Fatalf("the peak is one row's, not the count's: %v %v", one, ten)
		}
		if mode.Kind == ModeIncremental {
			if one.treeRows != 0 || ten.treeRows != 0 {
				t.Fatalf("every streamed row was dropped from the engine's tree: %d %d", one.treeRows, ten.treeRows)
			}
			if ten.treeBytes != one.treeBytes {
				t.Fatalf("the tree left behind does not grow with the rows: %d %d", one.treeBytes, ten.treeBytes)
			}
		}
	}
}

// The control: without pruning the engine's tree holds every row, so the
// assertion above can fail if pruning stops.
func TestWithoutPruningTheEnginesTreeHoldsEveryRow(t *testing.T) {
	mode := MaterializeMode()
	if AdapterBuilt() {
		mode = IncrementalMode(Prune{})
	}
	one := retentionRun(t, 200, mode)
	ten := retentionRun(t, 2000, mode)
	if one.treeRows != 200 || ten.treeRows != 2000 || ten.treeBytes <= 9*one.treeBytes || ten.capturedHigh != one.capturedHigh {
		t.Fatal(one, ten)
	}
}

// sameRowsXML is the worked example as XML with rows copies of one record
// and nothing between them, so every row is one <record> element of the
// same size, and the records element's children are the rows alone.
func sameRowsXML(rows int) string {
	record := `<record id="123456"><person><name>Person number 123456</name></person>` +
		`<account><balance>864192.56</balance></account></record>`
	return "<?xml version=\"1.0\"?>\n<response><records>" + strings.Repeat(record, rows) + "</records></response>\n"
}

// xmlRowsSelector is the records element's children: the root's first
// child's children.
func xmlRowsSelector() Selector {
	return Root().Property("children").Index(0).Property("children").EachIndex()
}

func retentionRunXML(t *testing.T, rows int, mode SourceMode) left {
	t.Helper()
	m := NewMetrics()
	tbl := &Table{}
	tf, f := NewTableFromJSON(TableBinding{Schema: InferSchema(), Rows: xmlRowsSelector()},
		DefaultLimits(), Reject, m, tbl)
	if f != nil {
		t.Fatal(f)
	}
	_, f, value := NewParserSource(makeGrammar("xml"), sameRowsXML(rows)).Grammar("xml").Mode(mode).Metrics(m).RunWithValue(tf)
	if f != nil || !tbl.Ended {
		t.Fatal(f)
	}
	tree := DatumFromValue(value)
	records, ok := tree.GetPath([]Segment{KeySegment("children"), IndexSegment(0), KeySegment("children")})
	if !ok || records.Kind != DatumArray {
		t.Fatal("the records element's children are in the tree")
	}
	return left{m.CapturedBytesHigh.Load(), m.RetainedBytesHigh.Load(), len(tbl.Rows), len(records.Items), tree.ByteSize()}
}

// xml streams too: its rules build each element in document order, so the
// rows of an XML document reach the table one at a time, and with the
// records element's children pruned the engine's tree holds none of them
// after the run.
func TestTenTimesTheRowsOfAnXMLDocumentLeaveTheRetainedHighWaterFlat(t *testing.T) {
	modes := []SourceMode{MaterializeMode()}
	if AdapterBuilt() {
		modes = append(modes, IncrementalMode(PruneUnderSelector(xmlRowsSelector())))
	}
	for _, mode := range modes {
		one := retentionRunXML(t, 200, mode)
		ten := retentionRunXML(t, 2000, mode)
		fmt.Printf("retention xml (mode %d): 200 rows, high-water %d bytes, tree %d bytes; 2000 rows, high-water %d bytes, tree %d bytes\n",
			mode.Kind, one.capturedHigh, one.treeBytes, ten.capturedHigh, ten.treeBytes)
		if one.rows != 200 || ten.rows != 2000 || one.capturedHigh == 0 {
			t.Fatal(one, ten)
		}
		if ten.capturedHigh != one.capturedHigh || ten.retainedHigh != one.retainedHigh {
			t.Fatalf("the peak is one row's, not the count's: %v %v", one, ten)
		}
		if mode.Kind == ModeIncremental {
			if one.treeRows != 0 || ten.treeRows != 0 {
				t.Fatalf("every streamed row was dropped from the engine's tree: %d %d", one.treeRows, ten.treeRows)
			}
			if ten.treeBytes != one.treeBytes {
				t.Fatalf("the tree left behind does not grow with the rows: %d %d", one.treeBytes, ten.treeBytes)
			}
			// The control: without pruning the tree keeps every row.
			kept := retentionRunXML(t, 2000, IncrementalMode(Prune{}))
			if kept.treeRows != 2000 || kept.capturedHigh != one.capturedHigh {
				t.Fatal(kept, one)
			}
		}
	}
}

// A chain sharing one Metrics counts the source events once.
func TestAChainSharingOneMetricsCountsTheSourceEventsOnce(t *testing.T) {
	text := `{"meta":[{"title":"Id","path":["id"]}],"rows":[{"id":1},{"id":2}]}`
	rowsSel := Root().Property("rows").EachIndex()
	for _, mode := range sourceModes() {
		if mode.Kind == ModeIncremental {
			mode = IncrementalMode(PruneUnderSelector(rowsSel))
		}
		m := NewMetrics()
		tf, _ := NewTableFromJSON(TableBinding{Schema: MetadataSchema(Root().Property("meta"), ColumnFromMeta), Rows: rowsSel},
			DefaultLimits(), Reject, m, &Table{})
		if _, f := NewParserSource(makeGrammar("json"), text).Grammar("json").Mode(mode).Metrics(m).Run(tf); f != nil {
			t.Fatal(f)
		}
		var rec Recorder
		NewParserSource(makeGrammar("json"), text).Run(&rec)
		keys, scalars := 0, 0
		for _, ev := range rec.Events {
			if ev.Kind == Key {
				keys++
			}
			if ev.IsScalar() {
				scalars++
			}
		}
		if m.Events.Load() != uint64(len(rec.Events)) || m.Keys.Load() != uint64(keys) || m.Scalars.Load() != uint64(scalars) || m.Rows.Load() != 2 {
			t.Fatal(mode, m.Events.Load(), len(rec.Events))
		}
	}
}
