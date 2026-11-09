// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

// Retention does not grow with the number of rows (rs/tests/retention_test.rs).
//
// The table transducer holds one row at a time and the router one
// capture at a time, so the bytes retained at their peak depend on the
// largest row, never on how many rows there are. Ten times the rows of
// the same size must leave captured_bytes_high (and so
// retained_bytes_high) exactly where it was.
//
// The rows reach the table from the parse of a JSON document, its value
// walked (ParserSource), through the source's guard, which counts the
// events as a source's guard counts them. The transducer's peak does not
// depend on how the events were made: a host that streams a format
// through its transducer part (alchemy's text over ParserEventSource's
// passes, pruned) hands the same events to the same stages. What pruning
// leaves of the engine's tree is prune_test.go's to hold flat, for every
// grammar; dmog, which runs the parts, holds a part's run to it.

import (
	"fmt"
	"strings"
	"testing"
)

// sameRows is the worked example with rows copies of one record, so
// every row has the same size and the peak is the row's, not the count's.
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
	rows                       int
}

// retentionRun is the table over the document's events, the parsed value
// walked; the metrics count the events once, at the source's guard in
// front of the table.
func retentionRun(t *testing.T, rows int) left {
	t.Helper()
	m := NewMetrics()
	tbl := &Table{}
	tf, f := NewTableFromJSON(TableBinding{Schema: MetadataSchema(metadataSelector(), ColumnFromMeta), Rows: recordsSelector()},
		DefaultLimits(), Reject, m, tbl)
	if f != nil {
		t.Fatal(f)
	}
	if _, f := NewParserSource(makeGrammar("json"), sameRows(rows)).Metrics(m).Run(tf); f != nil || !tbl.Ended {
		t.Fatal(f)
	}
	return left{m.CapturedBytesHigh.Load(), m.RetainedBytesHigh.Load(), len(tbl.Rows)}
}

func TestTenTimesTheRowsLeaveTheRetainedHighWaterFlat(t *testing.T) {
	one := retentionRun(t, 200)
	fmt.Printf("retention: 200 rows, captured high-water %d bytes\n", one.capturedHigh)
	ten := retentionRun(t, 2000)
	fmt.Printf("retention: 2000 rows, captured high-water %d bytes\n", ten.capturedHigh)
	if one.rows != 200 || ten.rows != 2000 || one.capturedHigh == 0 {
		t.Fatal(one, ten)
	}
	if ten.capturedHigh != one.capturedHigh || ten.retainedHigh != one.retainedHigh {
		t.Fatalf("the peak is one row's, not the count's: %v %v", one, ten)
	}
}

// A chain sharing one Metrics counts the source events once: the
// README's chain shares one Metrics between the source and the table
// transducer, and the source counts are the source's.
func TestAChainSharingOneMetricsCountsTheSourceEventsOnce(t *testing.T) {
	text := `{"meta":[{"title":"Id","path":["id"]}],"rows":[{"id":1},{"id":2}]}`
	rowsSel := Root().Property("rows").EachIndex()
	m := NewMetrics()
	tf, _ := NewTableFromJSON(TableBinding{Schema: MetadataSchema(Root().Property("meta"), ColumnFromMeta), Rows: rowsSel},
		DefaultLimits(), Reject, m, &Table{})
	if _, f := NewParserSource(makeGrammar("json"), text).Metrics(m).Run(tf); f != nil {
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
		t.Fatal(m.Events.Load(), len(rec.Events))
	}
}
