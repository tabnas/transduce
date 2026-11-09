// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

// A source bounds the engine's rule history (StreamRuleHistory).
//
// The engine links each rule to the rule it replaced and the rule that
// pushed it, and the Go engine keeps the whole chain by default: a parse of
// a long flat document keeps every rule it made reachable until the
// document ends, whatever the stream prunes, and its memory grows with the
// document's length (tabnas/parser doc/rule-history-bound.md). A
// ParserEventSource bounds the history at three predecessors, the most any
// grammar in the fleet reads and what the Rust engine bounds it at by
// default, unless the grammar or the host bounded it already. Read from a
// reader and pruned, ten times the records then leave the heap's high water
// where it was. The port of ts/test/history.test.ts.

import (
	"fmt"
	"io"
	"runtime"
	"testing"

	tabnasjson "github.com/tabnas/json/go"
	tabnas "github.com/tabnas/parser/go"
)

// heapSampler collects garbage every `every` passes and keeps the heap in
// use then: what the parse holds live at those points.
type heapSampler struct {
	every, passes int
	high          uint64
}

func (s *heapSampler) ParserEvent(ParserEvent) (Flow, *Fail) {
	s.passes++
	if s.passes%s.every == 0 {
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		if m.HeapAlloc > s.high {
			s.high = m.HeapAlloc
		}
	}
	return Continue, nil
}

// streamRecord is one record whose strings are the same in every record and
// whose numbers are not. The Go lexer keeps every distinct short string a
// parse meets (Lex.internAny's pool, which nothing empties), so distinct
// strings would hold memory that grows with the records whatever the
// history; that is the engine's to bound (tabnas/parser#324), and this test
// measures what a source bounds: the rules.
func streamRecord(i int) string {
	return fmt.Sprintf(`{"id":%d,"person":{"name":"Person"},"account":{"balance":%d.%02d}}`, i, i*7, i%100)
}

// recordsReader is a JSON array of n records, made as it is read, so the
// document itself is never held whole.
type recordsReader struct {
	n, next int
	pending []byte
}

func (r *recordsReader) Read(buf []byte) (int, error) {
	written := 0
	for written < len(buf) {
		if len(r.pending) == 0 {
			if r.next > r.n {
				break
			}
			switch {
			case r.n == 0:
				r.pending = []byte("[]")
				r.next = r.n
			case r.next == r.n:
				r.pending = []byte("]")
			case r.next == 0:
				r.pending = []byte("[" + streamRecord(0))
			default:
				r.pending = []byte("," + streamRecord(r.next))
			}
			r.next++
		}
		n := copy(buf[written:], r.pending)
		r.pending = r.pending[n:]
		written += n
	}
	if written == 0 {
		return 0, io.EOF
	}
	return written, nil
}

func TestASourceBoundsAHistoryTheGrammarLeftUnbounded(t *testing.T) {
	parser := tabnasjson.Make()
	if h := parser.Config().RuleHistory; h != 0 {
		t.Fatalf("json's parser has a history of %d before a source runs", h)
	}
	if _, f := NewParserEventSource(parser, "[1,2]").Run(&heapSampler{every: 1 << 30}); f != nil {
		t.Fatal(f)
	}
	if h := parser.Config().RuleHistory; h != StreamRuleHistory || StreamRuleHistory != 3 {
		t.Fatalf("the history is %d after the run, not %d", h, StreamRuleHistory)
	}
}

func TestAHistoryAlreadyBoundedIsLeftAsItIs(t *testing.T) {
	parser := tabnasjson.Make()
	five := 5
	parser.SetOptions(tabnas.Options{Rule: &tabnas.RuleOptions{History: &five}})
	if _, f := NewParserEventSource(parser, "[1,2]").Run(&heapSampler{every: 1 << 30}); f != nil {
		t.Fatal(f)
	}
	if h := parser.Config().RuleHistory; h != 5 {
		t.Fatalf("the history is %d after the run, not 5", h)
	}
}

// The records' lists are emptied behind the stream (Prune), the document
// comes from a reader, so the engine holds a window of it, and the history
// is bounded: nothing the parse keeps grows with the records.
func TestTenTimesTheRecordsLeaveTheHeapHighWaterWhereItWas(t *testing.T) {
	highWater := func(records int) *heapSampler {
		sink := &heapSampler{every: 5000}
		source := ParserEventSourceFromReader(tabnasjson.Make(), &recordsReader{n: records}).Grammar("json").Prune()
		if _, f := source.Run(sink); f != nil {
			t.Fatal(f)
		}
		return sink
	}
	few := highWater(2000)
	fmt.Printf("history: 1 of 2 (50%%) 2,000 records, %d passes, heap high water %.1f MB\n", few.passes, mb(few.high))
	many := highWater(20000)
	fmt.Printf("history: 2 of 2 (100%%) 20,000 records, %d passes, heap high water %.1f MB\n", many.passes, mb(many.high))
	if 9*few.passes >= many.passes {
		t.Fatalf("%d then %d passes", few.passes, many.passes)
	}
	if float64(many.high) >= 1.5*float64(few.high) {
		t.Fatalf("%.1f MB then %.1f MB", mb(few.high), mb(many.high))
	}
}

func mb(bytes uint64) float64 { return float64(bytes) / (1024 * 1024) }
