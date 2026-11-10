// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

// The differential suite behind BoundsHistoryGrammars, the twin of the
// history half of ts/test/prune.test.ts.
//
// For every grammar module the tests depend on and every document the
// prune suite has for it (pruneDocuments), a ParserEventSource run with the
// engine's rule history bounded at StreamRuleHistory (UnverifiedHistory)
// must relay the same passes as a run that leaves it unbounded, pass for
// pass in the fixture encoding, end the same way, both complete or both
// fail with the same code, position and message, and leave the same value.
// Neither run prunes. A grammar is listed only when no document differs,
// and the suite asserts the list in both directions. The prune suite's
// pruned runs of a listed grammar are bounded, as a host's are, so pruning
// is verified as it runs.

import (
	"fmt"
	"io"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// historyRun is one run of the suite: how it ended, its passes in the
// fixture encoding and the value the parse returned, with the rule history
// bounded or left as the engine leaves it.
func historyRun(name, text string, bounded bool) (string, []string, any) {
	source := NewParserEventSource(makeGrammar(name), text)
	if bounded {
		source.UnverifiedHistory()
	}
	var rec passRecorder
	flow, f, value := source.RunWithValue(&rec)
	ending := fmt.Sprintf("%v", flow)
	if f != nil {
		ending = fmt.Sprintf("%s at %d:%d: %s", f.Code, f.Row, f.Column, f.Message)
	}
	passes := make([]string, len(rec.seen))
	for i, ev := range rec.seen {
		passes[i] = ev.JSON()
	}
	return ending, passes, value
}

// valueText is a value as text, the same for two values that hold the
// same: pointers followed, a map's keys sorted, an ordered map's in order,
// and a float written as strconv writes it, so that NaN is NaN, which
// reflect.DeepEqual holds unequal to itself.
func valueText(v any) string {
	var b strings.Builder
	writeValue(&b, reflect.ValueOf(v), 0)
	return b.String()
}

func writeValue(b *strings.Builder, v reflect.Value, depth int) {
	if depth > 200 {
		b.WriteString("...")
		return
	}
	switch v.Kind() {
	case reflect.Invalid:
		b.WriteString("nil")
	case reflect.Pointer, reflect.Interface:
		if v.IsNil() {
			b.WriteString("nil")
			return
		}
		writeValue(b, v.Elem(), depth+1)
	case reflect.Struct:
		b.WriteString(v.Type().String() + "{")
		for i := 0; i < v.NumField(); i++ {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(v.Type().Field(i).Name + ": ")
			writeValue(b, v.Field(i), depth+1)
		}
		b.WriteString("}")
	case reflect.Map:
		keys := make([]string, 0, v.Len())
		vals := map[string]reflect.Value{}
		for it := v.MapRange(); it.Next(); {
			var kb strings.Builder
			writeValue(&kb, it.Key(), depth+1)
			keys = append(keys, kb.String())
			vals[kb.String()] = it.Value()
		}
		sort.Strings(keys)
		b.WriteString("map{")
		for i, k := range keys {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(k + ": ")
			writeValue(b, vals[k], depth+1)
		}
		b.WriteString("}")
	case reflect.Slice, reflect.Array:
		if v.Kind() == reflect.Slice && v.IsNil() {
			b.WriteString("nil-slice")
			return
		}
		b.WriteString("[")
		for i := 0; i < v.Len(); i++ {
			if i > 0 {
				b.WriteString(", ")
			}
			writeValue(b, v.Index(i), depth+1)
		}
		b.WriteString("]")
	case reflect.Float32, reflect.Float64:
		b.WriteString(strconv.FormatFloat(v.Float(), 'g', -1, 64))
	case reflect.String:
		b.WriteString(strconv.Quote(v.String()))
	case reflect.Bool:
		b.WriteString(strconv.FormatBool(v.Bool()))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		b.WriteString(strconv.FormatInt(v.Int(), 10))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		b.WriteString(strconv.FormatUint(v.Uint(), 10))
	default:
		b.WriteString(v.Kind().String())
	}
}

// preview is a value's text, shortened, for a message.
func preview(text string) string {
	if len(text) > 160 {
		return text[:157] + "..."
	}
	return text
}

// historyCompare says how the bounded run compares with the unbounded one:
// "" alike, and otherwise where they differ, by the ending, a pass or the
// value.
func historyCompare(name, text string) string {
	wholeEnd, whole, wholeValue := historyRun(name, text, false)
	boundEnd, bound, boundValue := historyRun(name, text, true)
	if wholeEnd != boundEnd {
		return fmt.Sprintf("ended %s unbounded, %s bounded", wholeEnd, boundEnd)
	}
	for i := 0; i < len(whole) || i < len(bound); i++ {
		at := func(passes []string) string {
			if i < len(passes) {
				return passes[i]
			}
			return "nothing"
		}
		if at(whole) != at(bound) {
			return fmt.Sprintf("pass %d of %d differs: %s unbounded, %s bounded", i, len(whole), at(whole), at(bound))
		}
	}
	if whole, bound := valueText(wholeValue), valueText(boundValue); whole != bound {
		return fmt.Sprintf("the value differs: %s unbounded, %s bounded", preview(whole), preview(bound))
	}
	return ""
}

// TestEveryGrammarBoundsTheHistoryAsTheListSays holds the list in both
// directions: a listed grammar no document makes differ bounded, and an
// unlisted grammar some document does.
func TestEveryGrammarBoundsTheHistoryAsTheListSays(t *testing.T) {
	var wrong []string
	for _, g := range pruneGrammars {
		docs := pruneDocuments(t, g)
		var found []string
		for i, doc := range docs {
			if why := historyCompare(g.name, doc.text); why != "" {
				found = append(found, doc.name+": "+why)
			}
			if (i+1)%200 == 0 || i+1 == len(docs) {
				t.Logf("%s bounded: %d of %d (%d%%) documents, %d differing", g.name, i+1, len(docs), (i+1)*100/len(docs), len(found))
			}
		}
		switch listed := BoundsHistory(g.name); {
		case listed && len(found) > 0:
			wrong = append(wrong, fmt.Sprintf("%s is in BoundsHistoryGrammars, but %d of its documents differ bounded:\n    %s",
				g.name, len(found), strings.Join(found, "\n    ")))
		case !listed && len(found) == 0:
			wrong = append(wrong, fmt.Sprintf("%s is not in BoundsHistoryGrammars, but none of its documents differs bounded", g.name))
		case !listed:
			t.Logf("%s bounded: %d documents differ, the first %s", g.name, len(found), found[0])
		}
	}
	for _, name := range BoundsHistoryGrammars() {
		known := false
		for _, g := range pruneGrammars {
			known = known || g.name == name
		}
		if !known {
			wrong = append(wrong, name+" is listed, and this suite never runs it")
		}
		if !Prunable(name) {
			wrong = append(wrong, name+" is in BoundsHistoryGrammars, and not prunable")
		}
	}
	if len(wrong) > 0 {
		t.Fatal(strings.Join(wrong, "\n"))
	}
}

// TestBoundsTheHistoryOfAPruningRunOfAListedGrammarOnly: a run bounds the
// engine's rule history when it prunes a grammar BoundsHistory lists, or
// when the suite asks, and leaves every other run's as the engine has it.
func TestBoundsTheHistoryOfAPruningRunOfAListedGrammarOnly(t *testing.T) {
	history := func(name string, set func(*ParserEventSource)) int {
		parser := makeGrammar(name)
		source := NewParserEventSource(parser, "[1, [2]]")
		set(source)
		if _, f := source.Run(&passRecorder{}); f != nil {
			t.Fatalf("%s: %s", name, f.Message)
		}
		return parser.Config().RuleHistory
	}
	for _, name := range []string{"json", "jsonl"} {
		want := 0
		if BoundsHistory(name) {
			want = StreamRuleHistory
		}
		if got := history(name, func(s *ParserEventSource) { s.Grammar(name).Prune() }); got != want {
			t.Errorf("%s pruned: history %d, want %d", name, got, want)
		}
		if got := history(name, func(s *ParserEventSource) { s.Grammar(name) }); got != 0 {
			t.Errorf("%s not pruned: history %d, want unbounded", name, got)
		}
		if got := history(name, func(s *ParserEventSource) { s.Prune().Unverified() }); got != 0 {
			t.Errorf("%s pruned, unnamed: history %d, want unbounded", name, got)
		}
		if got := history(name, func(s *ParserEventSource) { s.UnverifiedHistory() }); got != StreamRuleHistory {
			t.Errorf("%s with UnverifiedHistory: history %d, want %d", name, got, StreamRuleHistory)
		}
	}
}

// heapSampler is a sink that collects garbage every every passes and keeps
// the heap in use then: what the parse holds live at those points.
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
		s.high = max(s.high, m.HeapAlloc)
	}
	return Continue, nil
}

// recordsReader is a JSON array of records records, made as it is read, so
// the document itself is never held whole. A record's strings are the same
// in every record and its numbers are not: the measure is of what a source
// bounds, the rules.
type recordsReader struct {
	records, next int
	pending       []byte
}

func (r *recordsReader) Read(buf []byte) (int, error) {
	n := 0
	for n < len(buf) {
		if len(r.pending) == 0 {
			switch {
			case r.next > r.records:
				if n == 0 {
					return 0, io.EOF
				}
				return n, nil
			case r.next == r.records:
				r.pending = []byte("]")
			default:
				sep := ","
				if r.next == 0 {
					sep = "["
				}
				r.pending = []byte(fmt.Sprintf(`%s{"id":%d,"person":{"name":"Person"},"account":{"balance":%d.%02d}}`,
					sep, r.next, r.next*7, r.next%100))
			}
			r.next++
		}
		c := copy(buf[n:], r.pending)
		r.pending = r.pending[c:]
		n += c
	}
	return n, nil
}

// TestTenTimesTheRecordsLeaveTheHeapHighWaterWhereItWas: the records'
// lists are emptied behind the stream, the document comes from a reader, so
// the engine holds a window of it, and the history is bounded, json being
// listed: nothing the parse keeps grows with the records.
func TestTenTimesTheRecordsLeaveTheHeapHighWaterWhereItWas(t *testing.T) {
	if !BoundsHistory("json") {
		t.Fatal("json is not in BoundsHistoryGrammars")
	}
	highWater := func(records int) *heapSampler {
		sink := &heapSampler{every: 5000}
		if _, f := ParserEventSourceFromReader(makeGrammar("json"), &recordsReader{records: records}).Grammar("json").Prune().Run(sink); f != nil {
			t.Fatalf("%d records: %s", records, f.Message)
		}
		return sink
	}
	few := highWater(2000)
	t.Logf("history: 1 of 2 (50%%) 2,000 records, %d passes, heap high water %.1f MB", few.passes, float64(few.high)/(1<<20))
	many := highWater(20000)
	t.Logf("history: 2 of 2 (100%%) 20,000 records, %d passes, heap high water %.1f MB", many.passes, float64(many.high)/(1<<20))
	if many.passes <= 9*few.passes {
		t.Fatalf("%d then %d passes", few.passes, many.passes)
	}
	if float64(many.high) >= 1.5*float64(few.high) {
		t.Fatalf("heap high water %.1f MB at 2,000 records and %.1f MB at 20,000", float64(few.high)/(1<<20), float64(many.high)/(1<<20))
	}
}
