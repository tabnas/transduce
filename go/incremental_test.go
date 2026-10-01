// Copyright (c) 2026 tabnas, MIT License

//go:build tabnas_nodecell

package tabnastransduce

// The differential suite behind IncrementalGrammars, ported from
// rs/tests/incremental_test.rs.
//
// For every grammar module the tests depend on and every fixture that
// grammar reads, the events ModeIncremental produces must equal the
// events ModeMaterialize produces (number lexemes aside), or differ only
// where the document repeats a member name (a LastWins router builds the
// same value from both), or be a documented refusal after a
// protocol-valid prefix and before End. A fixture the grammar itself
// refuses must fail the incremental run too, with the grammar's code at
// the grammar's position, or with a documented refusal. A completed
// stream that disagrees with the walk is never accepted, and two
// refusals count against a grammar: a container opened inside a map
// before the member's key, and a container streamed and never stored. A
// grammar is listed only when no fixture mismatches or is refused that
// way, and the suite asserts BOTH directions.
//
// The fixtures are the Rust crate's (../rs/tests/fixtures), shared rather
// than copied, and four generated documents in the worked-example shape.

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	tabnas "github.com/tabnas/parser/go"
)

const diffRecords = 2000

type diffGrammar struct {
	name      string
	generated []string
}

var diffGrammars = []diffGrammar{
	{"json", []string{"records.json"}},
	{"jsonl", []string{"records.jsonl"}},
	{"jsonic", []string{"records.json"}},
	{"jsonc", []string{"records.json"}},
	{"json5", []string{"records.json"}},
	{"yaml", []string{"records.yaml"}},
	{"toml", nil},
	{"ini", nil},
	{"csv", []string{"records.csv"}},
	{"xml", nil},
	{"zon", nil},
	{"markdown", nil},
	{"feed", nil},
}

func fixturesDir() string { return filepath.Join("..", "rs", "tests", "fixtures") }

func fixtureText(t testing.TB, name string) string {
	b, err := os.ReadFile(filepath.Join(fixturesDir(), name))
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return string(b)
}

type namedText struct{ name, text string }

func committedFixtures(t testing.TB) []namedText {
	entries, err := os.ReadDir(fixturesDir())
	if err != nil {
		t.Fatalf("the Rust fixtures: %v", err)
	}
	var out []namedText
	for _, e := range entries {
		out = append(out, namedText{e.Name(), fixtureText(t, e.Name())})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}

func generatedDoc(name string) string {
	switch name {
	case "records.json":
		return recordsJSON(diffRecords)
	case "records.jsonl":
		return recordsJSONL(diffRecords)
	case "records.csv":
		return recordsCSV(diffRecords)
	case "records.yaml":
		return recordsYAML(diffRecords)
	}
	panic("no generator for " + name)
}

func withoutLexemes(events []Event) []Event {
	out := make([]Event, len(events))
	for i, ev := range events {
		out[i] = ev.WithoutLexeme()
	}
	return out
}

func diffRun(name, text string, mode SourceMode) (*Fail, []Event) {
	var rec Recorder
	_, f := NewParserSource(makeGrammar(name), text).Unverified().Mode(mode).Run(&rec)
	return f, rec.Events
}

func incrementalRun(name, text string) (*Fail, []Event) {
	return diffRun(name, text, IncrementalMode(Prune{}))
}

// wellFormed reports whether a recording is a protocol-valid stream, or a
// prefix of one.
func wellFormed(events []Event) bool {
	m := NewMatcher(nil)
	for _, ev := range events {
		if _, f := m.Event(ev); f != nil {
			return false
		}
	}
	return true
}

func hasEnd(events []Event) bool {
	for _, ev := range events {
		if ev.Kind == End {
			return true
		}
	}
	return false
}

func eventsEqual(a, b []Event) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// rootValue is the document's root value as a router materializes it
// from a recording under policy.
func rootValue(events []Event, policy Duplicates) (Datum, *Fail) {
	var rec SelectedRecorder
	r, f := NewRouter([]CaptureSpec{MaterializeSpec("root", Root())}, DefaultLimits(), policy, NewMetrics(), &rec)
	if f != nil {
		return Datum{}, f
	}
	if _, f := Replay(events, r); f != nil {
		return Datum{}, f
	}
	if len(rec.Matches) == 0 || rec.Matches[0].Value == nil {
		return NullDatum(), nil
	}
	return *rec.Matches[0].Value, nil
}

func unfollowed(f *Fail) bool {
	return f.Code == CodeStreamabilityUnknown &&
		strings.Contains(f.Message, "the incremental source cannot follow a grammar that builds")
}

type outcomeKind int

const (
	outNotRead outcomeKind = iota
	outMatch
	outMatchLastWins
	outRefused
	outUnfollowed
	outMismatch
)

type outcome struct {
	kind    outcomeKind
	events  int
	lexemes int
	code    Code
	walk    Code
	why     string
}

func compareModes(g diffGrammar, text string) outcome {
	whole, materialized := diffRun(g.name, text, MaterializeMode())
	if whole != nil {
		inc, incremental := incrementalRun(g.name, text)
		switch {
		case inc != nil && wellFormed(incremental) && !hasEnd(incremental) && unfollowed(inc):
			return outcome{kind: outUnfollowed, events: len(incremental)}
		case inc != nil && wellFormed(incremental) && !hasEnd(incremental) &&
			((inc.Code == whole.Code && inc.Row == whole.Row && inc.Column == whole.Column) ||
				inc.Code == CodeStreamabilityUnknown || inc.Code == CodeDuplicateMember):
			return outcome{kind: outNotRead, walk: whole.Code, code: inc.Code}
		case inc != nil:
			return outcome{kind: outMismatch, why: fmt.Sprintf(
				"the walk failed with %v; the incremental run failed with %v after %d events (protocol-valid prefix: %v)",
				whole, inc, len(incremental), wellFormed(incremental))}
		}
		return outcome{kind: outMismatch, why: fmt.Sprintf(
			"the walk failed with %v; the incremental run completed after %d events", whole, len(incremental))}
	}
	inc, incremental := incrementalRun(g.name, text)
	if inc != nil {
		documented := inc.Code == CodeStreamabilityUnknown || inc.Code == CodeDuplicateMember
		if documented && wellFormed(incremental) && !hasEnd(incremental) {
			if unfollowed(inc) {
				return outcome{kind: outUnfollowed, events: len(incremental)}
			}
			return outcome{kind: outRefused, code: inc.Code, events: len(incremental)}
		}
		return outcome{kind: outMismatch, why: fmt.Sprintf(
			"the incremental run failed with %v after %d events", inc, len(incremental))}
	}
	lexemes := 0
	for _, ev := range incremental {
		if ev.Kind == Number && ev.HasLexeme {
			lexemes++
			if v, err := strconv.ParseFloat(ev.Lexeme, 64); err != nil || v != ev.Value {
				return outcome{kind: outMismatch, why: fmt.Sprintf("lexeme %q does not spell the value %v", ev.Lexeme, ev.Value)}
			}
		}
	}
	stripped := withoutLexemes(incremental)
	if eventsEqual(stripped, materialized) {
		return outcome{kind: outMatch, events: len(materialized), lexemes: lexemes}
	}
	if wellFormed(stripped) {
		a, fa := rootValue(stripped, LastWins)
		b, fb := rootValue(materialized, Reject)
		if fa == nil && fb == nil && a.Equal(b) {
			return outcome{kind: outMatchLastWins, events: len(stripped)}
		}
	}
	first := 0
	for first < len(stripped) && first < len(materialized) && stripped[first] == materialized[first] {
		first++
	}
	show := func(events []Event) string {
		from, to := first-3, first+4
		if from < 0 {
			from = 0
		}
		if to > len(events) {
			to = len(events)
		}
		var parts []string
		for _, ev := range events[from:to] {
			parts = append(parts, ev.String())
		}
		return strings.Join(parts, " ")
	}
	return outcome{kind: outMismatch, why: fmt.Sprintf(
		"incremental produced %d events, the walk %d; first difference at event %d: incremental [%s] vs walk [%s]",
		len(stripped), len(materialized), first, show(stripped), show(materialized))}
}

// verifyGrammar runs one grammar over every fixture it reads and checks
// the verified list agrees with what happened.
func verifyGrammar(t *testing.T, name string) {
	var g diffGrammar
	for _, d := range diffGrammars {
		if d.name == name {
			g = d
		}
	}
	fixtures := committedFixtures(t)
	for _, gen := range g.generated {
		fixtures = append(fixtures, namedText{fmt.Sprintf("generated %s (%d records)", gen, diffRecords), generatedDoc(gen)})
	}
	total, read := len(fixtures), 0
	var mismatches, unfollowedList []string
	for i, fx := range fixtures {
		started := time.Now()
		o := compareModes(g, fx.text)
		var verdict string
		switch o.kind {
		case outNotRead:
			verdict = fmt.Sprintf("not read (%v; the incremental run failed with %v)", o.walk, o.code)
		case outMatch:
			read++
			verdict = fmt.Sprintf("MATCH (%d events, %d lexemes)", o.events, o.lexemes)
		case outMatchLastWins:
			read++
			verdict = fmt.Sprintf("MATCH after LastWins (%d events; the document repeats a member name)", o.events)
		case outRefused:
			read++
			verdict = fmt.Sprintf("REFUSED with %v after %d events", o.code, o.events)
		case outUnfollowed:
			read++
			unfollowedList = append(unfollowedList, fmt.Sprintf("%s: after %d events", fx.name, o.events))
			verdict = fmt.Sprintf("UNFOLLOWED: refused for how the grammar builds, after %d events", o.events)
		case outMismatch:
			read++
			mismatches = append(mismatches, fx.name+": "+o.why)
			verdict = "MISMATCH: " + o.why
		}
		fmt.Printf("incremental %s: %d of %d (%d%%) %s: %s [%.2fs]\n",
			name, i+1, total, (i+1)*100/total, fx.name, verdict, time.Since(started).Seconds())
	}
	if read == 0 {
		t.Fatalf("grammar %s read none of the fixtures", name)
	}
	verified := len(mismatches) == 0 && len(unfollowedList) == 0
	listed := Incremental(name)
	if listed != verified {
		problems := append([]string(nil), mismatches...)
		for _, u := range unfollowedList {
			problems = append(problems, u+": refused for how the grammar builds its values")
		}
		advice := "never mismatched; add it to the verified list"
		if !verified {
			advice = "mismatched or was refused for how it builds; remove it from the verified list, or fix the adapter or the grammar"
		}
		t.Fatalf("Incremental(%q) is %v, but the grammar %s over %d fixtures it reads:\n  %s",
			name, listed, advice, read, strings.Join(problems, "\n  "))
	}
}

func TestIncrementalJSON(t *testing.T)     { verifyGrammar(t, "json") }
func TestIncrementalJSONL(t *testing.T)    { verifyGrammar(t, "jsonl") }
func TestIncrementalJsonic(t *testing.T)   { verifyGrammar(t, "jsonic") }
func TestIncrementalJSONC(t *testing.T)    { verifyGrammar(t, "jsonc") }
func TestIncrementalJSON5(t *testing.T)    { verifyGrammar(t, "json5") }
func TestIncrementalYAML(t *testing.T)     { verifyGrammar(t, "yaml") }
func TestIncrementalTOML(t *testing.T)     { verifyGrammar(t, "toml") }
func TestIncrementalINI(t *testing.T)      { verifyGrammar(t, "ini") }
func TestIncrementalCSV(t *testing.T)      { verifyGrammar(t, "csv") }
func TestIncrementalXML(t *testing.T)      { verifyGrammar(t, "xml") }
func TestIncrementalZON(t *testing.T)      { verifyGrammar(t, "zon") }
func TestIncrementalMarkdown(t *testing.T) { verifyGrammar(t, "markdown") }
func TestIncrementalFeed(t *testing.T)     { verifyGrammar(t, "feed") }

// moduleGrammars is the grammar modules go.mod requires directly: a
// tabnas module whose own go.mod requires the engine. Read at test time,
// so a grammar added there without a diffGrammars row fails here.
func moduleGrammars(t *testing.T) []string {
	raw, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatal(err)
	}
	direct := map[string]bool{}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "require"))
		if strings.Contains(line, "// indirect") {
			continue
		}
		if f := strings.Fields(line); len(f) >= 2 && strings.HasPrefix(f[0], "github.com/tabnas/") {
			direct[f[0]] = true
		}
	}
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Path}}\t{{.Dir}}", "all").Output()
	if err != nil {
		t.Fatalf("go list -m: %v", err)
	}
	var names []string
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		path, dir, ok := strings.Cut(line, "\t")
		if !ok || !direct[path] || dir == "" || path == "github.com/tabnas/parser/go" {
			continue
		}
		mod, err := os.ReadFile(filepath.Join(dir, "go.mod"))
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		if strings.Contains(string(mod), "github.com/tabnas/parser/go") || strings.Contains(string(mod), "github.com/tabnas/jsonic/go") {
			names = append(names, strings.TrimSuffix(strings.TrimPrefix(path, "github.com/tabnas/"), "/go"))
		}
	}
	sort.Strings(names)
	return names
}

func TestEveryGrammarModuleIsVerifiedHere(t *testing.T) {
	var suite []string
	for _, g := range diffGrammars {
		suite = append(suite, g.name)
	}
	sort.Strings(suite)
	if got := moduleGrammars(t); fmt.Sprint(got) != fmt.Sprint(suite) {
		t.Fatalf("the grammar modules go.mod requires %v and the grammars this suite runs %v differ", got, suite)
	}
	for _, name := range IncrementalGrammars() {
		found := false
		for _, g := range diffGrammars {
			found = found || g.name == name
		}
		if !found {
			t.Fatalf("the verified list names %q, which this suite does not run", name)
		}
	}
}

// A repeated member name streams every occurrence where the walk keeps
// the survivor; LastWins agrees with the walk, Reject sees the duplicate.
func TestARepeatedScalarMemberStreamsEveryOccurrence(t *testing.T) {
	cases := [][2]string{
		{"json", `{"a":1,"a":2,"b":3}`},
		{"jsonl", "{\"a\":1,\"a\":2}\n{\"a\":3,\"a\":4,\"b\":5}\n"},
		{"yaml", "a: 1\na: 2\nb: 3\n"},
		{"json5", "{a:1,a:2,b:3}"},
		{"jsonc", `{"a":1,"a":2,"b":3}`},
		{"jsonic", "a:1,a:2,b:3"},
	}
	for _, c := range cases {
		f, events := incrementalRun(c[0], c[1])
		if f != nil {
			t.Fatalf("%s: %v", c[0], f)
		}
		if !wellFormed(events) {
			t.Fatalf("%s: %v", c[0], events)
		}
		keys := 0
		for _, ev := range events {
			if ev.Kind == Key && ev.Text == "a" {
				keys++
			}
		}
		if keys < 2 {
			t.Fatalf("%s: both occurrences of a are in the stream", c[0])
		}
		_, walked := diffRun(c[0], c[1], MaterializeMode())
		a, _ := rootValue(withoutLexemes(events), LastWins)
		b, _ := rootValue(walked, Reject)
		if !a.Equal(b) {
			t.Fatalf("%s: last wins %v is not the engine's value %v", c[0], a, b)
		}
		if _, f := rootValue(events, Reject); f == nil || f.Code != CodeDuplicateMember {
			t.Fatalf("%s: Reject: %v", c[0], f)
		}
	}
}

// The grammars with map.extend off replace the earlier value whatever the
// shapes, so both are streamed and last wins.
func TestARepeatedMemberAGrammarReplacesStreamsBothValues(t *testing.T) {
	for _, name := range []string{"json", "jsonl", "jsonc"} {
		for _, text := range []string{
			`{"a":{"x":1},"a":{"y":2}}`, `{"a":[1],"a":2}`, `{"a":1,"a":{"y":2}}`,
			`{"a":1,"a":1,"a":2}`, `{"a":{"x":1},"a":{"x":1}}`,
		} {
			f, events := incrementalRun(name, text)
			if f != nil || !wellFormed(events) {
				t.Fatalf("%s %s: %v %v", name, text, f, events)
			}
			_, walked := diffRun(name, text, MaterializeMode())
			a, _ := rootValue(withoutLexemes(events), LastWins)
			b, _ := rootValue(walked, Reject)
			if !a.Equal(b) {
				t.Fatalf("%s %s: %v vs %v", name, text, a, b)
			}
		}
	}
}

// A grammar that merges the containers of a repeated name fails with
// DUPLICATE_MEMBER rather than emit a stream the walk contradicts.
func TestARepeatedContainerMemberTheGrammarMergesFails(t *testing.T) {
	cases := [][3]string{
		{"yaml flow", "yaml", "a: {x: 1}\na: {y: 2}\n"},
		{"yaml block", "yaml", "a:\n  x: 1\na:\n  y: 2\n"},
		{"json5", "json5", "{a:{x:1},a:{y:2}}"},
		{"jsonic", "jsonic", "a:{x:1},a:{y:2}"},
	}
	for _, c := range cases {
		f, events := incrementalRun(c[1], c[2])
		if f == nil {
			t.Fatalf("%s: completed after %v", c[0], events)
		}
		if f.Code != CodeDuplicateMember || !strings.Contains(f.Message, "Materialize") {
			t.Fatalf("%s: %v", c[0], f)
		}
		if !wellFormed(events) || hasEnd(events) {
			t.Fatalf("%s: %v", c[0], events)
		}
		wf, walked := diffRun(c[1], c[2], MaterializeMode())
		if wf != nil {
			t.Fatal(wf)
		}
		if v, _ := rootValue(walked, Reject); v.String() != `{"a":{"x":1,"y":2}}` {
			t.Fatalf("%s: the walk has %v", c[0], v)
		}
	}
}

// zon refuses a repeated field itself, before the assignment: the
// incremental run fails exactly as the walk does, after a protocol-valid
// prefix that does not stream the member the grammar never stored.
func TestEveryRepeatedFieldZonFixtureFailsAsTheWalkDoes(t *testing.T) {
	k, n := EvKey, EvNumber
	os, oe, as, ae := EvObjectStart(), EvObjectEnd(), EvArrayStart(), EvArrayEnd()
	cases := []struct {
		name   string
		prefix []Event
	}{
		{"repeated-scalar.zon", []Event{os, k("a"), n(1), k("a")}},
		{"repeated-scalar-then-struct.zon", []Event{os, k("a"), n(1), k("a"), os, k("y"), n(2), oe}},
		{"repeated-struct-then-scalar.zon", []Event{os, k("a"), os, k("x"), n(1), oe, k("a")}},
		{"repeated-structs.zon", []Event{os, k("a"), os, k("x"), n(1), oe, k("a"), os, k("y"), n(2), oe}},
		{"repeated-scalar-then-tuple.zon", []Event{os, k("a"), n(1), k("a"), as, n(7), n(8), ae}},
		{"repeated-after-another.zon", []Event{os, k("a"), n(1), k("b"), n(5), k("a"), os, k("y"), n(2), oe}},
		{"repeated-nested.zon", []Event{os, k("o"), os, k("a"), n(1), k("a"), os, k("y"), n(2), oe}},
	}
	for _, c := range cases {
		text := fixtureText(t, c.name)
		expected, walked := diffRun("zon", text, MaterializeMode())
		if expected == nil || expected.Code != CodeInputInvalid || !strings.Contains(expected.Message, "zon_dup_field") {
			t.Fatalf("%s: the walk: %v", c.name, expected)
		}
		if len(walked) != 0 {
			t.Fatalf("%s: the walk emits nothing: %v", c.name, walked)
		}
		f, events := incrementalRun("zon", text)
		if f == nil {
			t.Fatalf("%s: completed after %v", c.name, events)
		}
		if f.Code != expected.Code || f.Message != expected.Message || f.Row != expected.Row || f.Column != expected.Column {
			t.Fatalf("%s: %v, the walk %v", c.name, f, expected)
		}
		if !wellFormed(events) || hasEnd(events) {
			t.Fatalf("%s: %v", c.name, events)
		}
		if !eventsEqual(withoutLexemes(events), c.prefix) {
			t.Fatalf("%s: the prefix %v, want %v", c.name, events, c.prefix)
		}
		var rec SelectedRecorder
		router, _ := NewRouter([]CaptureSpec{MaterializeSpec("root", Root())}, DefaultLimits(), LastWins, NewMetrics(), &rec)
		_, rf := NewParserSource(makeGrammar("zon"), text).Unverified().Mode(IncrementalMode(Prune{})).Run(router)
		if rf == nil || rf.Code != CodeInputInvalid || len(rec.Matches) != 0 {
			t.Fatalf("%s: a router consumer sees the grammar's failure: %v %v", c.name, rf, rec.Matches)
		}
	}
}

// YAML resolves a merge key when the mapping closes: the members the
// adapter streamed are no longer the map's, so the run is refused.
func TestAMapTheGrammarRewritesAfterStreamingIsRefused(t *testing.T) {
	text := "base: &b\n  x: 1\nd:\n  <<: *b\n  y: 2\n"
	f, events := incrementalRun("yaml", text)
	if f == nil || f.Code != CodeStreamabilityUnknown || !strings.Contains(f.Message, "merge key") {
		t.Fatalf("%v", f)
	}
	if !wellFormed(events) || hasEnd(events) {
		t.Fatal(events)
	}
	// An alias without a merge key copies the value and streams as the walk.
	text = "a: &r {x: 1}\nb: *r\n"
	f, events = incrementalRun("yaml", text)
	_, walked := diffRun("yaml", text, MaterializeMode())
	if f != nil || !eventsEqual(withoutLexemes(events), walked) {
		t.Fatalf("%v %v %v", f, events, walked)
	}
}

// jsonic drops a pair inside a list (list.pair off): a container the
// adapter streamed and the grammar never stored is refused.
func TestAContainerTheGrammarStreamedAndNeverStoredIsRefused(t *testing.T) {
	wf, walked := diffRun("jsonic", "[a:{b:1}]", MaterializeMode())
	if v, _ := rootValue(walked, Reject); wf != nil || v.String() != "[]" {
		t.Fatalf("%v %v", wf, v)
	}
	f, events := incrementalRun("jsonic", "[a:{b:1}]")
	if f == nil || f.Code != CodeStreamabilityUnknown || !strings.Contains(f.Message, "never stored it") {
		t.Fatalf("%v", f)
	}
	want := []Event{EvArrayStart(), EvObjectStart(), EvKey("b"), EvNumber(1), EvObjectEnd()}
	if !wellFormed(events) || hasEnd(events) || !eventsEqual(withoutLexemes(events), want) {
		t.Fatalf("%v", events)
	}
	for _, text := range []string{"[a:{b:1},2]", "[a:{b:1},c:{d:2}]"} {
		f, events := incrementalRun("jsonic", text)
		if f == nil || f.Code != CodeStreamabilityUnknown || !strings.Contains(f.Message, "never stored it") ||
			!wellFormed(events) || hasEnd(events) {
			t.Fatalf("%s: %v %v", text, f, events)
		}
	}
	for _, text := range []string{"[a:1]", "[1,a:1,2]"} {
		f, events := incrementalRun("jsonic", text)
		_, walked := diffRun("jsonic", text, MaterializeMode())
		if f != nil || !eventsEqual(withoutLexemes(events), walked) {
			t.Fatalf("%s: %v %v %v", text, f, events, walked)
		}
	}
}

// A YAML key that is itself a mapping (YAML Test Suite V9D5), in a mapping
// that starts in a sequence entry. The grammar named the first member of
// such a mapping in the pass that opened the mapping, so the value's
// mapping opened before the adapter had the key and the run was refused
// (tabnas/transduce#7). Since tabnas/yaml#107 the grammar names that member
// before its value's rule opens, and the document streams exactly as the
// walk; so do the same first member without the `?`, in a block or a flow
// sequence, and an explicit key whose value is a scalar.
func TestAYAMLKeyThatIsAMappingStreamsAsTheWalk(t *testing.T) {
	text := "- sun: yellow\n- ? earth: blue\n  : moon: white\n"
	wf, walked := diffRun("yaml", text, MaterializeMode())
	if v, _ := rootValue(walked, Reject); wf != nil || v.String() != `[{"sun":"yellow"},{"earth: blue":{"moon":"white"}}]` {
		t.Fatalf("%v %v", wf, v)
	}
	f, events := incrementalRun("yaml", text)
	if f != nil {
		t.Fatalf("%v after %v", f, events)
	}
	want := []Event{
		EvArrayStart(), EvObjectStart(), EvKey("sun"), EvString("yellow"), EvObjectEnd(),
		EvObjectStart(), EvKey("earth: blue"), EvObjectStart(), EvKey("moon"), EvString("white"),
		EvObjectEnd(), EvObjectEnd(), EvArrayEnd(), EvEnd(),
	}
	if !eventsEqual(withoutLexemes(events), want) || !eventsEqual(withoutLexemes(events), walked) {
		t.Fatalf("%v, the walk %v", events, walked)
	}
	for _, text := range []string{"- a:\n    b: 1\n", "- a:\n  - x\n", "[a: {b: 1}]\n", "? earth\n: moon\n"} {
		f, events := incrementalRun("yaml", text)
		_, walked := diffRun("yaml", text, MaterializeMode())
		if f != nil || !eventsEqual(withoutLexemes(events), walked) {
			t.Fatalf("%q: %v %v %v", text, f, events, walked)
		}
	}
}

// A grammar that builds a member's value in a rule of its own and names
// the member only when the pair closes is refused when the value opens.
// No listed grammar builds a member so since tabnas/yaml#107, so this
// grammar is what keeps the net tested.
func TestAContainerOpenedInAMapBeforeItsKeyIsRefused(t *testing.T) {
	lateKey := func() *tabnas.Tabnas {
		j := tabnas.Make()
		j.Rule("val", func(rs *tabnas.RuleSpec, _ *tabnas.Parser) {
			rs.AddBO(func(r *tabnas.Rule, _ *tabnas.Context) { r.Node = tabnas.NewOrderedMap() })
			rs.AddOpen(&tabnas.AltSpec{S: [][]tabnas.Tin{{tabnas.TinOS}}, P: "list"})
			rs.AddClose(&tabnas.AltSpec{S: [][]tabnas.Tin{{tabnas.TinZZ}}, A: func(r *tabnas.Rule, _ *tabnas.Context) {
				if m, ok := r.Node.(*tabnas.OrderedMap); ok {
					m.Set("k", r.Child.Node)
				}
			}})
		})
		j.Rule("list", func(rs *tabnas.RuleSpec, _ *tabnas.Parser) {
			rs.AddBO(func(r *tabnas.Rule, _ *tabnas.Context) { r.Node = []any{} })
			rs.AddOpen(&tabnas.AltSpec{S: [][]tabnas.Tin{{tabnas.TinNR}}, A: func(r *tabnas.Rule, _ *tabnas.Context) {
				if l, ok := r.Node.([]any); ok {
					r.Node = append(l, r.O0.Val)
				}
			}})
			rs.AddClose(&tabnas.AltSpec{S: [][]tabnas.Tin{{tabnas.TinCS}}})
		})
		return j
	}
	var rec Recorder
	_, wf := NewParserSource(lateKey(), "[1]").Run(&rec)
	if v, _ := rootValue(rec.Events, Reject); wf != nil || v.String() != `{"k":[1]}` {
		t.Fatalf("%v %v", wf, v)
	}
	rec = Recorder{}
	_, f := NewParserSource(lateKey(), "[1]").Unverified().Mode(IncrementalMode(Prune{})).Run(&rec)
	if f == nil || f.Code != CodeStreamabilityUnknown || !strings.Contains(f.Message, "before announcing the member's key") {
		t.Fatalf("%v", f)
	}
	if !eventsEqual(rec.Events, []Event{EvObjectStart()}) {
		t.Fatalf("%v", rec.Events)
	}
}

// The YAML root shapes: a single document streams as the walk; a stream
// of several documents streams exactly the walk or is refused before End.
func TestEveryYAMLRootShapeStreamsAsTheWalkOrIsRefused(t *testing.T) {
	for _, name := range []string{"empty.yaml", "comment.yaml", "scalar.yaml", "marker.yaml"} {
		text := fixtureText(t, name)
		wf, walked := diffRun("yaml", text, MaterializeMode())
		f, events := incrementalRun("yaml", text)
		if wf != nil || f != nil || !eventsEqual(withoutLexemes(events), walked) {
			t.Fatalf("%s: %v %v %v %v", name, wf, f, events, walked)
		}
	}
	one := func(k string, n float64) []Event {
		return []Event{EvObjectStart(), EvKey(k), EvNumber(n), EvObjectEnd()}
	}
	streams := []struct {
		name   string
		prefix []Event // nil: streams as the walk
	}{
		{"stream.yaml", one("a", 1)},
		{"stream-scalars.yaml", nil},
		{"stream-sequences.yaml", []Event{EvArrayStart(), EvNumber(1), EvArrayEnd()}},
		{"stream-map-scalar.yaml", one("a", 1)},
		{"stream-scalar-map.yaml", one("b", 2)},
		{"stream-empty-map.yaml", one("b", 2)},
		{"stream-map-empty.yaml", one("a", 1)},
	}
	for _, s := range streams {
		text := fixtureText(t, s.name)
		wf, walked := diffRun("yaml", text, MaterializeMode())
		if wf != nil || len(walked) == 0 || walked[0].Kind != ArrayStart {
			t.Fatalf("%s: the walk sees the documents wrapped in a list: %v %v", s.name, wf, walked)
		}
		f, events := incrementalRun("yaml", text)
		if s.prefix == nil {
			if f != nil || !eventsEqual(withoutLexemes(events), walked) {
				t.Fatalf("%s: %v %v", s.name, f, events)
			}
			continue
		}
		if f == nil || f.Code != CodeStreamabilityUnknown || !strings.Contains(f.Message, "several documents") ||
			!wellFormed(events) || hasEnd(events) || !eventsEqual(withoutLexemes(events), s.prefix) {
			t.Fatalf("%s: %v %v", s.name, f, events)
		}
	}
}

// markdown's nodes land whole and are walked at their insertion.
func TestMarkdownDocumentsStreamAsTheWalk(t *testing.T) {
	for _, text := range []string{
		"# Title\n\nSome *emphasis* and a [link](http://x).\n\n- one\n- two\n  - nested\n\n```rust\nfn x() {}\n```\n\n| a | b |\n|---|---|\n| 1 | 2 |\n\n> quote\n\n1. first\n2. second\n",
		"para one\npara one continued\n\npara two\n",
		"",
		"***\n\n# A\n## B\n### C\n",
		"text with `code` and **bold** and ![img](u) end\n",
		"- a\n\n  b\n- c\n\n> - d\n> - e\n",
	} {
		wf, walked := diffRun("markdown", text, MaterializeMode())
		f, events := incrementalRun("markdown", text)
		if wf != nil || f != nil || !eventsEqual(withoutLexemes(events), walked) {
			t.Fatalf("%q: %v %v\n%v\n%v", text, wf, f, events, walked)
		}
	}
}

// The source consults the list by the grammar's name: an unlisted grammar
// is refused before the parse, a listed one runs.
func TestAnUnlistedGrammarInIncrementalModeIsRefusedBeforeItEmitsAnything(t *testing.T) {
	text := fixtureText(t, "sample.csv")
	refused := 0
	for _, g := range diffGrammars {
		var rec Recorder
		_, f := NewParserSource(makeGrammar(g.name), text).Grammar(g.name).Mode(IncrementalMode(Prune{})).Run(&rec)
		if Incremental(g.name) {
			if f != nil && f.Code == CodeStreamabilityUnknown && strings.Contains(f.Message, "IncrementalGrammars") {
				t.Fatalf("%s: %v", g.name, f)
			}
			continue
		}
		if f == nil || f.Code != CodeStreamabilityUnknown || !strings.Contains(f.Message, g.name) || len(rec.Events) != 0 {
			t.Fatalf("%s: %v %v", g.name, f, rec.Events)
		}
		refused++
	}
	if refused != len(diffGrammars)-len(IncrementalGrammars()) {
		t.Fatalf("refused %d", refused)
	}
}
