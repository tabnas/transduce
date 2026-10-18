// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

// The differential suite behind PrunableGrammars, ported from
// rs/tests/prune_test.rs.
//
// For every grammar module the tests depend on and every document the
// suite has for it, a ParserEventSource run that prunes must relay the
// same passes as one that does not, pass for pass in the fixture
// encoding, and end the same way: both complete, or both fail with the
// same code, position and message. One difference short of identity is
// the contract, and the suite counts it apart: a token whose value is a
// container of the tree carries what the tree holds when its pass is
// made, so the lists in it may have been emptied, while its kind and its
// maps' members are as they were (a yaml alias, which the lexer resolves
// to the anchored node). The documents are the Rust crate's fixtures,
// offered to every grammar, the generated worked-example documents of
// each grammar's family, and every document of the grammar's own corpus,
// the test/spec/*.tsv rows of the sibling checkout. A grammar is listed
// only when no document differs, and the suite asserts the list in both
// directions. The second half measures what pruning is for: ten times the
// records leave the pruned tree as it was.

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	tabnas "github.com/tabnas/parser/go"
	support "github.com/tabnas/support/go"
)

// The generated documents' records in the differential: as many shapes as
// 2,000 would hold, at a tenth of the parsing.
const pruneRecords = 200

type pruneGrammar struct {
	name      string
	generated []string
	repo      string
}

var pruneGrammars = []pruneGrammar{
	{"json", []string{"records.json"}, "json"},
	{"jsonl", []string{"records.jsonl"}, "jsonl"},
	{"jsonic", []string{"records.json"}, "jsonic"},
	{"jsonc", []string{"records.json"}, "jsonc"},
	{"json5", []string{"records.json"}, "json5"},
	{"yaml", []string{"records.yaml"}, "yaml"},
	{"toml", nil, "toml"},
	{"ini", nil, "ini"},
	{"csv", []string{"records.csv"}, "csv"},
	{"xml", []string{"records.xml"}, "xml"},
	{"zon", nil, "zon"},
	{"markdown", nil, "markdown"},
	{"feed", nil, "feed"},
}

type prunedDoc struct{ name, text string }

func pruneGenerated(name string, records int) string {
	switch name {
	case "records.json":
		return recordsJSON(records)
	case "records.jsonl":
		return recordsJSONL(records)
	case "records.csv":
		return recordsCSV(records)
	case "records.yaml":
		return recordsYAML(records)
	case "records.xml":
		return recordsXML(records)
	}
	panic("no generator for " + name)
}

// pruneDocuments is what the suite has for g: the Rust crate's fixtures,
// g's generated documents and g's own corpus.
func pruneDocuments(t *testing.T, g pruneGrammar) []prunedDoc {
	t.Helper()
	dir := filepath.Join("..", "rs", "tests", "fixtures")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("the Rust fixtures: %v", err)
	}
	var docs []prunedDoc
	for _, e := range entries {
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		docs = append(docs, prunedDoc{e.Name(), string(b)})
	}
	sort.Slice(docs, func(i, j int) bool { return docs[i].name < docs[j].name })
	for _, name := range g.generated {
		docs = append(docs, prunedDoc{name, pruneGenerated(name, pruneRecords)})
	}
	specs, err := support.LoadSpecDir(filepath.Join("..", "..", g.repo, "test", "spec"), nil)
	if err != nil {
		t.Fatalf("%v: clone https://github.com/tabnas/%s beside this repository", err, g.repo)
	}
	for _, spec := range specs {
		for _, row := range spec.Rows {
			docs = append(docs, prunedDoc{row.Where(), row.UnescNamed("input")})
		}
	}
	return docs
}

// prunedRun is one run: how it ended, its passes in the fixture encoding
// and shaped as pruning may leave them, and the engine's tree afterwards.
func prunedRun(name, text string, prune bool) (string, []string, []string, any) {
	source := NewParserEventSource(makeGrammar(name), text)
	if prune {
		source.Prune().Unverified()
	}
	var rec passRecorder
	flow, f, value := source.RunWithValue(&rec)
	ending := fmt.Sprintf("%v", flow)
	if f != nil {
		ending = fmt.Sprintf("%s at %d:%d: %s", f.Code, f.Row, f.Column, f.Message)
	}
	exact := make([]string, len(rec.seen))
	shaped := make([]string, len(rec.seen))
	for i, ev := range rec.seen {
		exact[i] = ev.JSON()
		shaped[i] = shapedPass(ev).JSON()
	}
	return ending, exact, shaped, value
}

// shapedPass is the pass with the lists in its tokens' container values
// emptied.
func shapedPass(ev ParserEvent) ParserEvent {
	if ev.Pass == nil {
		return ev
	}
	pass := *ev.Pass
	pass.Tokens = make([]PassToken, len(ev.Pass.Tokens))
	for i, tok := range ev.Pass.Tokens {
		tok.Val = emptiedDatum(tok.Val)
		pass.Tokens[i] = tok
	}
	return ParserEvent{Kind: ev.Kind, Pass: &pass}
}

// emptiedDatum is d with every list in it emptied, its maps' members kept.
func emptiedDatum(d Datum) Datum {
	switch d.Kind {
	case DatumArray:
		return Datum{Kind: DatumArray, Items: []Datum{}}
	case DatumObject:
		members := make([]Member, len(d.Members))
		for i, m := range d.Members {
			members[i] = Member{Key: m.Key, Value: emptiedDatum(m.Value)}
		}
		return Datum{Kind: DatumObject, Members: members}
	}
	return d
}

// pruneCompare says how the pruned run compares with the one that did not
// prune: "" the same, "shape" the same once the lists in the tokens'
// container values are set aside, and otherwise where they differ.
func pruneCompare(name, text string) string {
	wholeEnd, kept, keptShape, _ := prunedRun(name, text, false)
	prunedEnd, left, leftShape, _ := prunedRun(name, text, true)
	if wholeEnd != prunedEnd {
		return fmt.Sprintf("ended %s without pruning, %s pruned", wholeEnd, prunedEnd)
	}
	if strings.Join(kept, "\n") == strings.Join(left, "\n") {
		return ""
	}
	for i := 0; i < len(keptShape) || i < len(leftShape); i++ {
		if i >= len(keptShape) || i >= len(leftShape) || keptShape[i] != leftShape[i] {
			at := func(passes []string) string {
				if i < len(passes) {
					return passes[i]
				}
				return "nothing"
			}
			return fmt.Sprintf("pass %d of %d differs: %s without pruning, %s pruned", i, len(kept), at(kept), at(left))
		}
	}
	return "shape"
}

// TestEveryGrammarPrunesAsTheListSays holds the list in both directions: a
// listed grammar no document makes differ, and an unlisted grammar some
// document does.
func TestEveryGrammarPrunesAsTheListSays(t *testing.T) {
	var wrong []string
	for _, g := range pruneGrammars {
		docs := pruneDocuments(t, g)
		var found, shaped []string
		for i, doc := range docs {
			switch why := pruneCompare(g.name, doc.text); why {
			case "":
			case "shape":
				shaped = append(shaped, doc.name)
			default:
				found = append(found, doc.name+": "+why)
			}
			if (i+1)%200 == 0 || i+1 == len(docs) {
				t.Logf("%s: %d of %d (%d%%) documents, %d differing", g.name, i+1, len(docs), (i+1)*100/len(docs), len(found))
			}
		}
		if len(shaped) > 0 {
			t.Logf("%s: %d the same once the lists in their tokens' containers are set aside: %s",
				g.name, len(shaped), strings.Join(shaped, ", "))
		}
		switch listed := Prunable(g.name); {
		case listed && len(found) > 0:
			wrong = append(wrong, fmt.Sprintf("%s is in PrunableGrammars, but %d of its documents differ:\n    %s",
				g.name, len(found), strings.Join(found, "\n    ")))
		case !listed && len(found) == 0:
			wrong = append(wrong, fmt.Sprintf("%s is not in PrunableGrammars, but none of its documents differs", g.name))
		}
	}
	for _, name := range PrunableGrammars() {
		known := false
		for _, g := range pruneGrammars {
			known = known || g.name == name
		}
		if !known {
			wrong = append(wrong, name+" is listed, and this suite never runs it")
		}
	}
	if len(wrong) > 0 {
		t.Fatal(strings.Join(wrong, "\n"))
	}
}

// treeElements counts every element of every list in v, and every member
// of every map.
func treeElements(v any) int {
	switch v := v.(type) {
	case []any:
		n := len(v)
		for _, e := range v {
			n += treeElements(e)
		}
		return n
	case tabnas.ListRef:
		return treeElements(v.Val)
	case map[string]any:
		n := len(v)
		for _, e := range v {
			n += treeElements(e)
		}
		return n
	case *tabnas.OrderedMap:
		n := len(v.Keys)
		for _, e := range v.Vals {
			n += treeElements(e)
		}
		return n
	case tabnas.MapRef:
		return treeElements(v.Val)
	}
	return 0
}

// prunedTree is what the tree holds after a run of records records.
func prunedTree(t *testing.T, name, doc string, records int, prune bool) int {
	t.Helper()
	_, _, _, value := prunedRun(name, pruneGenerated(doc, records), prune)
	if value == nil {
		t.Fatalf("%s over %d records returned no value", name, records)
	}
	return treeElements(value)
}

// TestTenTimesTheRecordsLeaveThePrunedTreeAsItWas measures pruning for
// every grammar the hosts read records with: ten times the records leave
// the pruned tree as it was, while the tree a run that does not prune
// hands back grows with them.
func TestTenTimesTheRecordsLeaveThePrunedTreeAsItWas(t *testing.T) {
	pairs := [][2]string{
		{"json", "records.json"}, {"jsonc", "records.json"}, {"json5", "records.json"},
		{"jsonic", "records.json"}, {"jsonl", "records.jsonl"}, {"yaml", "records.yaml"},
		{"xml", "records.xml"}, {"csv", "records.csv"},
	}
	var wrong []string
	for i, pair := range pairs {
		name, doc := pair[0], pair[1]
		few, many := prunedTree(t, name, doc, 200, true), prunedTree(t, name, doc, 2000, true)
		wholeFew, wholeMany := prunedTree(t, name, doc, 200, false), prunedTree(t, name, doc, 2000, false)
		t.Logf("retention: %d of %d (%d%%) %s: pruned %d then %d elements, not pruned %d then %d",
			i+1, len(pairs), (i+1)*100/len(pairs), name, few, many, wholeFew, wholeMany)
		if few != many {
			wrong = append(wrong, fmt.Sprintf("%s: the pruned tree held %d elements after 200 records and %d after 2000", name, few, many))
		}
		if wholeMany < wholeFew+1800 {
			wrong = append(wrong, fmt.Sprintf("%s: the tree held %d then %d elements without pruning: is the document %s records?", name, wholeFew, wholeMany, doc))
		}
	}
	if len(wrong) > 0 {
		t.Fatal(strings.Join(wrong, "\n"))
	}
}

// TestARunThatPrunesNeedsAListedGrammar: the gate refuses an unnamed or
// unlisted grammar before the parse, sending nothing, and opens for a
// listed one; the suite's switch lifts it, and a run that does not prune
// needs no name.
func TestARunThatPrunesNeedsAListedGrammar(t *testing.T) {
	for _, name := range []string{"", "chess", "Json"} {
		source := NewParserEventSource(makeGrammar("json"), "[1]").Prune()
		if name != "" {
			source.Grammar(name)
		}
		var rec passRecorder
		_, f := source.Run(&rec)
		if f == nil || f.Code != CodeStreamabilityUnknown || !strings.Contains(f.Message, "PrunableGrammars") {
			t.Errorf("%q: %v", name, f)
		}
		if len(rec.seen) != 0 {
			t.Errorf("%q: %d passes sent", name, len(rec.seen))
		}
	}
	for i, source := range []*ParserEventSource{
		NewParserEventSource(makeGrammar("json"), "[1]").Grammar("json").Prune(),
		NewParserEventSource(makeGrammar("json"), "[1]").Prune().Unverified(),
		NewParserEventSource(makeGrammar("json"), "[1]").Grammar("chess"),
	} {
		var rec passRecorder
		if _, f := source.Run(&rec); f != nil {
			t.Errorf("%d: %v", i, f)
		}
		if n := len(rec.seen); n == 0 || rec.seen[n-1].Kind != ParserEnd {
			t.Errorf("%d: %d passes, no End", i, n)
		}
	}
}

// TestPruningEmptiesTheTreesListsAndLeavesThePasses, on one JSON document:
// the passes are the same, every list in the tree is emptied, and the
// maps keep their members.
func TestPruningEmptiesTheTreesListsAndLeavesThePasses(t *testing.T) {
	src := `{"a":[1,2,3],"b":[[4],[5,{"c":[6]}]],"d":{"e":"f"}}`
	_, kept, _, whole := prunedRun("json", src, false)
	_, left, _, pruned := prunedRun("json", src, true)
	if strings.Join(kept, "\n") != strings.Join(left, "\n") {
		t.Fatalf("the passes differ:\n%s\n%s", strings.Join(kept, "\n"), strings.Join(left, "\n"))
	}
	// The three members, each list empty, and d's one member.
	if n := treeElements(pruned); n != 4 {
		t.Errorf("the pruned tree holds %d elements: %v", n, pruned)
	}
	if n := treeElements(whole); n != 14 {
		t.Errorf("the tree without pruning holds %d elements: %v", n, whole)
	}
}
