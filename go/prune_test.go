// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

// The differential suite behind PrunableGrammars and PrunesMapsGrammars,
// ported from rs/tests/prune_test.rs.
//
// For every grammar module the tests depend on and every document the
// suite has for it, a ParserEventSource run that prunes must relay the
// same passes as one that does not, pass for pass in the fixture
// encoding, and end the same way: both complete, or both fail with the
// same code, position and message. The suite runs each document twice
// pruned, with the grammar's maps left and with them emptied
// (UnverifiedMaps), and holds each list to its runs. One difference short
// of identity is the contract, and the suite counts it apart: a token
// whose value is a container of the tree carries what the tree holds when
// its pass is made, so the lists in it may have been emptied, and its maps
// too in the run that empties them, while its kind is as it was (a yaml
// alias, which the lexer resolves to the anchored node). The documents are
// the Rust crate's fixtures, offered to every grammar, the generated
// worked-example documents of each grammar's family, and every document of
// the grammar's own corpus, the test/spec/*.tsv rows of the sibling
// checkout. A grammar is listed only when no document differs, and the
// suite asserts both lists in both directions. The second half measures
// what pruning is for: ten times the records leave the pruned tree as it
// was, and so do ten times an INI document's sections, a JSON object's
// members, a toml document's array of tables, a stylesheet's rules and a
// grammar's rules in ABNF, EBNF and GBNF. A PGN database's games stay, in
// a typed value pruning does not empty (DIVERGENCE.md 8).

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	tabnaschess "github.com/tabnas/chess/go"
	tabnas "github.com/tabnas/parser/go"
	support "github.com/tabnas/support/go"
)

// The generated documents' records in the differential: as many shapes as
// 2,000 would hold, at a tenth of the parsing.
const pruneRecords = 200

// pruning is how a run prunes: not at all, the lists, or the lists and the
// maps, whatever the grammar.
type pruning int

const (
	pruneNo pruning = iota
	pruneLists
	pruneMaps
)

type pruneGrammar struct {
	name      string
	generated []string
	repo      string
}

var pruneGrammars = []pruneGrammar{
	{"json", []string{"records.json", "members.json"}, "json"},
	{"jsonl", []string{"records.jsonl"}, "jsonl"},
	{"jsonic", []string{"records.json"}, "jsonic"},
	{"jsonc", []string{"records.json"}, "jsonc"},
	{"json5", []string{"records.json"}, "json5"},
	{"yaml", []string{"records.yaml"}, "yaml"},
	{"toml", []string{"tables.toml"}, "toml"},
	{"ini", []string{"sections.ini"}, "ini"},
	{"csv", []string{"records.csv"}, "csv"},
	{"xml", []string{"records.xml"}, "xml"},
	{"zon", nil, "zon"},
	{"markdown", []string{"paragraphs.md"}, "markdown"},
	{"feed", nil, "feed"},
	{"pgn", []string{"records.pgn"}, "chess"},
	{"css", []string{"rules.css"}, "css"},
	{"abnf", []string{"rules.abnf"}, "abnf"},
	{"ebnf", []string{"rules.ebnf"}, "ebnf"},
	{"gbnf", []string{"rules.gbnf"}, "gbnf"},
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
	case "members.json":
		return membersJSON(records)
	case "sections.ini":
		return sectionsINI(records)
	case "tables.toml":
		return tablesTOML(records)
	case "paragraphs.md":
		return paragraphsMD(records)
	case "records.pgn":
		return recordsPGN(records)
	case "rules.css":
		return rulesCSS(records)
	case "rules.abnf":
		return rulesABNF(records)
	case "rules.ebnf":
		return rulesEBNF(records)
	case "rules.gbnf":
		return rulesGBNF(records)
	}
	panic("no generator for " + name)
}

// rulesCSS is a stylesheet of records rules, each of two declarations.
func rulesCSS(records int) string {
	var b strings.Builder
	for i := 0; i < records; i++ {
		fmt.Fprintf(&b, ".c%d { margin: %dpx; content: \"the text of rule %d\"; }\n", i, i, i)
	}
	return b.String()
}

// rulesABNF is an ABNF grammar of records rules, each of two alternatives,
// its lines ended as RFC 5234 ends them.
func rulesABNF(records int) string {
	var b strings.Builder
	for i := 0; i < records; i++ {
		fmt.Fprintf(&b, "r%d = \"a%d the text of rule %d\" / \"b%d\"\r\n", i, i, i, i)
	}
	return b.String()
}

// rulesEBNF is an EBNF grammar of records rules, each of two alternatives.
func rulesEBNF(records int) string {
	var b strings.Builder
	for i := 0; i < records; i++ {
		fmt.Fprintf(&b, "r%d ::= \"a%d the text of rule %d\" | \"b%d\"\n", i, i, i, i)
	}
	return b.String()
}

// rulesGBNF is a GBNF grammar of records rules, each of two alternatives,
// after the root that starts it.
func rulesGBNF(records int) string {
	var b strings.Builder
	b.WriteString("root ::= r0\n")
	for i := 0; i < records; i++ {
		fmt.Fprintf(&b, "r%d ::= \"a%d the text of rule %d\" | \"b%d\"\n", i, i, i, i)
	}
	return b.String()
}

// membersJSON is a JSON object of records members, each an object of three.
func membersJSON(records int) string {
	members := make([]string, records)
	for i := range members {
		members[i] = fmt.Sprintf(`"k%d": {"id": %d, "name": "n%d", "tags": ["a", "b"]}`, i, i, i)
	}
	return "{\n" + strings.Join(members, ",\n") + "\n}\n"
}

// sectionsINI is an INI document of records sections, each of three keys.
func sectionsINI(records int) string {
	var b strings.Builder
	for i := 0; i < records; i++ {
		fmt.Fprintf(&b, "[s%d]\nid = %d\nname = n%d\ntext = the text of section %d\n\n", i, i, i, i)
	}
	return b.String()
}

// paragraphsMD is a markdown document of records paragraphs.
func paragraphsMD(records int) string {
	var b strings.Builder
	for i := 0; i < records; i++ {
		fmt.Fprintf(&b, "Paragraph %d. The text of paragraph %d, *with* some `code`.\n\n", i, i)
	}
	return b.String()
}

// tablesTOML is a toml array of records tables, each of three keys.
func tablesTOML(records int) string {
	var b strings.Builder
	for i := 0; i < records; i++ {
		fmt.Fprintf(&b, "[[items]]\nid = %d\nname = \"n%d\"\ntext = \"the text of table %d\"\n\n", i, i, i)
	}
	return b.String()
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
// and shaped as a run pruning as shape would leave them, and the engine's
// tree afterwards. A run that prunes a grammar BoundsHistory lists bounds
// the rule history, as a host's does.
func prunedRun(name, text string, how, shape pruning) (string, []string, []string, any) {
	source := NewParserEventSource(makeGrammar(name), text)
	switch how {
	case pruneLists:
		source.Prune().Unverified()
	case pruneMaps:
		source.Prune().UnverifiedMaps()
	}
	if how != pruneNo && BoundsHistory(name) {
		source.UnverifiedHistory()
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
		shaped[i] = shapedPass(ev, shape == pruneMaps).JSON()
	}
	return ending, exact, shaped, value
}

// shapedPass is the pass with the lists in its tokens' container values
// emptied, and the maps when maps is set.
func shapedPass(ev ParserEvent, maps bool) ParserEvent {
	if ev.Pass == nil {
		return ev
	}
	pass := *ev.Pass
	pass.Tokens = make([]PassToken, len(ev.Pass.Tokens))
	for i, tok := range ev.Pass.Tokens {
		tok.Val = emptiedDatum(tok.Val, maps)
		pass.Tokens[i] = tok
	}
	return ParserEvent{Kind: ev.Kind, Pass: &pass}
}

// emptiedDatum is d with every list in it emptied, and every map when maps
// is set; a map that is kept keeps its members, their own containers
// emptied.
func emptiedDatum(d Datum, maps bool) Datum {
	switch d.Kind {
	case DatumArray:
		return Datum{Kind: DatumArray, Items: []Datum{}}
	case DatumObject:
		if maps {
			return Datum{Kind: DatumObject, Members: []Member{}}
		}
		members := make([]Member, len(d.Members))
		for i, m := range d.Members {
			members[i] = Member{Key: m.Key, Value: emptiedDatum(m.Value, maps)}
		}
		return Datum{Kind: DatumObject, Members: members}
	}
	return d
}

// pruneCompare says how the pruned run compares with the one that did not
// prune: "" the same, "shape" the same once the lists in the tokens'
// container values are set aside, and otherwise where they differ.
func pruneCompare(name, text string, how pruning) string {
	wholeEnd, kept, keptShape, _ := prunedRun(name, text, pruneNo, how)
	prunedEnd, left, leftShape, _ := prunedRun(name, text, how, how)
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

// pruneDifferences is g's documents pruned as how says, compared with
// unpruned runs: the differences.
func pruneDifferences(t *testing.T, g pruneGrammar, docs []prunedDoc, how pruning) []string {
	what := "pruned"
	if how == pruneMaps {
		what = "with its maps emptied"
	}
	var found, shaped []string
	for i, doc := range docs {
		switch why := pruneCompare(g.name, doc.text, how); why {
		case "":
		case "shape":
			shaped = append(shaped, doc.name)
		default:
			found = append(found, doc.name+": "+why)
		}
		if (i+1)%200 == 0 || i+1 == len(docs) {
			t.Logf("%s %s: %d of %d (%d%%) documents, %d differing", g.name, what, i+1, len(docs), (i+1)*100/len(docs), len(found))
		}
	}
	if len(shaped) > 0 {
		t.Logf("%s %s: %d the same once the containers in their tokens' values are set aside: %s",
			g.name, what, len(shaped), strings.Join(shaped, ", "))
	}
	return found
}

// TestEveryGrammarPrunesAsTheListSays holds both lists in both directions:
// a listed grammar no document makes differ, and an unlisted grammar some
// document does. A grammar that is not prunable is not run with its maps
// emptied, and is in neither.
func TestEveryGrammarPrunesAsTheListSays(t *testing.T) {
	var wrong []string
	for _, g := range pruneGrammars {
		docs := pruneDocuments(t, g)
		found := pruneDifferences(t, g, docs, pruneLists)
		listed := Prunable(g.name)
		switch {
		case listed && len(found) > 0:
			wrong = append(wrong, fmt.Sprintf("%s is in PrunableGrammars, but %d of its documents differ:\n    %s",
				g.name, len(found), strings.Join(found, "\n    ")))
		case !listed && len(found) == 0:
			wrong = append(wrong, fmt.Sprintf("%s is not in PrunableGrammars, but none of its documents differs", g.name))
		}
		if !listed {
			if PrunesMaps(g.name) {
				wrong = append(wrong, g.name+" is in PrunesMapsGrammars, and not prunable")
			}
			continue
		}
		found = pruneDifferences(t, g, docs, pruneMaps)
		switch listedMaps := PrunesMaps(g.name); {
		case listedMaps && len(found) > 0:
			wrong = append(wrong, fmt.Sprintf("%s is in PrunesMapsGrammars, but %d of its documents differ with its "+
				"maps emptied:\n    %s", g.name, len(found), strings.Join(found, "\n    ")))
		case !listedMaps && len(found) == 0:
			wrong = append(wrong, fmt.Sprintf("%s is not in PrunesMapsGrammars, but none of its documents differs "+
				"with its maps emptied", g.name))
		case !listedMaps:
			t.Logf("%s with its maps emptied: %d documents differ, the first %s", g.name, len(found), found[0])
		}
	}
	for _, name := range append(PrunableGrammars(), PrunesMapsGrammars()...) {
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
	case *[]any:
		if v == nil {
			return 0
		}
		return treeElements(*v)
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

// heldElements is what a tree keeps reachable, as treeElements counts it
// but each list to its capacity: a truncated slice keeps its backing
// array, and with it whatever that array still holds.
func heldElements(v any) int {
	switch v := v.(type) {
	case []any:
		n := 0
		for _, e := range v[:cap(v)] {
			if e != nil {
				n++
			}
			n += heldElements(e)
		}
		return n
	case tabnas.ListRef:
		return heldElements(v.Val)
	case *[]any:
		if v == nil {
			return 0
		}
		return heldElements(*v)
	case map[string]any:
		n := len(v)
		for _, e := range v {
			n += heldElements(e)
		}
		return n
	case *tabnas.OrderedMap:
		n := len(v.Keys)
		for _, e := range v.Vals {
			n += heldElements(e)
		}
		return n
	case tabnas.MapRef:
		return heldElements(v.Val)
	}
	return 0
}

// prunedTree is what the tree keeps reachable after a run of records
// records (heldElements), pruned as a host prunes, by the grammar's name and
// its verified lists, or not.
func prunedTree(t *testing.T, name, doc string, records int, prune bool) int {
	t.Helper()
	source := NewParserEventSource(makeGrammar(name), pruneGenerated(doc, records)).Grammar(name)
	if prune {
		source.Prune()
	}
	var rec passRecorder
	_, f, value := source.RunWithValue(&rec)
	if f != nil {
		t.Fatalf("%s over %d records: %v", name, records, f)
	}
	if value == nil {
		t.Fatalf("%s over %d records returned no value", name, records)
	}
	return heldElements(value)
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
		{"json", "members.json"}, {"ini", "sections.ini"}, {"toml", "tables.toml"},
		{"markdown", "paragraphs.md"},
		{"css", "rules.css"}, {"abnf", "rules.abnf"}, {"ebnf", "rules.ebnf"}, {"gbnf", "rules.gbnf"},
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

// TestAPrunedPGNRunKeepsEveryGame pins DIVERGENCE.md entry 8. chess's Go
// plugin keeps its database as a typed *tabnaschess.Database, a slice of
// *Game, and pruning empties the engine's own lists and maps, so a pruned
// run of a PGN database keeps every game, where Rust's and TypeScript's
// sources empty the database behind the stream. Once the plugin keeps its
// games where pruning reaches them, this test fails: delete it and the
// entry, and add pgn's records to
// TestTenTimesTheRecordsLeaveThePrunedTreeAsItWas.
func TestAPrunedPGNRunKeepsEveryGame(t *testing.T) {
	for _, games := range []int{200, 2000} {
		source := NewParserEventSource(makeGrammar("pgn"), pruneGenerated("records.pgn", games)).Grammar("pgn").Prune()
		var rec passRecorder
		_, f, value := source.RunWithValue(&rec)
		if f != nil {
			t.Fatalf("%d games: %v", games, f)
		}
		db, ok := value.(*tabnaschess.Database)
		if !ok || db == nil {
			t.Fatalf("%d games: the value is %T, not chess's database", games, value)
		}
		kept := 0
		for _, game := range *db {
			if game != nil {
				kept++
			}
		}
		t.Logf("pgn: %d games pruned, %d kept", games, kept)
		if kept != games {
			t.Errorf("a pruned run of %d games kept %d of them: DIVERGENCE.md entry 8 may have closed", games, kept)
		}
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
	_, kept, _, whole := prunedRun("json", src, pruneNo, pruneNo)
	_, left, _, pruned := prunedRun("json", src, pruneLists, pruneLists)
	if strings.Join(kept, "\n") != strings.Join(left, "\n") {
		t.Fatalf("the passes differ:\n%s\n%s", strings.Join(kept, "\n"), strings.Join(left, "\n"))
	}
	// The three members, each list empty, and d's one member.
	if n := treeElements(pruned); n != 4 {
		t.Errorf("the pruned tree holds %d elements: %v", n, pruned)
	}
	// Nor does a list's backing array keep what the list held.
	if n := heldElements(pruned); n != 4 {
		t.Errorf("the pruned tree keeps %d elements reachable: %v", n, pruned)
	}
	if n := treeElements(whole); n != 14 {
		t.Errorf("the tree without pruning holds %d elements: %v", n, whole)
	}
}

// TestPruningAGrammarWhoseMapsAreEmptiedEmptiesThem: a grammar PrunesMaps
// lists has its maps emptied with its lists, and its passes are the same;
// toml, which reads its maps back, keeps its tables' keys.
func TestPruningAGrammarWhoseMapsAreEmptiedEmptiesThem(t *testing.T) {
	src := `{"a":[1,2,3],"b":[[4],[5,{"c":[6]}]],"d":{"e":"f"}}`
	_, kept, _, _ := prunedRun("json", src, pruneNo, pruneNo)
	var rec passRecorder
	_, f, value := NewParserEventSource(makeGrammar("json"), src).Grammar("json").Prune().RunWithValue(&rec)
	if f != nil {
		t.Fatal(f)
	}
	left := make([]string, len(rec.seen))
	for i, ev := range rec.seen {
		left[i] = ev.JSON()
	}
	if strings.Join(kept, "\n") != strings.Join(left, "\n") {
		t.Fatalf("the passes differ:\n%s\n%s", strings.Join(kept, "\n"), strings.Join(left, "\n"))
	}
	if n := heldElements(value); n != 0 {
		t.Errorf("the pruned tree keeps %d elements reachable: %v", n, value)
	}
	var toml passRecorder
	_, f, value = NewParserEventSource(makeGrammar("toml"), "[a]\nb = 1\n").Grammar("toml").Prune().RunWithValue(&toml)
	if f != nil {
		t.Fatal(f)
	}
	if n := heldElements(value); n != 2 {
		t.Errorf("the pruned toml tree keeps %d elements reachable, not the table and its key: %v", n, value)
	}
}

// TestAHeldPruningEmptiesNoList: a grammar holds the pruning off with a
// count above zero under PruneHold in the parse context's bag (ctx.U), as
// tabnas/yaml does while a value with an anchor is open. While it is held
// the source empties no list, and a count of zero holds nothing.
func TestAHeldPruningEmptiesNoList(t *testing.T) {
	src := `{"a":[1,2,3],"b":[[4],[5,{"c":[6]}]],"d":{"e":"f"}}`
	held := func(count int) ([]string, any) {
		parser := makeGrammar("json")
		parser.SetOptions(tabnas.Options{Parse: &tabnas.ParseOptions{
			Prepare: map[string]func(ctx *tabnas.Context){
				"hold": func(ctx *tabnas.Context) {
					if ctx.U == nil {
						ctx.U = map[string]any{}
					}
					ctx.U[PruneHold] = count
				},
			},
		}})
		source := NewParserEventSource(parser, src).Prune().Unverified()
		var rec passRecorder
		flow, f, value := source.RunWithValue(&rec)
		if f != nil || flow != Continue {
			t.Fatalf("a hold of %d: %v %v", count, flow, f)
		}
		exact := make([]string, len(rec.seen))
		for i, ev := range rec.seen {
			exact[i] = ev.JSON()
		}
		return exact, value
	}
	_, kept, _, _ := prunedRun("json", src, pruneNo, pruneNo)
	passes, one := held(1)
	if strings.Join(kept, "\n") != strings.Join(passes, "\n") {
		t.Fatalf("the passes differ:\n%s\n%s", strings.Join(kept, "\n"), strings.Join(passes, "\n"))
	}
	if n := treeElements(one); n != 14 {
		t.Errorf("a hold of 1 left the tree holding %d elements, not 14: %v", n, one)
	}
	if _, zero := held(0); treeElements(zero) != 4 {
		t.Errorf("a hold of 0 left the tree holding %d elements, not 4: %v", treeElements(zero), zero)
	}
}
