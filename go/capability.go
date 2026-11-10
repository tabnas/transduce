// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

// Which grammars a ParserEventSource may prune behind its stream, and which
// of them have their maps emptied as well as their lists
// (rs/src/source/capability.rs): verified capabilities, never assumptions,
// earned per runtime. prune_test.go runs every document of
// every grammar module the tests depend on pruned and not, compares the
// passes and the outcomes, and asserts the list in both directions, so it
// can rot in neither.
//
// The incremental list, the grammars whose rule events the inference in
// rule_events.go turned into a document's events, is gone with the
// inference (alchemy's streaming plan, step 13, in Rust first and then in
// this port): every format on it has a transducer part now, or reads whole
// as markdown declares, and a part makes the events.

// prunableGrammars are the grammars prune_test.go verified in this
// runtime: their passes are the same whether ParserEventSource prunes or
// not, on every document the suite reads them. Go's omits pgn, which its
// suite does not run (DIVERGENCE.md 4).
var prunableGrammars = []string{
	"csv", "feed", "ini", "json", "json5", "jsonc", "jsonic", "jsonl", "markdown", "toml", "xml", "yaml", "zon",
}

// PrunableGrammars is the grammars a ParserEventSource may prune, by the
// name of the format each reads: a copy of the list, which the caller may
// keep.
func PrunableGrammars() []string {
	return append([]string(nil), prunableGrammars...)
}

// prunesMapsGrammars are the grammars whose maps a pruning ParserEventSource
// empties too, at a close pass whose rule's node is the map, as it empties
// lists: prune_test.go verified in this runtime that their passes are the
// same with their maps emptied, on every document the suite reads them, and
// asserts the list in both directions. Every one is in prunableGrammars.
// The grammars it leaves out read their maps back during the parse: yaml,
// toml, xml, zon and feed find a key or a table defined twice, or a name to
// merge or resolve, by looking it up in the tree they are building, so with
// their maps emptied they pass documents they refuse, or refuse documents
// they pass.
var prunesMapsGrammars = []string{
	"csv", "ini", "json", "json5", "jsonc", "jsonic", "jsonl", "markdown",
}

// PrunesMapsGrammars is the grammars whose maps a pruning ParserEventSource
// empties as well as their lists: a copy of the list, which the caller may
// keep.
func PrunesMapsGrammars() []string {
	return append([]string(nil), prunesMapsGrammars...)
}

// PrunesMaps reports whether grammar's maps are emptied behind a pruning
// ParserEventSource's stream as well as its lists.
func PrunesMaps(grammar string) bool {
	for _, name := range prunesMapsGrammars {
		if name == grammar {
			return true
		}
	}
	return false
}

// Prunable reports whether a ParserEventSource may empty grammar's lists
// behind its stream (ParserEventSource.Prune).
func Prunable(grammar string) bool {
	for _, name := range prunableGrammars {
		if name == grammar {
			return true
		}
	}
	return false
}

// boundsHistoryGrammars are the grammars a pruning ParserEventSource bounds
// the engine's rule history for (StreamRuleHistory): history_test.go
// verified in this runtime that every document the suite reads them parses
// alike with the history bounded and without, the same passes, the same
// ending and the same value, and asserts the list in both directions.
// Every one is in prunableGrammars, and every grammar the suite runs is on
// it. A grammar off it would keep the engine's default, an unbounded
// history, because what it reads back through the rule links reaches
// further than the bound keeps.
var boundsHistoryGrammars = []string{
	"csv", "feed", "ini", "json", "json5", "jsonc", "jsonic", "jsonl", "markdown", "toml", "xml", "yaml", "zon",
}

// BoundsHistoryGrammars is the grammars a pruning ParserEventSource bounds
// the rule history for: a copy of the list, which the caller may keep.
func BoundsHistoryGrammars() []string {
	return append([]string(nil), boundsHistoryGrammars...)
}

// BoundsHistory reports whether a pruning ParserEventSource bounds the
// engine's rule history for grammar.
func BoundsHistory(grammar string) bool {
	for _, name := range boundsHistoryGrammars {
		if name == grammar {
			return true
		}
	}
	return false
}
