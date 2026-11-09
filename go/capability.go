// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

// Which grammars a ParserEventSource may prune behind its stream
// (rs/src/source/capability.rs): a verified capability, never an
// assumption, earned per runtime. prune_test.go runs every document of
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
