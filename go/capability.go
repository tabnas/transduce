// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

// Which grammars the incremental source is verified for.
//
// Incremental streaming is a per-grammar VERIFIED capability, never an
// assumption, and it is earned per runtime: incremental_test.go runs
// every fixture of every grammar module the tests depend on through both
// ModeIncremental and ModeMaterialize and compares the recordings. A
// grammar is listed only when no fixture produced a completed stream
// that disagrees with the walk, and the test asserts the list in both
// directions, so it can rot in neither. A grammar not listed still works
// through ModeMaterialize and ValueSource; it just retains the whole
// value, and ParserSource refuses to run it incrementally.
//
// The adapter needs the engine's node-cell identity, built only with the
// tabnas_nodecell tag (see the package documentation); without it the
// list is empty.

// IncrementalGrammars is the grammars whose incremental events never
// contradict the whole-value walk on any fixture, by the name their
// module uses (github.com/tabnas/<name>/go). The slice is a copy.
func IncrementalGrammars() []string {
	return append([]string(nil), incrementalGrammars...)
}

// Incremental reports whether grammar may be run with ModeIncremental.
func Incremental(grammar string) bool {
	for _, name := range incrementalGrammars {
		if name == grammar {
			return true
		}
	}
	return false
}

// AdapterBuilt reports whether this build has the rule-event adapter
// (the tabnas_nodecell build tag).
func AdapterBuilt() bool { return adapterBuilt }
