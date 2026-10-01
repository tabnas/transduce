// Copyright (c) 2026 tabnas, MIT License

//go:build tabnas_nodecell

package tabnastransduce

// The grammars incremental_test.go verified in this runtime, with the
// adapter built over the engine's node-cell identity.
var incrementalGrammars = []string{
	"json", "json5", "jsonc", "jsonic", "jsonl", "markdown", "yaml", "zon",
}

const adapterBuilt = true
