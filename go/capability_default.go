// Copyright (c) 2026 tabnas, MIT License

//go:build !tabnas_nodecell

package tabnastransduce

// Without the engine's node-cell identity there is no adapter, so no
// grammar is verified and every incremental run is refused before the
// parse.
var incrementalGrammars = []string{}

const adapterBuilt = false
