// Copyright (c) 2026 tabnas, MIT License

//go:build !tabnas_nodecell

package tabnastransduce

// runIncremental is never reached without the adapter: the gate refuses
// every incremental run first. It refuses again rather than walk, so a
// caller that bypassed the gate cannot take a walk for a stream.
func runIncremental(p *ParserSource, _ Sink) (Flow, *Fail, any) {
	return Continue, p.gate(), nil
}

// jsonlIncremental is LinesSource.RunIncremental's JSON Lines path,
// refused before anything is read in a build without the adapter.
func jsonlIncremental(_ *LinesSource, _ Sink) (Flow, *Fail) {
	return Continue, unverifiedLines()
}
