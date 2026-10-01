// Copyright (c) 2026 tabnas, MIT License

// Package tabnastransduce is the Go port of tabnas-transduce: streaming
// transducers over any tabnas parser.
//
// A transducer decides what a source contributes to an output model; a
// renderer decides how that model becomes text. This package is the
// transducer side:
//
//   - the source protocol [Event] (`JsonEvents/1`) and the push boundary
//     [Sink];
//   - sources that produce it from a parsed value ([ValueSource]), from a
//     tabnas parse ([ParserSource]) and from line-delimited input
//     ([LinesSource]);
//   - [Selector]s and the [Matcher] and [Router] that recognize selected
//     scopes in one pass and materialize them under a byte limit;
//   - the table protocol [TableEvent] (`TableRows/1`) and the standard
//     metadata-first table transducer [TableFromJSON];
//   - [Limits], [Metrics], [AbortFlag] and the stable failure [Code]s
//     every stage reports.
//
// Everything is synchronous and push-based: the parser calls the first
// sink from inside its own callback, and a slow writer at the end of the
// chain slows the parse at the start. That is the backpressure.
//
// The Rust crate in ../rs is the reference; docs/reference.md describes
// the contracts, and the shared fixtures in ../test/spec pin them for
// every runtime.
//
// # The incremental source in this build
//
// ParserSource's SourceMode Incremental (and LinesSource's JSON Lines
// RunIncremental) turns the engine's rule events into source events as
// the parse proceeds. It needs the engine's node-cell identity
// (tabnas.Rule.NodeCell and SetNode), which a released Go engine does not
// have yet, so the adapter is compiled only with the build tag
// `tabnas_nodecell`. Without the tag the verified list
// ([IncrementalGrammars]) is empty and every incremental run is refused
// with STREAMABILITY_UNKNOWN before the parse, exactly as an unlisted
// grammar is refused; Materialize, ValueSource and the walking line
// sources are complete in either build.
package tabnastransduce

// VERSION is this module's version. It must equal rs/Cargo.toml's
// [package] version; version_test.go fails the build when they drift.
const VERSION = "0.1.0"
