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
//   - sources that produce it from a parsed value ([ValueSource]) and from
//     a tabnas parse, its value walked ([ParserSource]), and the source of
//     the parser's rule passes ([ParserEventSource], `ParserEvents/1`),
//     what a transducer part reads, from a text or from a reader
//     ([ParserEventSourceFromReader]), from which a format streams (the
//     part is alchemy's text, which alchemy's runtime runs over them);
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
// The protocol types (the events and the Sink, the table protocol,
// selectors, retained values, limits, metrics, the abort flag and the
// codes) are declared in alchemy's shared package,
// github.com/tabnas/alchemy/go/shared, which render and alchemy build on
// too; this package names each again, as an alias, so its API is
// unchanged. [Routers] is the stages alchemy's runtime builds, as
// alchemy's shared.Routers, which a host hands to alchemy's Compile.
//
// The Rust crate in ../rs is the reference; docs/reference.md describes
// the contracts, and the shared fixtures in ../test/spec pin them for
// every runtime.
//
// The rule-event inference that ParserSource's incremental mode ran, and
// the line sources that read JSON Lines and CSV a record or a chunk at a
// time, were retired at alchemy's streaming plan's steps 12 and 13, in
// Rust first and then in this port. The inference was compiled only with
// the build tag `tabnas_nodecell`; nothing in the package needs the tag
// now, and a build with it is the same as one without. ParserEventSource's
// pruning uses the engine's node-cell identity (tabnas.Rule.NodeCell and
// SetNode), which is in every build of the engine since v0.12.10.
package tabnastransduce

// VERSION is this module's version. It must equal rs/Cargo.toml's
// [package] version; version_test.go fails the build when they drift.
const VERSION = "0.6.2"
