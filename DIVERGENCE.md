# Divergences

Where a runtime of this crate produces a **different result for the same
input**, and why that difference is allowed to stand. Three runtimes run
the shared fixtures in [`test/spec/`](test/spec/), which
[`docs/reference.md`](docs/reference.md) ("Shared fixtures") describes:
Rust in [`rs/`](rs/), TypeScript in [`ts/`](ts/) and Go in [`go/`](go/).
Every runtime runs every row: TypeScript agrees with Rust on all of them,
and Go skips the rows `specDivergences` in `go/spec_test.go` names, each
a difference in a Go grammar module, measured against Rust. Every entry
here is re-measured by running its input through each runtime, and an
entry that closes is deleted together with the test that pins it.

Entry 4 is measured: a verified list that differs, because one runtime's
suite cannot earn an entry the others' do. Entry 5 is measured too: what a
Go stream holds, which an engine defect keeps from being bounded. The rest is what the shared
fixtures deliberately leave out, because it belongs to the Rust runtime
or is not yet decided, so that a port does not read the absence of a row
as coverage, or as licence.

## Where these are pinned

Each entry names the test that pins it: Rust's, or the ports' where Rust
has retired the code. None is a fixture
row: a row says what every runtime must do, and these are the things that
are not, or not yet, the same everywhere.

## 1. tabnas-json refuses nesting past 128 with a guard of its own

| input | Rust |
|---|---|
| 200 `[` then `1` then 200 `]`, `json`, walked or through its part | `INPUT_INVALID` at 1:128, the message naming the grammar's guard |

**A grammar's behaviour, not this crate's.** The Rust `tabnas-json`
installs a parse guard that cancels a parse nested deeper than 128,
below `Limits::max_depth` (256), so `max_depth` is unreachable for JSON
in Rust unless it is set lower. This crate maps the grammar's cancel to
`INPUT_INVALID` rather than `ABORTED`, and that mapping is shared (a
port does the same with whatever guard its grammar installs); the depth
at which JSON's guard fires is the grammar's, and the `json` repository
owns its parity. `limits.tsv` therefore trips `max_depth` with small
limits only. Pinned by
`a_grammars_own_guard_is_invalid_input_that_names_the_grammar` in
`rs/src/source/parser.rs`.

## 2. A number without a lexeme, written as text

| value (no lexeme) | Rust | ECMAScript `String(x)` |
|---|---|---|
| `1e21` | `1000000000000000000000` | `1e+21` |
| `1e-7` | `0.0000001` | `1e-7` |

**Open; to be decided when the first port lands.** `write_json_number`,
and with it a `Datum`'s compact JSON and a container cell's text
(`Cell::from_datum`), writes a number that has no lexeme with Rust's
`f64` `Display`, which never uses an exponent. Inside
`1e-6 <= |x| < 1e21` that is the shortest round-trip text every runtime
writes; outside it the runtimes differ. Only the walk (`materialize`,
`value`) emits numbers without lexemes, and only a
container projected into a table cell turns one into text here. No
fixture row does that with a number outside the range. The choice is
whether the contract is ECMAScript's `Number::toString` (Rust and Go
change) or this crate's text (the ports change); until it is made, the
row cannot be written. Pinned on the Rust side, for the numbers inside
the range, by `cells_print_as_json` and `lexemes_survive` in alchemy's
`rs/src/shared/table.rs` and `rs/src/shared/datum.rs`, where the shared
types and `write_json_number` live.

## 3. Rust-only harness and tooling

Not behaviour, listed so their absence elsewhere is not mistaken for a
gap:

- **The criterion benches** (`rs/benches/throughput.rs`, `docs/BENCH.md`)
  measure the Rust implementation. A port measures itself its own way.
- **What a TSV cell cannot carry** stays in each runtime's own tests: a
  reader that fails or gives bytes that are not UTF-8, and a reader that
  hands out a few bytes at a time (`rs/tests/reader_test.rs`,
  `ts/test/reader.test.ts`, `go/reader_test.go`), the abort flag, a sink
  that stops or fails part-way, and the metrics. They are API shapes (a
  borrowed or an owned sink, a reader of each runtime's kind), not
  results a row can name.

## 4. Go does not prune PGN

`ParserEventSource`'s pruning is a verified capability, and Go's list
(`PrunableGrammars`, `go/capability.go`) omits `pgn`, which Rust's and
TypeScript's (`capability::PRUNABLE`, `ts/src/capability.ts`) name. So a
Go run that prunes a PGN database is refused before the parse with
`STREAMABILITY_UNKNOWN`, as for every unlisted grammar, and a Go host
reads the database through tabnas-chess's part unpruned, its tree holding
every game; Rust and TypeScript empty the database behind the stream.

Why: the Rust and TypeScript sides take tabnas-chess as a dev-dependency
(on the maintainer's instruction of 2026-10-14), and their prune suites
read its documents. Go takes none and runs no PGN, so its list cannot
name it: the suite asserts the list in both directions, and a name it
does not run fails it. (Until step 13 of alchemy's streaming plan this
entry was the incremental source's list, which named `pgn` in TypeScript
and not in Go; every port has retired that list.)

Pinned in Go by `TestEveryGrammarPrunesAsTheListSays` (`go/prune_test.go`),
and in TypeScript by the prune suite's `pgn` case
(`ts/test/prune.test.ts`).

## 5. Go's source leaves the rule history unbounded

A TypeScript `ParserEventSource` bounds the engine's rule history at
three predecessors before its parse (`STREAM_RULE_HISTORY`), where the
grammar or the host left it unbounded, and the Rust engine bounds it at
three by default; a Go `ParserEventSource` bounds nothing. The values and
the passes are the same in every runtime. What differs is what a parse
holds: a Go stream keeps every rule it made reachable until the document
ends, so its memory grows with the document's length, pruned or not
(100,000 JSON records through dmog's Go `translate`: 2.8 GB, against
276 MB at 10,000).

Why: a bounded link in the Go engine is a copy of the rule, and a Go
rule's `Node` is a field a copy does not share, so a grammar's write
through a link (`r.Parent.Node = r.Node` and `r.Prev.Node = r.Node` in
jsonic's Go grammar, `r.Parent.Node = owner.Node` in the engine's own
builtins) lands on the copy and is lost: bounded at three, the Go prune suite's unpruned trees of
jsonc, json5, jsonic, yaml and xml held 6 elements at ten times the
records. The engine's to repair (tabnas/parser#325); this entry closes,
and the Go source bounds the history as TypeScript's does, when a bounded
Go history keeps those writes.

Pinned in TypeScript by `ts/test/history.test.ts`. Go has no test of a
bound it does not set.
