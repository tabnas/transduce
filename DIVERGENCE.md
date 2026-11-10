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
entry that closes is deleted together with the test that pins it. The
others keep their numbers, which other repositories cite.

Entry 4 is measured: a verified list that differs, because one runtime's
suite cannot earn an entry the others' do. Entry 6
is measured: a token's value as two engines give it and the third does
not. The rest is what the shared
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

## 6. The engine's own space, line and comment tokens carry their text as their value in Rust

A token item (`ParserEvent::Token`) carries the token's value as the
engine gives it, as a pass's tokens do. The engine's own ignored tokens,
`#SP`, `#LN` and `#CM`, have no value in TypeScript and Go, which the
encoding writes as `null`, and their text in Rust:

| token | TypeScript | Go | Rust |
|---|---|---|---|
| `#SP` for `  ` | `null` | `null` | `"  "` |
| `#LN` for a line feed | `null` | `null` | `"\n"` |
| `#CM` for `/* x */` | `null` | `null` | `"/* x */"` |

The name, the text (`src`), the position and the byte offset are the
same in every runtime, and a part reads a token item's text from its
`src`. The split was there before token items: a grammar that consumes
`#LN` (JSON Lines does) carries it in a pass's tokens with the same two
values. It is the engine's to repair, in TypeScript's direction, as it
repaired a fixed token's value (tabnas/parser's `DIVERGENCE.md`, "Rule
instance numbers start at 0 in Go and Rust, and a fixed token's value is
its text in Rust"); this entry closes then. Until it does, no row of
`parser-events.tsv` writes a token item's value: the `trivia` stage
writes its name, its text and its offset.

Pinned in Rust by `a_space_token_item_carries_its_text_as_its_value`
(`rs/src/source/parser_events.rs`), in TypeScript by `a space token item
has no value` (`ts/test/parser-events.test.ts`) and in Go by
`TestASpaceTokenItemHasNoValue` (`go/parser_events_test.go`).
