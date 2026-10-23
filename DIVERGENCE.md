# Divergences

Where a runtime of this crate produces a **different result for the same
input**, and why that difference is allowed to stand. Three runtimes run
the shared fixtures in [`test/spec/`](test/spec/), which
[`docs/reference.md`](docs/reference.md) ("Shared fixtures") describes:
Rust in [`rs/`](rs/), TypeScript in [`ts/`](ts/) and Go in [`go/`](go/).
TypeScript agrees with Rust on every row both run (`events.tsv`'s
`incremental` rows are the ports' alone since step 13 of alchemy's
streaming plan retired Rust's inference); Go skips the rows
`specDivergences` in `go/spec_test.go` names, each a difference in a Go
grammar module, measured against Rust. Every entry here is re-measured
by running its input through each runtime, and an entry that closes is
deleted together with the test that pins it.

Entries 3 and 5 are measured: a grammar's difference between runtimes,
which each runtime's sources follow. The rest is what the shared
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
`value`, the `lines` path) emits numbers without lexemes, and only a
container projected into a table cell turns one into text here. No
fixture row does that with a number outside the range. The choice is
whether the contract is ECMAScript's `Number::toString` (Rust and Go
change) or this crate's text (the ports change); until it is made, the
row cannot be written. Pinned on the Rust side, for the numbers inside
the range, by `cells_print_as_json` and `lexemes_survive` in alchemy's
`rs/src/shared/table.rs` and `rs/src/shared/datum.rs`, where the shared
types and `write_json_number` live.

## 3. tabnas-csv reads a newline in a field differently under `record.separators`

| input, CSV with `record: { separators: ';' }` | Rust | TypeScript |
|---|---|---|
| `a,b;1,x\ny;3,4\nz;5,6` | three records, the newlines field text | `INPUT_INVALID` (`unexpected`) at 2:3 |

**A grammar's behaviour, not this crate's.** With the separator set, `\n`
is no line character to either grammar; the Rust one reads it as field
text and the TypeScript one refuses it. The TypeScript and Go line
sources hold to the whole parse of their runtime's grammar, and neither
cuts a record at the newline, so this crate agrees with itself and the
`csv` repository owns the difference. No row can carry it, since the
`options` column has no separators. Pinned in `ts/test/sources.test.ts`;
Rust's twin went with its line source at step 12 of alchemy's streaming
plan, and a Rust host reads CSV through its part, as the grammar
parses it.

## 4. Rust-only harness and tooling

Not behaviour, listed so their absence elsewhere is not mistaken for a
gap:

- **The criterion benches** (`rs/benches/throughput.rs`, `docs/BENCH.md`)
  measure the Rust implementation. A port measures itself its own way.
- **What a TSV cell cannot carry** stays in the Rust tests: input that
  is not UTF-8 or a reader that fails, and a reader that hands out a few
  bytes at a time (`rs/tests/reader_test.rs`, `rs/tests/records_test.rs`),
  the abort flag, a sink that stops or fails
  part-way, and the metrics.
  A port tests the same contracts in its own suite; they are API shapes
  (a borrowed or an owned sink, a `BufRead`), not results a row can name.
- **A grammar the test builds.** A row names a fleet grammar, and since
  tabnas/yaml#107 no listed one opens a container inside a map before
  the member's key, so the adapter's refusal of that
  (`STREAMABILITY_UNKNOWN`, "before announcing the member's key") has no
  row: `events.tsv`'s YAML key that is a mapping now streams as the
  walk. The TypeScript and Go suites pin the refusal over a grammar
  built in the test (`ts/test/incremental.test.ts`,
  `go/incremental_test.go`); Rust pinned it so until step 13 of
  alchemy's streaming plan retired its adapter.

## 5. Go does not stream PGN

A PGN database streams a game at a time through TypeScript's incremental
source, whose list (`ts/src/capability.ts`) names `pgn`, and is read
whole and then walked in Go, whose list (`go/capability_nodecell.go`)
does not: an incremental run of `pgn` in Go is refused before the parse
with `STREAMABILITY_UNKNOWN`, as for every unlisted grammar, and a host
reads the database through the walk. Rust's incremental source streamed
it as TypeScript's does until step 13 of alchemy's streaming plan retired
it; a Rust host reads PGN through tabnas-chess's transducer part.

Why: the Go adapter follows the containers a rule holds as generic maps
and lists (`map[string]any`, `[]any`), which is how every other Go
grammar builds its values. tabnas-chess's Go port builds typed values
(`*Database`, `*Game`, `*Line`, `*Move`), which are its public API, so the
adapter finds no container to follow. Go streams PGN once the adapter
follows typed values, or the module builds generic ones, which changes
its API. The Rust and TypeScript sides take tabnas-chess as a
dev-dependency (on the maintainer's instruction of 2026-10-14):
TypeScript's differential suite verifies `pgn`, and Rust's prune suite
reads it. Go takes none and runs no PGN, so its list cannot name it.

Pinned in TypeScript by the differential suite's `pgn` case
(`ts/test/incremental.test.ts`).

## 6. Only Rust reads the parser's source from a reader

`ParserEventSource::from_reader` hands the parser a reader in place of a
text (`Tabnas::parse_reader`, admin ADR-30), so a grammar that declares
`lex.window` streams a long document holding a window of its text. The
TypeScript and Go engines parse a string, so their sources take a text
read whole: the same passes, and the whole text held. They take a reader
when their engines do, in the maintainer's order, and this entry goes.

Pinned in Rust by `every_fixture_streams_the_same_from_a_reader` and
`an_unreadable_source_is_invalid_input` (`rs/tests/reader_test.rs`).
