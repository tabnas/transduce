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

Entry 6 is measured: a token's value as two engines give it and the third
does not. Entries 7 and 8 are measured too: a verified list that differs,
because one runtime's ports of four grammars read back what the others'
do not, and what a pruned run keeps of a tree one runtime's grammar holds
out of pruning's reach. The rest is what the shared fixtures deliberately
leave out, because it belongs to the Rust runtime or is not yet decided,
so that a port does not read the absence of a row as coverage, or as
licence.

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

## 7. TypeScript does not empty the maps of css and the grammar notations

`capability::PRUNES_MAPS` (`rs/src/source/capability.rs`) and Go's
`PrunesMapsGrammars` (`go/capability.go`) name `abnf`, `css`, `ebnf` and
`gbnf`, and TypeScript's (`ts/src/capability.ts`) names none of them. Each
is prunable in every runtime, and its rule history is bounded in
TypeScript and Go: the third shape is what differs, and with it what a
pruned run leaves of the tree.

Each of the four builds a node as a map and appends a finished child to a
list member of it in the parent's close phase: css's pushers
(`@cssPushRule` in `items`, `@cssPushDecl` in `decls` and the others), and
each notation's `prod`, whose `bc` appends a rule to the grammar's
`rules`. Emptying the map at its close pass deletes the list, and the next
append finds none. The Rust ports append only while the list is there, or
make it again, and the Go ports make it again, so their passes are the
same with the maps emptied. The TypeScript ports push onto it as it was
(`node.rules.push(...)`), which throws (`Cannot read properties of
undefined (reading 'push')`): with the maps emptied, 24 of css's 233
documents end so, 2 of abnf's 229, 2 of ebnf's 83 and 1 of gbnf's 104, the
generated document of 200 rules among each.

What it costs: a child is appended after its own close pass, so the second
shape, an array's elements before a node whose close the sink has seen,
never finds it, and the maps are all that would let the tree go. Ten times
a stylesheet's rules leave TypeScript's pruned tree holding 2,602 and then
26,002 elements, as many as a run that does not prune keeps, and ten times
a grammar's rules 802 and then 8,002 (807 and 8,007 in GBNF), each rule's
alternatives emptied and the rule kept, where Rust's and Go's hold none.
The repair is the grammars' (tabnas/css#79, tabnas/abnf#134,
tabnas/ebnf#81, tabnas/gbnf#80): once a TypeScript port makes a missing
list, as the Go ports do, the suite fails until TypeScript's list names
it, and this entry closes for it.

Pinned in TypeScript by the prune suite's `every grammar prunes as the
lists say` (`ts/test/prune.test.ts`), which holds `PRUNES_MAPS` both ways,
and in Rust and Go by the same suites and by their tests of ten times a
stylesheet's and a grammar's rules (`rs/tests/prune_test.rs`,
`go/prune_test.go`).

## 8. A pruned run keeps every game of a PGN database in Go

Every runtime's lists name `pgn`, and its passes are the same pruned or
not, but only Rust's and TypeScript's sources empty the database behind
the stream. chess's Go plugin keeps the database as a typed value, a
`*tabnaschess.Database` (a slice of `*Game`), and a Go source empties the
engine's own containers alone (`[]any`, `*[]any`, `tabnas.ListRef`,
`map[string]any`, `*tabnas.OrderedMap` and `tabnas.MapRef`), so a Go run
that prunes a PGN database keeps every game to its end: 200 games of 200,
and 2,000 of 2,000, where the Rust and TypeScript trees hold none.

Entry 4, "Go does not prune PGN", recorded the same consequence for
another reason until Go's suite ran PGN: its lists could not name it,
and a Go run that pruned a PGN database was refused. Go's suite runs PGN
now (on the maintainer's instruction of 2026-11-11), and its lists name
`pgn` as the others do. The repair is chess's (tabnas/chess#74): once
its Go plugin keeps its games where pruning reaches them, this entry
closes, and pgn's games join Go's test of ten times the records.

Pinned in Go by `TestAPrunedPGNRunKeepsEveryGame` (`go/prune_test.go`),
and in Rust and TypeScript by their tests of ten times PGN's games
(`ten_times_the_pgn_games_leave_the_pruned_tree_as_it_was` in
`rs/tests/prune_test.rs`, and the `pgn` pair of `ten times the records
leave the pruned tree as it was` in `ts/test/prune.test.ts`).
