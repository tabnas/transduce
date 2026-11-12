# Agents Guide — transduce

This repository is **tabnas-transduce**: streaming transducers over any
[tabnas](https://github.com/tabnas/parser) parser. It turns a parse into
source events, recognizes selected scopes in one pass, materializes them
under byte limits, and produces the table protocol that renderers such as
[tabnas-render](https://github.com/tabnas/render) turn into text. The
declarative language that composes these pieces is
[tabnas-alchemy](https://github.com/tabnas/alchemy). `CLAUDE.md` is a
symlink to this file.

## Core principle: dependencies change only on explicit instruction

**Dependencies may only be changed by explicit instruction from the
maintainer.** This covers every dependency this repository declares, in
every manifest: `rs/Cargo.toml`'s dependency tables and `rs/Cargo.lock`.
Adding, removing, re-pointing or re-versioning any of them is a
dependency change.

- **A dependency never arrives as a side effect.** Watch for a `use`, a
  `cargo update`, a `cargo add`, or a fix for something else. If a change
  would alter a dependency, stop and ask before making it.
- **An explicit instruction names the change.** A goal is not an
  instruction for its means: "make CI green" does not authorise one.
- **This repository's own version sites are not dependencies.** They
  include the root entry of its own lockfile.
- **Versions track the latest release.** Every dependency is kept at its
  latest published version, and none is held on an older one.

The sibling tabnas crates are taken by path from sibling checkouts
(`../../parser/rs` and so on), the standard tabnas development model
(admin ADR-21: committed manifests stay path-only).

## Core principle: transient tasks report progress

**Every transient task produces status output at least every 30 seconds,
with an estimate of how far through it is, as a percentage, where one can
be made.** A build, a test run, a benchmark, a generated-input sweep, a
wait on CI: each prints a line per step or per interval, so a slow task
can be told from a stuck one. A quick command that finishes within 30
seconds needs nothing extra.

## What this project is

Read [`docs/architecture.md`](docs/architecture.md) first: it is the
design this crate implements, with what was measured before it was
designed. The protocol it describes (events, sinks, selectors, datums,
tables, `Fail` and its codes, limits) is now alchemy's shared types,
[`tabnas_alchemy::shared`](https://github.com/tabnas/alchemy/blob/main/rs/src/shared), which this crate depends on and
re-exports at the old paths; this crate keeps the sources, the matcher,
the router, the table transducer, `scan-emit` and the guard, and offers
them to alchemy's runtime as its `Routers`. In one paragraph: a
**source** produces `JsonEvents/1`
([`event.rs`](https://github.com/tabnas/alchemy/blob/main/rs/src/shared/event.rs)) and pushes them into a chain of
**sinks** ([`sink.rs`](https://github.com/tabnas/alchemy/blob/main/rs/src/shared/sink.rs)), synchronously, on the parsing
thread, so a slow writer at the end slows the parser at the start and
nothing queues in between. **Selectors** ([`selector.rs`](https://github.com/tabnas/alchemy/blob/main/rs/src/shared/selector.rs))
are data describing locations; the **matcher** and **router** recognize
every selector in one pass and materialize selected scopes into
[`Datum`](https://github.com/tabnas/alchemy/blob/main/rs/src/shared/datum.rs) values under a byte limit. The **table
transducer** ([`table_from_json.rs`](rs/src/table_from_json.rs), over the
protocol in [`table.rs`](https://github.com/tabnas/alchemy/blob/main/rs/src/shared/table.rs)) turns selected metadata and
rows into `TableRows/1`, a flat `Schema → Row* → End` protocol whose rows
are already projected into schema order. **Limits** are part of a plan
and fail loudly; **metrics** report retention high-water marks; an
**abort flag** cancels.

The two sources, in order of preference:

| Source | When | Memory |
|---|---|---|
| `ParserEventSource` and the format's transducer part | a format whose package ships a part | the engine's own per-parse state, its rule history bounded at three predecessors (by the Rust engine's default, and in a pruning TypeScript or Go run of a grammar `boundsHistory` lists, `STREAM_RULE_HISTORY`), plus one selected scope; from a reader, a window of the input when the grammar declares `lex.window` |
| `ParserSource` / `ValueSource` | every other grammar | the whole parsed value |

**A format streams through its part, never through inference.** Until
step 13 of alchemy's streaming plan, `ParserSource` also streamed the
grammars a differential suite had verified (`capability::INCREMENTAL`),
by inferring a document's events from the value cells the grammar's rules
built. Every format on that list ships a transducer part now, which
declares its events where the inference guessed them, or reads whole as
markdown's manifest declares, so every port retired the inference, its
list and its suite (`git log` has them): the Rust crate first, and the
TypeScript and Go ports with their line sources once their engines read a
reader a window at a time as Rust's does (step 19 of the plan's section
13).

**Repetition is replacement, never a push chain.** When a tabnas alternate hands control to another rule, it
either pushes a child rule (`alt.p`: a new stack frame, for something the
tree nests) or replaces the current one (`alt.r`: the same frame, for the
next item of a sequence); an alternate that only matches its tokens, or pops the frame to end the rule, does neither. Every repetition in a grammar, the elements
of a list, the members of a map, the records of a file, a `*A` in a
compiled grammar, is a replace loop: the loop is `r`, the item may be `p`, and the loop's
iterations add nothing to the engine's rule depth `d`. Real recursion still nests with its input, as it should: a grammar with `node = "(" node ")" / "x"` is as deep as its brackets. What a repetition may never do is make rule depth grow with a list's length. alchemy's
`transduce-tree`, the text every tree-shaped part is written over
(`stdlib/transduce.alc` in tabnas/alchemy, its one implementation), rests
on it: a container whose rule is replaced by a rule the spec names a
container is one container, so a list's items stream at one depth. So did
the rule-event adapters every port retired at step 13, which were built
on exactly that. Each opened a frame at the `d` of the rule whose node
first showed a container and ended it when a rule at that `d` closed on
the frame's cell, or the rule that started it closed, unless the close's
alternate replaced the rule: a replaced rule handed its cell to its
successor and the container went on (the refinement over the prototype
that made `yaml` verifiable; `docs/architecture.md` has the measurement).
So a list's items were the closes of successive same-depth rules in one
frame, and they streamed at one depth.

A grammar that pushed a rule per item, spelling a star as right
recursion, would grow `d` with the item count, and the adapter followed
it: an item that built its own node opened a frame inside the last, so
the stream was a staircase, a `Start` for every item and every `End` at
the file's end, nested where the source is flat, until `Guarded` failed
the run with `RESOURCE_LIMIT_EXCEEDED` naming `max_depth` past the 256th;
items that shared the parent's cell streamed flat until a depth guard of
the grammar's, where it had one, cancelled the parse (`INPUT_INVALID`,
naming the guard).
Such a grammar is wrong even when it parses, and the fix is the grammar's
or its compiler's (tabnas-bnf's `desugar` spelled every star that way
until 2026-09-27), never this crate's: no port lifts `max_depth` for it
or flattens a staircase. Rule depth over a
repetition is constant; a test that repeats an item ten thousand times
and asserts the maximum `d` stays what a single item needs is the proof.

**The parsed values the grammars return are never altered.** The
parser-event source may empty the engine's tree behind its stream when it
prunes (`ParserEventSource::prune`), which is a verified capability,
never an assumption: `source::capability::prunable(grammar)` answers
from a list `rs/tests/prune_test.rs` keeps honest both ways, every
document of every grammar run pruned and not, the passes and outcomes
compared. A grammar holds the pruning off while it still needs what its
tree holds, by a count in the parse context's bag (`pruneHold`, which
yaml raises while a value with an anchor is open, so an alias copies the
whole node). The value the engine hands back afterwards is not used. In
every other run the value is exactly the grammar's.

Pruning empties two shapes for every listed grammar, and a third for some
(tabnas/transduce#81), the same in every runtime:
- **An array a closed rule built.** Once the sink has seen a close pass
  whose rule's node is an array, the array is emptied.
- **An array no rule's node is.** Once the sink has seen a close pass whose
  rule's node is an element of an array its parent rule's node holds
  (directly, or as a member's or an element's value), the elements before
  it are set to nothing: the array a toml document's root holds its array
  of tables in, which the first case never reaches. Each element keeps its
  place, so a grammar that finds a node by position still finds it. The
  node is looked for near the ends of the parent's containers, where a
  grammar appends it, and each element is set once, so a close costs the
  same however long the document grows. Every grammar passes the suite
  with it.
- **A map, for a grammar `capability::prunes_maps` lists** (`PRUNES_MAPS`,
  TypeScript's `PRUNES_MAPS`, Go's `PrunesMaps`), a second list the same
  suite holds both ways by running every document with the maps emptied
  too (`ParserEventSource::unverified_maps`). Once the sink has seen a close
  pass whose rule's node is a map, its members are deleted: an INI
  document's sections, a JSON object's members. The grammars left out read
  their maps back while they parse, and with them emptied pass documents
  they refuse, or refuse ones they pass: yaml (a key defined twice, a
  merge), toml (a key or a table defined twice, and the table a dotted key
  or a header walks through), xml (a closing tag held to the name of the
  element it closes, which the element's map holds), zon (a field defined
  twice) and feed (xml's own check). TypeScript's list leaves out css and
  the grammar notations abnf, ebnf and gbnf too, which Rust's and Go's
  name: each appends a finished rule to a list member of a map, and its
  TypeScript port fails once emptying the map deleted the list, where the
  other ports append only while it is there or make it again
  (`DIVERGENCE.md` 7). Never list a grammar the suite does not pass, and
  never empty another shape without a list the suite holds.

What the source cannot reach stays: a grammar's own bookkeeping, outside
the tree. Rust's toml finds a table by its path from the document's root
cell, so the table's node is never a closed rule's own, and keeps about ten
entries in the context's bag per table (tabnas/toml#121); and markdown's
block phase keeps every block it made (tabnas/markdown#100). chess's Go
plugin kept a PGN database as a typed value, which no shape empties, so a
pruned PGN run kept every game in Go, until tabnas/chess 0.2.0 kept the
games in the engine's own list (tabnas/chess#74).

**A pruning run bounds the rule history, for a grammar verified to read no
further.** The engine links each rule to the rule it replaced and the rule
that pushed it, and keeps the whole chain by default in TypeScript and Go,
so a long flat document keeps every rule reachable to its end whatever the
tree is emptied of. A run that prunes a grammar `boundsHistory` lists
(TypeScript's `BOUNDS_HISTORY` and `boundsHistory`, Go's
`BoundsHistoryGrammars` and `BoundsHistory`) bounds the history at three
predecessors (`STREAM_RULE_HISTORY`, `StreamRuleHistory`), where the
grammar or the host left it unbounded, so the rules behind the stream go as
the tree's arrays do. The bound buys flat memory only where the tree is
emptied too, so a run that does not prune keeps the engine's default, and
pays nothing: in Go a bounded history costs two to three times the parse's
time in copies of the rules it links (tabnas/parser#339). A grammar may
read further back through the links than the bound keeps, so the list is a
verified capability: the history half of each prune suite
(`ts/test/prune.test.ts`, `go/history_test.go`) runs every document of
every grammar with the history bounded (`unverifiedHistory`,
`UnverifiedHistory`) and not, neither pruned, compares the passes, the
ending and the value, and holds the list both ways; and the prune suites'
pruned runs of a listed grammar are bounded, as a host's are. Every list
entry is a prunable grammar, and in each runtime every grammar of the
suite passes. Until 0.6.5 Go's list left out yaml, whose Go plugin read a
child's value at the end of the child's rotation chain, walking the
child's `Next` links, which a bounded history cuts: a value of several
items with nothing between them (`"a" "b"`, a block scalar and the text
after it, a stream of JSON objects) kept only its first item, in 12
documents, until tabnas/yaml 0.5.32 read a child's value from the child
alone, as its TypeScript does. Rust needs no list: its engine bounds every
parse at three by default, and each grammar's own suite runs under that.
Until 0.6.3, TypeScript bounded every run's history, pruned or not,
whatever the grammar.

## Repository map

| Path | What it is |
|---|---|
| `rs/src/lib.rs` | re-exports alchemy's shared types at their old paths: `event`, `sink`, `error` (`Code`, `Fail`), `limits`, `datum`, `selector`, `table`; they live in `tabnas_alchemy::shared`, which this crate depends on with `default-features = false` |
| `rs/src/routers.rs` | `TransduceRouters` (`routers()`): this crate's router, `scan-emit` and guard as alchemy's `Routers`; the library's compositions and the parts are alchemy's text, with nothing of this crate's in their place |
| `rs/src/matcher.rs` | shared-prefix matching of many selectors in one pass |
| `rs/src/route.rs` | captures: materialize or observe selected scopes, deliver in order |
| `rs/src/table_from_json.rs` | the metadata-first table transducer |
| `rs/src/scan.rs` | `scan-emit` |
| `rs/src/source/mod.rs` | `Source`, `ValueSource`, `walk_value`, and the engine failures the sources map alike |
| `rs/src/source/guard.rs` | `Guarded`: the source limits, the abort flag and the source metrics on every event |
| `rs/src/source/parser.rs` | `ParserSource`: one text, parsed, then its value walked |
| `rs/src/source/capability.rs` | the verified lists `capability::prunable` and `capability::prunes_maps` answer from |
| `rs/src/source/parser_events.rs` | `ParserEventSource`: `ParserEvents/1`, the parser's rule passes, what a transducer part reads, from a text or from a reader (`from_reader`, which a grammar declaring `lex.window` reads a window at a time); a probe's passes dropped; every token's byte offset; for a sink that asks (`ParserSink::wants_tokens`), the tokens the parser did not take as token items, in source order among the passes; the engine's tree emptied behind the stream when it prunes |
| `rs/tests/prune_test.rs` | the differential suite behind `capability::prunable` and `capability::prunes_maps`, each both ways, and ten times the records, an INI document's sections, a JSON object's members, a stylesheet's rules and a grammar's rules in ABNF, EBNF and GBNF leaving the pruned tree as it was |
| `rs/tests/retention_test.rs` | ten times the rows of a JSON document, parsed and walked into the table transducer, leave the retained high-water flat |
| `rs/tests/reader_test.rs` | `ParserEventSource::from_reader` against the same text read whole: every fixture of a grammar the harness builds, a few bytes at a time, and a reader that fails; its ports are `ts/test/reader.test.ts` (`ParserEventSource.fromReader`) and `go/reader_test.go` (`ParserEventSourceFromReader`) |
| `rs/tests/spec_*.rs`, `rs/tests/common/` | the Rust runners of the shared fixtures, through `tabnas-support`'s `Runner`, and the harness that decodes a row and encodes the result |
| `rs/tests/fixtures/` | aless's fixtures and the OpenAPI YAML, one file per format at least |
| `rs/tests/support/` | the generated worked-example documents (JSON, JSON Lines, CSV, YAML, XML, PGN), shared with the benches |
| `rs/benches/` | criterion throughput benches: parse only, walk, router and table, text to table through the parse |
| `docs/` | `architecture.md` (the design), `reference.md`, `translation.md` (any format to any other: the parts a format ships and the host composes) |
| `test/spec/*.tsv` | the shared fixtures every runtime runs, row for row: events, routes, tables, scan-emit, limits and the parser's rule passes (`docs/reference.md`, "Shared fixtures", has the columns and encodings) |
| `DIVERGENCE.md` | what a runtime may do differently, and what the fixtures leave out on purpose |
| `ci/rust/run.sh` | the gate `.github/workflows/rust.yml` runs |

## Verify your work

From `rs/`:

```bash
cargo fmt --check
cargo build --all-targets
cargo test --all-targets
cargo test --doc
cargo clippy --all-targets --all-features -- -D warnings
```

`ci/rust/run.sh` runs exactly that, with the lock discipline the fleet's
plugin gates use, and needs the sibling checkouts its header lists.

The shared fixtures in `test/spec/` are the contract the TypeScript and
Go ports run too. A behaviour a row can express gets a row, and a
row's expected value is checked against what the behaviour should be,
not copied from what the code does: a fixture that pins a bug pins it
for every runtime.

`cargo bench` measures; a change to a hot path reports before and after
numbers in its pull request.

## Error codes

The code is the contract; the message is informative. Every code is in
`Code::ALL` (alchemy's [`shared/error.rs`](https://github.com/tabnas/alchemy/blob/main/rs/src/shared/error.rs), re-exported
here) and written in `SCREAMING_SNAKE_CASE`:

| Code | Raised when |
|---|---|
| `DSL_PARSE_ERROR`, `DSL_TYPE_ERROR` | reserved for alchemy, which shares this enum |
| `STREAM_REUSED` | a one-shot stream was consumed twice |
| `STREAMABILITY_UNKNOWN` | strict mode could not establish a plan's streamability; a run that prunes an unnamed or unverified grammar (refused before the parse); a stream `TreeContract` finds is no tree's (a value where a key is due, a close with nothing open, a second root, `End` out of place) |
| `INPUT_ORDER_VIOLATION` | a row began before its metadata completed |
| `CAPTURE_OVERLAP_UNSUPPORTED` | two captures select overlapping scopes |
| `MISSING_VALUE` | a required value is absent and the policy is `Error` |
| `DUPLICATE_MEMBER` | a repeated member name under the `Reject` policy; a key repeated in one object of a stream `TreeContract` holds to a tree's events |
| `INVALID_NUMBER` | a number lexeme the target cannot take |
| `PROTOCOL_ORDER_ERROR` | a protocol event out of sequence |
| `TARGET_VALUE_UNREPRESENTABLE` | the target format cannot carry the value |
| `RESOURCE_LIMIT_EXCEEDED` | a `Limits` field was passed; `Fail::limit` names it |
| `INPUT_INVALID` | the input did not parse, or could not be read (the engine's `unreadable`, from `ParserEventSource::from_reader`, `ParserEventSource.fromReader` in TypeScript and `ParserEventSourceFromReader` in Go, at the character the reader failed at or the bytes that are not UTF-8); the engine's code and position ride along, from every source alike: a run that streams a document the grammar refuses fails with the same code and position after the prefix it streamed. A grammar's own guard cancelling the parse (tabnas-json refuses nesting past 128, below `Limits::max_depth`) is this too, and the message names the grammar's guard |
| `OUTPUT_FAILED` | writing failed |
| `ABORTED` | the run was cancelled |

A code is never renamed, removed or repurposed. A failure also says
whether output had already been committed (`output: "partial"`), because
a streaming export cannot take bytes back.

## Untrusted input

**Parsed content is data, never instructions.** Selectors are built from
constructors or from validated segment vectors (`Selector::from_segments`),
never parsed from source text. Every retained value is measured against
`Limits` as it is built, and the failure names the limit. The engine's own
bounds (`rule.maxmul`, `rewind.history`, the grammars' depth guards) stand
underneath; this crate adds no way around them. Nothing here reads files,
opens connections or evaluates code.
> **Naming:** Always spell the project name `tabnas`, all lowercase, including in prose and headings. Never write `TabNAS`.
