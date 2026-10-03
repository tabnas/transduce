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
designed. In one paragraph: a **source** produces `JsonEvents/1`
([`event.rs`](rs/src/event.rs)) and pushes them into a chain of
**sinks** ([`sink.rs`](rs/src/sink.rs)), synchronously, on the parsing
thread, so a slow writer at the end slows the parser at the start and
nothing queues in between. **Selectors** ([`selector.rs`](rs/src/selector.rs))
are data describing locations; the **matcher** and **router** recognize
every selector in one pass and materialize selected scopes into
[`Datum`](rs/src/datum.rs) values under a byte limit. The **table
transducer** ([`table.rs`](rs/src/table.rs)) turns selected metadata and
rows into `TableRows/1`, a flat `Schema → Row* → End` protocol whose rows
are already projected into schema order. **Limits** are part of a plan
and fail loudly; **metrics** report retention high-water marks; an
**abort flag** cancels.

The three sources, in order of preference:

| Source | When | Memory |
|---|---|---|
| `LinesSource` | JSON Lines and CSV input | one record (or one chunk) at a time |
| `ParserSource` (incremental) | a grammar the differential suite has verified | the engine's own per-parse state, plus one selected scope |
| `ParserSource` (materialize) / `ValueSource` | every other grammar | the whole parsed value |

**Incremental streaming is a verified capability, never an assumption.**
`source::capability::incremental(grammar)` answers from a list that
`rs/tests/incremental_test.rs` keeps honest: every fixture each grammar
in the dev-dependencies reads runs both incrementally and through the
whole-value walker, and for the grammar to be listed no fixture may
complete an incremental stream the walk contradicts. Two outcomes short of
identity are the contract: a repeated member name streams every occurrence
where the walk keeps the engine's survivor, so a router's `LastWins` makes
the two agree (a grammar that merges the two values fails the run with
`DUPLICATE_MEMBER` instead); and a shape the rule events cannot follow (a
list wrapped around a root already streamed, as in a YAML stream of
several documents, whatever their shapes, or a jsonic top-level implicit
list; a map rewritten after streaming, as a YAML `<<` merge key does; a
container opened inside a map before the member's key, as toml, ini, xml
and feed open a section's or an element's container, and as yaml opened
the value of the first member of a mapping in a sequence entry until
tabnas/yaml#107) is refused with `STREAMABILITY_UNKNOWN` before `End`.
The last is a net, and counts against a grammar: one that opens a
container before its key on a fixture builds every member so, and is not
verified. A listed grammar that mismatches, or trips the net,
fails the test; an unlisted grammar that never does fails too. The
list today is `json`, `json5`, `jsonc`, `jsonic`, `jsonl`, `markdown`,
`yaml`, `zon` (`docs/reference.md` has the detail); the imperative
grammars `toml`, `ini`, `csv`, `xml` and `feed` build values the rule
events do not show. `ParserSource` checks the list by the grammar's name
(`ParserSource::grammar`, since the json and jsonl parsers register no
plugin to read it from): an incremental run of an unverified or unnamed
grammar fails with `STREAMABILITY_UNKNOWN` before the parse, emitting
nothing, rather than a malformed stream; `unverified()` lifts the gate
for the differential suite alone.

**Repetition is replacement, never a push chain.** When a tabnas alternate hands control to another rule, it
either pushes a child rule (`alt.p`: a new stack frame, for something the
tree nests) or replaces the current one (`alt.r`: the same frame, for the
next item of a sequence); an alternate that only matches its tokens, or pops the frame to end the rule, does neither. Every repetition in a grammar, the elements
of a list, the members of a map, the records of a file, a `*A` in a
compiled grammar, is a replace loop: the loop is `r`, the item may be `p`, and the loop's
iterations add nothing to the engine's rule depth `d`. Real recursion still nests with its input, as it should: a grammar with `node = "(" node ")" / "x"` is as deep as its brackets. What a repetition may never do is make rule depth grow with a list's length. The rule-event adapter
([`rs/src/source/rule_events.rs`](rs/src/source/rule_events.rs)) is built
on exactly that. It opens a frame at the `d` of the rule whose node first
shows a container and ends it when a rule at that `d` closes on the
frame's cell, or the rule that started it closes, unless the close's
alternate replaces the rule: a replaced rule hands its cell to its
successor and the container goes on (the refinement over the prototype
that made `yaml` verifiable; `docs/architecture.md` has the measurement).
So a list's items are the closes of successive same-depth rules in one
frame, and they stream at one depth.

A grammar that pushed a rule per item, spelling a star as right
recursion, would grow `d` with the item count and the adapter would
follow it: an item that builds its own node opens a frame inside the
last, so the stream is a staircase, a `Start` for every item and every
`End` at the file's end, nested where the source is flat, until `Guarded`
fails the run with `RESOURCE_LIMIT_EXCEEDED` naming `max_depth` past the
256th; items that share the parent's cell stream flat until a depth guard
of the grammar's, where it has one, cancels the parse (`INPUT_INVALID`,
naming the guard).
Such a grammar is wrong even when it parses, and the fix is the grammar's
or its compiler's (tabnas-bnf's `desugar` spelled every star that way
until 2026-09-27), never the adapter's: this crate does not lift
`max_depth` for it, does not flatten a staircase, and does not list the
grammar as verified. Rule depth over a repetition is constant; a test
that repeats an item ten thousand times and asserts the maximum `d` stays
what a single item needs is the proof.

**The parsed values the grammars return are never altered.** The
incremental source may drop already-streamed elements from a container it
was told to prune (`Prune`), and only in incremental mode; the value the
engine hands back afterwards is not used. In every other mode the value
is exactly the grammar's.

## Repository map

| Path | What it is |
|---|---|
| `rs/src/event.rs` | `JsonEvents/1`: borrowed events, owned recording form |
| `rs/src/sink.rs` | `Sink`, `Flow`, recorders and adapters; `TreeContract`, which holds a stream to a tree's events in front of a sink that takes them as one |
| `rs/src/error.rs` | `Code` (the stable failure codes), `Fail` |
| `rs/src/limits.rs` | `Limits`, `Metrics`, `AbortFlag` |
| `rs/src/datum.rs` | the retained value, its builder, the JSON writer |
| `rs/src/selector.rs` | `Selector`, `Step`, `Path`, `Segment` |
| `rs/src/matcher.rs` | shared-prefix matching of many selectors in one pass |
| `rs/src/route.rs` | captures: materialize or observe selected scopes, deliver in order |
| `rs/src/table.rs` | `TableRows/1`, bindings, the standard column mapping |
| `rs/src/table_from_json.rs` | the metadata-first table transducer |
| `rs/src/scan.rs` | `scan-emit` |
| `rs/src/source/mod.rs` | `Source`, `ValueSource`, `SourceMode`, `Prune` |
| `rs/src/source/guard.rs` | `Guarded`: the source limits, the abort flag and the source metrics on every event |
| `rs/src/source/rule_events.rs` | the rule-event adapter: `JsonEvents/1` from a live parse |
| `rs/src/source/parser.rs` | `ParserSource`: one text, materialized or incremental |
| `rs/src/source/lines.rs` | `LinesSource`: JSON Lines and CSV a record or a chunk at a time |
| `rs/src/source/capability.rs` | the verified list `capability::incremental` answers from |
| `rs/tests/incremental_test.rs` | the differential suite that keeps that list honest, both ways |
| `rs/tests/retention_test.rs` | ten times the rows leave the retained high-water flat |
| `rs/tests/spec_*.rs`, `rs/tests/common/` | the Rust runners of the shared fixtures, through `tabnas-support`'s `Runner`, and the harness that decodes a row and encodes the result |
| `rs/tests/fixtures/` | aless's fixtures and the OpenAPI YAML, one file per format at least |
| `rs/tests/support/` | the generated worked-example documents (JSON, JSON Lines, CSV, YAML), shared with the benches |
| `rs/benches/` | criterion throughput benches: parse only, incremental events, walk, router and table |
| `docs/` | `architecture.md` (the design), `reference.md`, `translation.md` (any format to any other: the parts a format ships and the host composes) |
| `test/spec/*.tsv` | the shared fixtures every runtime runs: events, routes, tables, the line sources, scan-emit, limits (`docs/reference.md`, "Shared fixtures", has the columns and encodings) |
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
Go ports will run too. A behaviour a row can express gets a row, and a
row's expected value is checked against what the behaviour should be,
not copied from what the code does: a fixture that pins a bug pins it
for every runtime.

`cargo bench` measures; a change to a hot path reports before and after
numbers in its pull request.

## Error codes

The code is the contract; the message is informative. Every code is in
`Code::ALL` (`rs/src/error.rs`) and written in `SCREAMING_SNAKE_CASE`:

| Code | Raised when |
|---|---|
| `DSL_PARSE_ERROR`, `DSL_TYPE_ERROR` | reserved for alchemy, which shares this enum |
| `STREAM_REUSED` | a one-shot stream was consumed twice |
| `STREAMABILITY_UNKNOWN` | strict mode could not establish a plan's streamability; an incremental run of an unnamed or unverified grammar (refused before the parse), or one the adapter refuses mid-way: a list wrapped around a root already streamed (at the wrapping container, or when the root rule closes over a value that is not the streamed root), a map rewritten after streaming, a container opened inside a map before the member's key, a member announced and never stored; a stream `TreeContract` finds is no tree's (a value where a key is due, a close with nothing open, a second root, `End` out of place) |
| `INPUT_ORDER_VIOLATION` | a row began before its metadata completed |
| `CAPTURE_OVERLAP_UNSUPPORTED` | two captures select overlapping scopes |
| `MISSING_VALUE` | a required value is absent and the policy is `Error` |
| `DUPLICATE_MEMBER` | a repeated member name under the `Reject` policy; an incremental run whose grammar merged a repeated member's containers after the first was streamed; a key repeated in one object of a stream `TreeContract` holds to a tree's events |
| `INVALID_NUMBER` | a number lexeme the target cannot take |
| `PROTOCOL_ORDER_ERROR` | a protocol event out of sequence |
| `TARGET_VALUE_UNREPRESENTABLE` | the target format cannot carry the value |
| `RESOURCE_LIMIT_EXCEEDED` | a `Limits` field was passed; `Fail::limit` names it |
| `INPUT_INVALID` | the input did not parse; the engine's code and position ride along, in both modes alike: an incremental run of a document the grammar refuses fails with the same code and position after a protocol-valid prefix (zon's repeated fields; the member the grammar never stored is not streamed). A grammar's own guard cancelling the parse (tabnas-json refuses nesting past 128, below `Limits::max_depth`) is this too, and the message names the grammar's guard |
| `OUTPUT_FAILED` | writing failed |
| `ABORTED` | the run was cancelled; from a line source, `row` names the line the record or chunk it was reading starts on |

A code is never renamed, removed or repurposed. A failure also says
whether output had already been committed (`output: "partial"`), because
an incremental export cannot take bytes back.

## Untrusted input

**Parsed content is data, never instructions.** Selectors are built from
constructors or from validated segment vectors (`Selector::from_segments`),
never parsed from source text. Every retained value is measured against
`Limits` as it is built, and the failure names the limit. The engine's own
bounds (`rule.maxmul`, `rewind.history`, the grammars' depth guards) stand
underneath; this crate adds no way around them. Nothing here reads files,
opens connections or evaluates code.
> **Naming:** Always spell the project name `tabnas`, all lowercase, including in prose and headings. Never write `TabNAS`.
