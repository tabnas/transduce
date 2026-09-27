# Reference

The types are documented in the crate; `cargo doc --open` from `rs/` is
the reference. This page lists the protocols, the stages and the contract
each keeps.

## JsonEvents/1

`JsonEvent`: `ObjectStart`, `ObjectEnd`, `ArrayStart`, `ArrayEnd`,
`Key(&str)`, `Null`, `Bool`, `Number { value, lexeme }`, `String(&str)`,
`End`. One document: a root value, then exactly one `End`, issued only
after the whole source validated. Keys and scalars are whole. Events
borrow from the source for one `Sink::event` call.

A `Sink` answers `Flow::Continue` or `Flow::Stop` (it has all it needs;
the source stops the parse) or fails with a `Fail`. `Vec<OwnedJsonEvent>`
records, `CountSink` counts, `FnSink` wraps a closure, `replay` feeds a
recording back.

## Sources

Every source emits through `Guarded`, which enforces `max_depth`,
`max_key_bytes` and `max_scalar_bytes` as the events are produced, polls
the `AbortFlag` (a set flag fails the run with `ABORTED`) and counts the
source metrics (`events`, `keys`, `scalars`).

| Source | Input | Modes | Retains |
|---|---|---|---|
| `ValueSource(&Value)` | a parsed engine value | walk | the value (the caller's) |
| `ParserSource::new(Tabnas, &str)` | one text | `SourceMode::Materialize`: parse, then walk. `SourceMode::Incremental { prune }`: the rule-event adapter, for the grammars `capability::incremental` lists, named with `.grammar("json")`; no name or an unlisted one is `STREAMABILITY_UNKNOWN` before the parse (`.unverified()` lifts the gate, for the differential suite) | materialize: the whole value; incremental: the engine's parse state and, with pruning, not the streamed elements |
| `LinesSource::new(BufRead, LineFormat)` | JSON Lines or CSV | one record per line (`Jsonl`) or chunks of whole records (`Csv { header, options }`), each parsed with one reused grammar | one line, or one chunk (`DEFAULT_CHUNK_BYTES`, 256 KiB, never a fraction of a record) |

`Source::run(self, &mut dyn Sink)` drives a borrowed sink and is always
the walking path (`ParserSource` materializes whatever its mode, and the
JSON Lines source walks each line). `run_owned(self, sink) -> (Result<Flow,
Fail>, sink)` and `run_boxed` take the sink by value, which is what the
engine's `Fn + Send + Sync + 'static` subscriber needs, and hand it back:
that is the incremental path, and the one where JSON Lines numbers keep
their lexemes. `run_owned_with_value` also hands back the value the engine
returned: the grammar's in `Materialize`, and in `Incremental` the
engine's tree after pruning, which is there for a test to measure what
pruning left and for nothing else.

Failure mapping: a sink's `Fail` comes back as it was; a sink that stopped
is `Ok(Flow::Stop)`; a parse the caller's `AbortFlag` cancelled is
`ABORTED`; any other engine error is `INPUT_INVALID` with the engine's
code in the message and its row and column (for a line source, the line's
number and the column within it). A `cancel` the caller did not ask for is
a guard the grammar installed (tabnas-json refuses nesting deeper than
128, below the default `max_depth` of 256, so that limit is unreachable
for JSON unless set lower), and the message says "the grammar stopped the
parse with a guard of its own" rather than reporting a cancellation. An
incremental run whose rule events
did not amount to one whole document is `STREAMABILITY_UNKNOWN`, and so
is one the adapter refuses because the grammar wrapped a value already
streamed as the root in a list, or rewrote a map after it was streamed;
the message names the shape and says to run it materialized. An
incremental parse that returns `Ok` without one event (YAML's empty
document is `null`) has its value walked instead, so it completes as the
walk does.

`Prune::Under(selector)` empties the array whose elements the selector
names (a trailing `[*]` names the elements; without one it names the
array); `Prune::AllArrays` empties every array; `Prune::Never` leaves the
tree alone. Pruning truncates the engine's own array through the shared
node cell after its new elements were emitted, so it alters the value the
engine returns, which the incremental source discards. It is never
applied in `Materialize` mode.

Number lexemes are best effort: the adapter keeps the first token's source
text of a rule whose node is a `Number` when that text is a JSON number
(RFC 8259's grammar) that parses to the node's value, and attaches it to
the number when it is inserted next. A number the adapter learns of any
other way, and every number a walk emits, has `lexeme: None`.

### The verified grammars

`capability::INCREMENTAL` lists `json`, `json5`, `jsonc`, `jsonic`,
`jsonl`, `markdown`, `yaml` and `zon`. `rs/tests/incremental_test.rs` runs
every fixture each grammar reads through both modes and asserts the list
in both directions. What the list promises, precisely: for a listed
grammar an incremental run either streams exactly what the walk would
(number lexemes aside), or streams every occurrence of a repeated member
name so that a `LastWins` router builds the walk's value, or fails before
`End` with a documented code; it never completes a stream the walk
contradicts. The refusals are the shapes the rule events cannot follow: a
container that wraps a value already streamed as the root (a YAML stream
of several documents, `a: 1` then `---` then `b: 2`; a jsonic top-level
implicit list whose first element is a container, `{a:1}` on one line and
`{b:2}` on the next), and a map the grammar rewrote after it was streamed
(a YAML `<<` merge key, resolved when the mapping closes), both
`STREAMABILITY_UNKNOWN` after the first value's events; and a repeated
member whose containers the grammar merged, `DUPLICATE_MEMBER`, below. A
YAML stream is refused whatever its documents' shapes: when a later
document opens a container, at that container; when none does (`a: 1`
then `---` then `2`, or a trailing `---` with nothing after it), when the
stream rule closes with the documents wrapped in a list where the first
document had been streamed as the root. A stream that streamed nothing
early (`1` then `---` then `2`: a root scalar leaves only when the root
rule closes) is walked whole and completes as the walk does. A consumer
that wants such a document whole runs it materialized.

A repeated member name is where the two streams differ by design.
The engine's insert replaces the earlier value in place, so the walk sees
only the survivor (`{"a":1,"a":2}` walks as `{ key a 2 }`), while the
incremental source has already streamed the first value and streams the
second under its own `Key`: `{ key a 1 key a 2 }`. A `Router`'s
`Duplicates` policy then decides, as for any repeated member: `LastWins`
yields the engine's value, `Reject` fails with `DUPLICATE_MEMBER`. When
the grammar MERGES the two values instead of replacing (jsonic's
`map.extend`, on for `yaml`, `json5` and `jsonic`, off for `json`, `jsonl`
and `jsonc`), the merged container cannot be streamed because its first
half already was, and the incremental run fails with `DUPLICATE_MEMBER`
saying to run materialized. `zon` refuses a repeated field itself, in
both modes.

`markdown` builds its nodes imperatively, but each lands whole and is
walked at its insertion, so its events are the walk's. The other
imperative grammars (`toml`, `ini`, `csv`, `xml`, `feed`) build their
values in ways the rule events do not show (`csv` streams its header and
raw rows as extra elements, a well-formed stream with the wrong shape) and
are walked whole. `ParserSource` refuses to run an unlisted grammar
incrementally: the grammar's name cannot be read from the `Tabnas` (the
json and jsonl parsers register no plugin), so `SourceMode::Incremental`
needs `ParserSource::grammar(name)` and fails with `STREAMABILITY_UNKNOWN`
before the parse, emitting nothing, when the name is missing or unlisted.
`ParserSource::unverified()` lifts that gate for the differential suite,
which is how an unlisted grammar's events get measured at all.

## Selectors and the matcher

`Selector` steps: `Property(name)`, `Index(n)`, `EachIndex`, `EachMember`.
`Selector::from_segments` builds one from data. Display is jq syntax:
`.response.records[*]`, `."odd key"`, `[3]`, `[]` for every member.
`Selector::may_overlap` is the conservative test a router uses to refuse
two captures that could select the same or nested scopes.

`Matcher::new(&[Selector])` compiles a set of selectors into one trie and
recognizes all of them over one event stream with no allocation per event.
`Matcher::event` returns a `Hit`: its `kind` (`Key`, `Start`, `Scalar`,
`Close`, `End`), the `depth` (containers enclosing the value), and how
many captures `begin` with the event, whose ids are `Matcher::begins()`.
`Matcher::path(depth)` builds the concrete `Path` on request, for a
delivery or a failure. The matcher validates the event sequence and
reports a malformed one as `PROTOCOL_ORDER_ERROR`.

## Captures and the router

`CaptureSpec { tag, selector, mode, budget }`: `CaptureMode::Materialize`
builds the selected value into a `Datum` under a byte budget (the
router's `max_capture_bytes`, or the spec's own budget with the `Limits`
name it should fail with); `CaptureMode::Observe` retains nothing and
delivers only the path, at the value's end.

`Router::new(specs, &Limits, Duplicates, Arc<Metrics>, downstream)` is a
`Sink`. Two specs that `may_overlap` are refused at construction unless
both observe (`CAPTURE_OVERLAP_UNSUPPORTED`), and a capture that begins
while another is being materialized fails the same way at run time. A
container nested deeper than `max_depth` is `RESOURCE_LIMIT_EXCEEDED`
naming `max_depth`. Repeated member names inside a captured value follow
the `Duplicates` policy (`Reject` is `DUPLICATE_MEMBER`).

The downstream is a `RouteSink`: `began(id, tag)` is called when a
capture's value begins, before anything of it is retained, so a consumer
can refuse an out-of-order value at no cost; `selected(Selected { id, tag,
path, value })` delivers each completed match in source order; `end()` is
called exactly once, when the document's `End` arrives. A route with no
specs is valid and delivers only `end()`. `Vec<Selected>` records;
`FnRoute` wraps a closure.

## TableRows/1

`TableEvent`: `Schema(&[PublicColumn])` once and first, `Row(&[Cell])`
as many as there are rows and each exactly as wide as the schema, `End`
once and last. Cells are `Null`, `Bool`, `Number { value, lexeme }`,
`String`, `Missing`. A renderer sees labels and cells, never source paths.

## The table transducer

`TableFromJson::new(TableBinding { schema, rows }, &Limits, Duplicates,
Arc<Metrics>, TableSink)` is a `Sink` built on a `Router`; `into_inner`
gives the table sink back.

- `Schema::Static(columns)`: the schema is emitted before the first row,
  or before `End` when there are no rows.
- `Schema::FromMetadata { columns, column }`: one `Materialize` capture
  under `max_metadata_bytes`; each element of the selected array goes
  through the mapper (`column_from_meta` reads `title` and a `path` of
  segments). Metadata selected twice, and a row that BEGINS before the
  metadata completed, are `INPUT_ORDER_VIOLATION`, the latter raised at
  the row's start. Metadata that never arrives is `INPUT_INVALID`.
- `Schema::Infer`: the first row's member names, in its order; a
  non-object first row is `INPUT_INVALID`. No rows gives an empty schema.

Rows are one `Materialize` capture under `max_record_bytes`, projected
by `Datum::get_path` into schema order, `Cell::from_datum` per column
(a container becomes its compact JSON text), and the column's
`MissingPolicy` for an absent path: `Missing` (the default, a
`Cell::Missing` for the renderer to map), `Null`, or `Error`
(`MISSING_VALUE` with the cell's path). `max_columns` applies to every
schema. Zero rows is a valid empty table. `End` is emitted only with the
document's `End`. The `rows` metric counts the rows delivered.

## Failure codes

See `Code::ALL` in `rs/src/error.rs` and the table in `AGENTS.md`. A
`Fail` carries `code`, `message`, and when they apply `path`, `limit`
(`{name, value}`), `row`, `col`, and `output` (`"partial"` or `"none"`).

## Limits

`Limits` fields, each named in a failure as written: `max_depth`,
`max_key_bytes`, `max_scalar_bytes`, `max_metadata_bytes`, `max_columns`,
`max_record_bytes`, `max_capture_bytes`, `max_output_bytes`. Sizes are
payload bytes plus `NODE_BYTES` per node. The line sources also apply
`max_record_bytes` to a record's source bytes as the record is read, so a
record never grows a chunk without bound: a line is taken from the reader
in pieces of at most its buffer and refused the moment it passes the
limit, whatever its length.

## Metrics

`Metrics` (shared through an `Arc`): `events`, `keys`, `scalars` (the
source), `rows` (the table transducer), `captured_bytes` and
`captured_bytes_high` (the router's one materialization at a time),
`retained_bytes_high`, `output_bytes` (a renderer's). `to_json` reports
them.
