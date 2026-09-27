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
| `ParserSource::new(Tabnas, &str)` | one text | `SourceMode::Materialize`: parse, then walk. `SourceMode::Incremental { prune }`: the rule-event adapter, for the grammars `capability::incremental` lists | materialize: the whole value; incremental: the engine's parse state and, with pruning, not the streamed elements |
| `LinesSource::new(BufRead, LineFormat)` | JSON Lines or CSV | one record per line (`Jsonl`) or chunks of whole records (`Csv { header, options }`), each parsed with one reused grammar | one line, or one chunk (`DEFAULT_CHUNK_BYTES`, 256 KiB, never a fraction of a record) |

`Source::run(self, &mut dyn Sink)` drives a borrowed sink and is always
the walking path (`ParserSource` materializes whatever its mode, and the
JSON Lines source walks each line). `run_owned(self, sink) -> (Result<Flow,
Fail>, sink)` and `run_boxed` take the sink by value, which is what the
engine's `Fn + Send + Sync + 'static` subscriber needs, and hand it back:
that is the incremental path, and the one where JSON Lines numbers keep
their lexemes.

Failure mapping: a sink's `Fail` comes back as it was; a sink that stopped
is `Ok(Flow::Stop)`; a parse the caller's `AbortFlag` cancelled is
`ABORTED`; any other engine error is `INPUT_INVALID` with the engine's
code in the message and its row and column (for a line source, the line's
number and the column within it). An incremental run whose rule events
did not amount to one whole document is `STREAMABILITY_UNKNOWN`; that is
what a grammar outside the verified list produces, and the message says
to run it materialized.

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

`capability::INCREMENTAL` lists `json`, `json5`, `jsonc`, `jsonl`, `yaml`
and `zon`. `rs/tests/incremental_test.rs` runs every fixture each grammar
reads through both modes and asserts the list in both directions. `jsonic`
is not listed: its top-level implicit lists whose first element is a
container (`{a:1}` on one line, `{b:2}` on the next) wrap a value that has
already been streamed as the root, and the incremental source refuses them
with `STREAMABILITY_UNKNOWN`. The imperative grammars (`toml`, `ini`,
`csv`, `xml`, `markdown`, `feed`) build their values in ways the rule
events do not show and are walked whole.

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
`max_record_bytes` to a record's source bytes before parsing it, so a
record never grows a chunk without bound.

## Metrics

`Metrics` (shared through an `Arc`): `events`, `keys`, `scalars` (the
source), `rows` (the table transducer), `captured_bytes` and
`captured_bytes_high` (the router's one materialization at a time),
`retained_bytes_high`, `output_bytes` (a renderer's). `to_json` reports
them.
