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

`TreeContract` wraps a sink that takes the events as one tree (a render
that writes a document from them) and holds the stream to a tree's
contract: one root value, and in each object a key and then its value,
each key once. A value walked from a parsed tree keeps it by
construction; a parse streamed as it proceeds may not, since a part
streams every occurrence of a member its grammar reads twice (JSON's
`{"a":1,"a":2}`), and the TypeScript and Go ports' incremental source
refuses most shapes it cannot follow but not every one a grammar can
produce. A repeated key in one object fails with
`DUPLICATE_MEMBER`; a value where a key is due, a key outside an object,
a close with nothing open, a second root value or `End` inside an open
container fails with `STREAMABILITY_UNKNOWN`; both name the path of the
object concerned, and the event is not passed on. What follows is the
host's: aless falls back once to the parsed value when nothing has been
written. Each open object keeps the keys it has had, dropped when it
closes, so the cost is a lookup per key and the keys of the objects open
at once.

## Sources

Every source emits through `Guarded`, which enforces `max_depth`,
`max_key_bytes` and `max_scalar_bytes` as the events are produced, polls
the `AbortFlag` (a set flag fails the run with `ABORTED`) and counts the
source metrics (`events`, `keys`, `scalars`).

| Source | Input | Modes | Retains |
|---|---|---|---|
| `ValueSource(&Value)` | a parsed engine value | walk | the value (the caller's) |
| `ParserSource::new(Tabnas, &str)` | one text | parse, then walk (the TypeScript and Go ports also stream incrementally: "The rule-event inference, in TypeScript and Go", below) | the whole value |
| `LinesSource::new(BufRead, LineFormat)` | CSV | chunks of whole records (`Csv { header, options }`), each parsed with one reused grammar; a record ends where the grammar ends one, at any of its line characters outside a token. JSON Lines is read through tabnas-jsonl's part from a reader (`ParserEventSource::from_reader`), a window at a time | one chunk (`DEFAULT_CHUNK_BYTES`, 256 KiB, never a fraction of a record) |

`Source::run(self, &mut dyn Sink)` drives a borrowed sink.
`run_owned(self, sink) -> (Result<Flow, Fail>, sink)` and `run_boxed` take
the sink by value and hand it back; each source walks on both, the line
source its records and `ParserSource` its value.
`ParserSource::run_owned_with_value` also hands back the grammar's value.

Failure mapping: a sink's `Fail` comes back as it was; a sink that stopped
is `Ok(Flow::Stop)`; a parse the caller's `AbortFlag` cancelled is
`ABORTED` (from a line source, with the row the record or chunk it was
reading starts on and no column, since an abort lands between two of the
engine's steps, where it has no position of its own); any other engine error is `INPUT_INVALID` with the engine's
code in the message and its row and column (for a line source, the line's
number and the column within it), from every source alike: a run that
streams a document the grammar refuses, through a part, fails with the
same code and position after the prefix it streamed and before `End`. A `cancel` the caller did not ask for is
a guard the grammar installed (tabnas-json refuses nesting deeper than
128, below the default `max_depth` of 256, so that limit is unreachable
for JSON unless set lower), and the message says "the grammar stopped the
parse with a guard of its own" rather than reporting a cancellation.

A walk carries no number lexemes: every number it emits has
`lexeme: None`. The parser's rule passes carry a number's source text as
its lexeme when that text is a JSON number (RFC 8259's grammar) spelling
the value (below), so a part that yields the token keeps it, as JSON's
does.

### The parser's rule passes

`ParserEventSource::new(Tabnas, &str)` is the source of `ParserEvents/1`,
alchemy's protocol of the parser's rule passes (`tabnas_alchemy::shared::
parser_event`), the input a transducer part reads. It installs one
`rule_done` subscriber and relays each pass the engine completes, on the
thread that parses, into a `ParserSink`: `ParserEvent::Open` when a
rule's open pass has run, `Close` when its close pass has, and one `End`
once the engine has returned the parse's value. It knows no grammar, and
it has one mode. `run_owned(self, sink)` and `run_boxed` take the sink by
value and hand it back, with `ParserSource`'s failure mapping.

`ParserEventSource::from_reader(Tabnas, impl Read + Send)` is the same
source over a reader, which the parser reads with `Tabnas::parse_reader`
(admin ADR-30, tabnas/parser's `doc/chunked-source.md`). A grammar that
declares `lex.window` is read a window at a time, so a long document
streams holding a window of its text rather than all of it; any other
grammar reads the reader whole first. The passes, the value and the
failures are those of `new` over the same text. A reader that fails, or
bytes that are not UTF-8, end the run with `INPUT_INVALID`, carrying the
engine's `unreadable` and the row and column the source failed at. Only
Rust has it so far (`DIVERGENCE.md` 6).

A pass is a `RulePass`: the rule's `name`, depth `d` and instance `i`
(the engine's, which numbers rules from 1, the start rule's, in the order
it makes them); the alternate that matched (`p`, `r`, `b`, and its
groups `g`, sorted, since the engines keep them in different orders), or
none when the state declared no alternates; whether recovery forced the close; the named
counters `n`, sorted by name; the `tokens` the pass consumed; and, on a
close pass whose node is a scalar, its `value`. A pass's tokens are those
its alternate matched, less the `b` it gave back from their end, which
are the next rule's to consume, and none when `b` is more than it
recorded, since a close alternate can back up a token it did not record.
A token is its name, its value (`null` for a token with none, such as the
end of the source or a fixed token such as `[`), its source text and its
1-based row and column. A
number token's value carries its source text as its lexeme by the lexeme
rule above, and so does a close pass's number `value`, from the rule's
first open token.

A pass made while a probe is open is dropped: the grammar parses ahead
and rewinds past it (tabnas-bnf's probe, the engine's own for a
serialized grammar's `pd_` keys), so it is never the document's, and the
engine flags it (`RuleDone::probe`). The source holds `max_scalar_bytes`
on each token's value and source text and on a pass's value as it builds
the pass, so a huge token is refused by name before any sink sees it;
`max_depth` and `max_key_bytes` hold where a part's outputs become
`JsonEvents`. It polls the `AbortFlag` per pass and counts the events it
delivers in `Metrics::events`.

### Pruning behind the parser's passes

The engine builds its value as it parses, so a `ParserEventSource` over a
long document leaves the engine holding every record in its tree, though
nothing a pass carries is a container of the tree (a pass's `value` is a
scalar or nothing) and nothing reads the value the parse returns.
`ParserEventSource::prune()` empties each array behind the stream: once
the sink has seen a close pass whose rule's node is an array, the source
empties that array in place (in Go, in every rule that builds into it,
by `tabnas.Rule.SetNode`, its elements cleared first, since a truncated
slice keeps its backing array), so the tree holds the elements of the
passes since the last close on it and no more. Rust leaves an array also held
elsewhere as it is, since emptying the node's copy would free nothing and
would copy it. `run_owned_with_value` (`RunWithValue`, `runWithValue`)
hands back the tree the run left, which exists for a test to measure.

Pruning alters the value the engine returns, so it is sound only for a
grammar whose passes do not depend on what its arrays hold, and it is a
verified capability. `capability::PRUNABLE`
lists `csv`, `feed`, `ini`, `json`, `json5`, `jsonc`, `jsonic`, `jsonl`,
`markdown`, `pgn`, `xml`, `yaml` and `zon` (Go's omits `pgn`, which its
suite does not run). `rs/tests/prune_test.rs` and its ports run every
document each grammar reads, pruned and not: the committed fixtures, the
generated worked-example documents, and every document of the grammar's
own `test/spec` corpus. A listed grammar's pruned run relays the same
passes, pass for pass in the fixture encoding, and ends the same way, with
one difference the contract allows: a token whose value is a container of
the tree carries what the tree holds when its pass is made, so the arrays
in it may have been emptied, while its kind and its maps' members are as
they were. yaml's lexer resolves an alias so, to the anchored node (`a:
&items [1, 2]` and then `b: *items`), and yaml's part reads such a token's
kind alone, refusing an alias to a sequence or a mapping as one whose
members the passes do not show. A part that read the elements of a
token's arrays would read what pruning left, so none does. `toml` is not
listed: its message for a key defined twice quotes the earlier value,
which pruning empties (`cannot define arr, it already has the value []`).
The suite asserts the list in both directions, and measures what pruning
is for: ten times the records of a JSON, JSON Lines, CSV, YAML, XML or PGN
document leave the pruned tree holding as many elements as before (six
for the worked example's JSON, four for its XML), where the tree without
pruning holds every record.

A run that prunes is gated on the list by the grammar's name
(`ParserEventSource::grammar(name)`): without a listed name it fails with
`STREAMABILITY_UNKNOWN` before the parse, having sent nothing. `ParserEventSource::unverified()` lifts the
gate for the suite. A run that does not prune needs no name.

### The native transduce-tree

`TransduceTree::new(TreeSpec, Sink)` is alchemy's `transduce-tree` run
natively: a `ParserSink` that writes a tree's `JsonEvents/1` to its sink
as the spec, keyed by rule name, says. `routers()` offers it to alchemy
(`Routers::transduce_tree`), and alchemy's lowering asks for it where a
program applies the library's `transduce-tree` to the host's parser
events with a spec of the standard shape, the source's guard in front of
what it writes. The library's text, `stdlib/transduce.alc`, is the
specification, and alchemy's `TreeSpec` documents what the native does
pass for pass: an open pass announces its `key`, then begins its
`container` or yields its `scalar` from one of its tokens, then announces
its `member`; a close pass yields its `text`, when its rule is a `:text`
rule, then ends its container, or yields its own `value`; a container
rule replaced by a container rule is one container. A `:text` rule's text
is the source text of the tokens the passes between its open and its
close consumed, in order, and a text closed inside another joins it.
Its state is the count of open containers, whether a close handed its
container on, and the text each open `:text` rule has gathered, so it
retains the document's nesting and an open rule's text, and nothing else
of its width or length. A text is held to the spec's `max_text_bytes`,
which the lowering sets to `max_scalar_bytes`, at the joins the library's
`string-join` makes and in their order: a pass's tokens together, then
the innermost text with them, and a closed text with the text around it.
`routers()` says the native reads `:member` and `:text`
(`Routers::transduce_tree_reads_member_and_text`), which is what lets the
lowering hand it a spec that uses either; routers that do not say so get
the library's text for such a spec. A pass's events are worked out
before the first is written, and its failures are the library's, in its
order: `DSL_TYPE_ERROR` for a key or a member that is not a string, a
scalar that is a container, a close that ends a container with nothing
open and a `:text` rule's close with no text open,
`RESOURCE_LIMIT_EXCEEDED` naming `max_scalar_bytes` for a text past the
bound, `PROTOCOL_ORDER_ERROR` for passes that end inside a container. A close
that hands its container on ends none, so it is not held to an open one,
in the library's text or here; the passes a source delivers always open
a rule before they close it. alchemy-cli's differential test
holds it to the library's text.

### The native transduce-records

`TransduceRecords::new(RecordsSpec, Sink)` is alchemy's
`transduce-records` run natively: a `ParserSink` that writes the
`JsonEvents/1` of a document of records to its sink, as the spec says.
The spec names the rule whose passes begin and end the document, the
rule whose passes begin and end a record, and the rule whose close
pass's value is the next cell. It also names a group whose alternates
are empty cells, the scalar an empty cell holds, whether the first
record names the columns, and the prefix of a name past them.

`routers()` offers it to alchemy (`Routers::transduce_records`), and
alchemy's lowering asks for it where a program applies the library's
`transduce-records` to the host's parser events with a spec of the
standard shape, the source's guard in front of what it writes.
tabnas-csv's transducer part is such a spec. The library's text,
`stdlib/transduce.alc`, is the specification, and alchemy's
`RecordsSpec` documents what the native does pass for pass.

- With a header, each record after the first is an object. Its cells
  are keyed by the header's names in order, a cell past them by the
  prefix and its position from 0, and a name it lacks holds the fill.
- Without one, each record is an array of its cells.
- A header that names a column twice streams both cells, where a walk
  of the grammar's value keeps the last, so a tree's contract after it
  refuses the document.
- Its state is the header's names and a count of the current record's
  cells, held to `max_metadata_bytes` by the measure the library's scan
  holds its state to, so the two refuse a header at the same pass.

`rs/tests/records_test.rs` holds it, over the csv grammar's own passes
with csv's part as its spec, to the walk of the value the parse returns:
every document of tabnas-csv's corpus, the committed CSV fixtures and
the generated records, from the text and from a reader a few bytes at a
time. Only Rust has it; TypeScript and Go run the library's text.

### The rule-event inference, in TypeScript and Go

Until step 13 of alchemy's streaming plan, `ParserSource` had a second
mode in every port, which streamed a document as the parse proceeded by
inferring its events from the value cells the grammar's rules built. The
Rust crate retired it then: every format its list named ships a
transducer part, or reads whole as markdown's manifest declares, and a
host reads a format through its part. The TypeScript and Go ports keep
it, since their JSON Lines line sources read through it until their
engines read a reader a window at a time, and this section is theirs:
`new ParserSource(parser, text).grammar(name).mode(SourceMode.incremental(prune))`
in TypeScript, `NewParserSource(parser, text).Grammar(name).Mode(IncrementalMode(prune))`
in Go, whose adapter is built with the `tabnas_nodecell` build tag
alone. An incremental run retains the engine's parse state and, with
pruning, not the streamed elements.

An incremental run whose rule events did not amount to one whole document
is `STREAMABILITY_UNKNOWN`, and so is one the adapter refuses because the
grammar wrapped a value already streamed as the root in a list, or
rewrote a map after it was streamed; the message names the shape and
says to run it materialized. An incremental parse that returns without
one event (YAML's empty document is `null`) has its value walked
instead, so it completes as the walk does. A document the grammar
refuses fails with the walk's code and position after a protocol-valid
prefix and before `End`.

Pruning under a selector empties the array whose elements the selector
names (a trailing `[*]` names the elements; without one it names the
array); pruning all arrays empties every array; never pruning leaves the
tree alone. Pruning truncates the engine's own array through the shared
node cell after its new elements were emitted, so it alters the value the
engine returns, which the incremental source discards. It is never
applied in the materialize mode.

Number lexemes are best effort: the adapter keeps the first token's source
text of a rule whose node is a `Number` when that text is a JSON number
(RFC 8259's grammar) that parses to the node's value, and attaches it to
the number when it is inserted next. A number the adapter learns of any
other way has no lexeme.

The grammars it streams are a verified capability, never an assumption.
TypeScript's `capability.INCREMENTAL` lists `json`, `json5`, `jsonc`,
`jsonic`, `jsonl`, `markdown`, `pgn`, `xml`, `yaml` and `zon`, each by the name of
the format it reads: its crate's, but `pgn` for tabnas-chess, which
builds each game on a stack in the parse's context and stores it in the
database only once complete, so a database streams a game at a time, each
game walked whole as it is stored. That holds from tabnas-chess 0.1.17
(earlier releases attached a game's parts after the adapter had sent
them), and transduce does not depend on chess, so a host that streams PGN
takes 0.1.17 or later. Go's (`IncrementalGrammars()`) omits `pgn`
([`DIVERGENCE.md`](../DIVERGENCE.md) 5). `ts/test/incremental.test.ts`
and `go/incremental_test.go` run every fixture each grammar reads through
both modes and assert the list in both directions; a grammar is listed only when no fixture
mismatches or is refused for how it builds, and at least one streams to
the end. What the list promises, precisely: for a listed
grammar an incremental run either streams exactly what the walk would
(number lexemes aside), or streams every occurrence of a repeated member
name so that a `LastWins` router builds the walk's value, or fails before
`End` with a documented code; it never completes a stream the walk
contradicts. The refusals are the shapes the rule events cannot follow: a
container that wraps a value already streamed as the root (a YAML stream
of several documents, `a: 1` then `---` then `b: 2`; a jsonic top-level
implicit list whose first element is a container, `{a:1}` on one line and
`{b:2}` on the next), and a map the grammar rewrote after it was streamed
(a YAML `<<` merge key, resolved when the mapping closes), and a
container the grammar opens inside a map before the member's key, whose
events would leave before the key, all `STREAMABILITY_UNKNOWN` after the
first value's events; and a repeated member whose containers the grammar
merged, `DUPLICATE_MEMBER`, below. The container-before-key refusal is a
net, not a licence: the suite counts a fixture refused that way against
the grammar, since a grammar that builds its members so does it for
every document, and a listed grammar does it on no fixture. The
imperative grammars below trip it on their own samples. yaml tripped it
on documents no fixture held until tabnas/yaml#107: it named the first
member of a mapping that starts in a sequence entry in the same pass
that opened the mapping, so when that member's value was a collection,
the value opened before the adapter had its key (`- ? earth: blue` with
`: moon: white` under it, tabnas/transduce#7, but equally `- a:` with a
block mapping under it, or `[a: {b: 1}]`). The grammar now names that
member before its value's rule opens, and each of those documents
streams as the walk. A
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
both modes: its guard fails the pair's close before the assignment, so the
incremental run fails with the grammar's `INPUT_INVALID` at the same
position as the walk, after a protocol-valid prefix that ends with the
repeated `Key` (or the second value's own container, streamed as the
grammar built it). The map's earlier value is not streamed in its place:
a member whose announcing rule closed without growing the map is held
until the engine's next event, which a pass that failed never sends.

`markdown` builds its nodes imperatively, but each lands whole and is
walked at its insertion, so its events are the walk's. `xml` builds each
element in document order, in a cell of its own made when its start tag
is read, its members in their order, with `children` named in `u.key`
before its list opens and the list in a cell of its own; an element's
`attributes`, and a self-closing element's empty `children`, land
complete and are walked at their insertion, as markdown's nodes are. The
other imperative grammars (`toml`, `ini`, `csv`, `feed`) build their
values in ways the rule events do not show (`csv` streams its header and
raw rows as extra elements, a well-formed stream with the wrong shape;
`toml` and `ini` open a section's container before its key, which the
adapter refuses on their own samples; `feed` streams xml's element tree
and then replaces the root with the feed it makes of it, so each of its
documents is refused as a rewritten root when the root closes) and are
walked whole. `ParserSource` refuses to run an unlisted grammar
incrementally: the grammar's name cannot be read from the parser (the
json and jsonl parsers register no plugin), so the incremental mode needs
the grammar's name (`.grammar(name)`, `.Grammar(name)`) and fails with
`STREAMABILITY_UNKNOWN` before the parse, emitting nothing, when the name
is missing or unlisted. `unverified()` (`Unverified()`) lifts that gate
for the differential suite, which is how an unlisted grammar's events
get measured at all.

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
- `Schema::Infer`: the columns are the first row's, by its kind. An
  object row gives its member names, in its order, each sourced at its
  key; an array row gives positional columns labelled `0`, `1`, ... up
  to its length, each sourced at its index (an empty array row gives a
  table of no columns, as no rows does, whose rows are empty; whether
  that can be written is the renderer's to say, as it is for the empty
  table); a scalar row (null, a boolean, a number or a string) gives one
  column, `value`, sourced at the row itself. A later row of any kind
  projects through those paths as a static schema's rows do, so a row of
  another kind than the first lands empty where its paths miss (a key
  path on an array or a scalar, an index path on an object or a scalar),
  under the column's missing policy; a mixed table is not refused, and
  the `value` column's empty path finds every row, a container as its
  compact JSON text. The labels are the table's metadata for as long as
  it lasts, so they are bound under `max_metadata_bytes`, measured as
  the array of their strings would be, positional labels and `value`
  included, as well as under `max_columns`. No rows gives an empty
  schema.

Rows are one `Materialize` capture under `max_record_bytes`, projected
by `Datum::get_path` into schema order, `Cell::from_datum` per column
(a container becomes its compact JSON text), and the column's
`MissingPolicy` for an absent path: `Missing` (the default, a
`Cell::Missing` for the renderer to map), `Null`, or `Error`
(`MISSING_VALUE` with the cell's path). `max_columns` applies to every
schema. Zero rows is a valid empty table. `End` is emitted only with the
document's `End`. The `rows` metric counts the rows delivered.

## Failure codes

See `Code::ALL` in alchemy's [`rs/src/shared/error.rs`](https://github.com/tabnas/alchemy/blob/main/rs/src/shared/error.rs) and the table in `AGENTS.md`. A
`Fail` carries `code`, `message`, and when they apply `path`, `limit`
(`{name, value}`), `row`, `col`, `file` (the source the position is in,
when the program was compiled from several), and `output` (`"partial"`
or `"none"`).

## Limits

`Limits` fields, each named in a failure as written: `max_depth`,
`max_key_bytes`, `max_scalar_bytes`, `max_metadata_bytes`, `max_columns`,
`max_record_bytes`, `max_capture_bytes`, `max_output_bytes`. Sizes are
payload bytes plus `NODE_BYTES` per node. The line source also applies
`max_record_bytes` to a record's source bytes as the record is read, so a
record never grows a chunk without bound: the input is read in pieces
that end at the grammar's line endings, a lone `\r` or a configured
separator as much as `\n`, so the limit is each record's however many
records one `\n`-terminated line holds, and a record is taken a buffer at
a time and refused the moment it passes the limit, whatever its length.

## Metrics

`Metrics` (shared through an `Arc`): `events`, `keys`, `scalars` (the
source), `rows` (the table transducer), `captured_bytes` and
`captured_bytes_high` (the router's one materialization at a time),
`retained_bytes_high`, `output_bytes` (a renderer's). `to_json` reports
them.

## Shared fixtures

`test/spec/*.tsv` is the behaviour every runtime of this crate must
reproduce, row for row, but `events.tsv`'s `incremental` rows, which are
the TypeScript and Go ports' alone since step 13. The Rust runners are `rs/tests/spec_<file>.rs`
over the harness in `rs/tests/common/mod.rs`, through `tabnas-support`'s
`Runner`; a TypeScript or Go port runs the same files through its own
half of `@tabnas/support`, with a harness that does what this section
says. `spec_events.rs` also fails when a file appears without a runner.

The format is the fleet's: a header row names the columns, a line that
starts with `#` and holds no tab is a comment, the `input` (or `script`)
column is escape-decoded (`\n`, `\r`, `\t`, `\\`; every other backslash
sequence stays as written, so a JSON escape such as `é` reaches the
grammar intact) and every other column is read raw. A trailing column a
row leaves out is empty. The `expected` column is JSON, compared
structurally (`-0` is not `0`), or `ERROR:<CODE>`, optionally
`ERROR:<CODE>@<row>:<col>` to pin the 1-based position the failure
reports.

### Columns

| Column | Meaning |
|---|---|
| `input` | the source text |
| `grammar` | `json`, `jsonl`, `json5`, `jsonc`, `jsonic`, `yaml`, `zon`, `csv`, `toml` or `ini`; in `parser-events.tsv`, also `spec`, the serialized grammar in `document` |
| `document` | `parser-events.tsv`, grammar `spec`: a serialized grammar, installed with `Tabnas::grammar_json` |
| `mode` | `materialize`: `ParserSource` over the grammar, parsed and walked (the TypeScript and Go harnesses name the grammar with `.grammar(name)` in that mode). `part`: JSON's transducer part, natively (`TransduceTree` with the spec `tree.tsv`'s json rows give), over the passes `ParserEventSource` delivers, behind `Guarded`, its texts held to `max_scalar_bytes`: the document's events as a host reads JSON, lexemes kept; JSON only. `incremental`: the TypeScript and Go ports' incremental mode ("The rule-event inference, in TypeScript and Go"), the grammar named with `.grammar(name)`; the Rust runner skips these rows by their mode, since the crate retired the mode at step 13. `value`: the grammar's parse (an engine error is `Fail::from_tabnas`), then `ValueSource` over the value, which applies no limits. `lines`: `LinesSource` (`csv`) on its walking path, `Source::run`. `lines-incremental`: `LinesSource::run_owned`, which walks the records too |
| `prune` | `incremental` rows only: `"all"` to prune every array, a selector to prune the arrays it names, empty to prune none |
| `options` | the line source: a JSON object of `header` (default `true`), `object`, `number`, `value`, `trim` and `strict` for the CSV grammar, and `chunk_bytes` for the source |
| `captures` | a JSON array of `{"tag", "select", "mode"}`, `select` a selector, `mode` `materialize` (the default) or `observe` |
| `binding` | `{"schema": ..., "rows": selector}`, the schema `"infer"`, `{"metadata": selector}` (each descriptor read by `column_from_meta`) or `{"static": [{"label", "source": segments, "missing"}]}`, `missing` `missing` (the default), `null` or `error` |
| `duplicates` | `reject` (the default), `last_wins` or `first_wins` |
| `stage` | `limits.tsv`: `events`, `route` or `table`, the pipeline `events.tsv`, `route.tsv` or `table.tsv` runs. `parser-events.tsv`: `passes`, every event in the encoding below; `tokens`, each pass as `["rule:open", [[name, val, src, row, col]...]]`, its tokens as that encoding writes them; or `names`, each pass as `"rule:open"` or `"rule:close"`; the end is `"end"` in the last two |
| `spec` | `tree.tsv`: a JSON object of rules, each of `container` (`"object"` or `"array"`), `key` (a string, or a token's index) and `scalar` (a token's index, or `"value"`) |
| `limits` | a JSON object of `Limits` fields over `Limits::default()` |
| `path` | on an error row, the failure's `path`, in jq syntax |
| `limit` | on an error row, the `Limits` field the failure names |
| `prefix` | on an error row, the events the source emitted before it failed |

A selector is a JSON array of steps: a string is `Property`, a
non-negative integer `Index`, `{"each":"index"}` `EachIndex` and
`{"each":"member"}` `EachMember`; `[]` is the root. A path of segments
(a static column's `source`) has strings for keys and integers for
indexes.

### Encodings

An **event** is an array whose first element names it:
`["object_start"]`, `["object_end"]`, `["array_start"]`,
`["array_end"]`, `["key", name]`, `["null"]`, `["bool", b]`,
`["string", text]`, `["number", value, lexeme]` and `["end"]`. The
`value` is the number as a JSON number; the `lexeme` is the source text
the event carried, or `null`. Strings and keys are the decoded text.
`events.tsv` and `lines.tsv` expect the whole stream; a `prefix` is the
same encoding.

A **value** (a `Datum`, what a capture materializes) is `null`, `true`,
`false` or a string as itself, `["number", value, lexeme]`,
`["array", value...]` or `["object", [key, value]...]`, so member order
and lexemes are compared, not only the JSON they print as.

`route.tsv` expects the **deliveries** in order: `[tag, path]` for an
observed capture, `[tag, path, value]` for a materialized one, the path
in jq syntax, and then the string `"end"` when the router called
`RouteSink::end`.

`table.tsv` expects `TableRows/1` in order: `["schema", [label...]]`,
`["row", [cell...]]` per row and `["end"]`. A **cell** is `null`, a
boolean, a string, `["number", value, lexeme]` or `["missing"]`; a
container projected into a cell is the string of its compact JSON.

`parser-events.tsv` expects **parser events** in alchemy's fixture
encoding (`ParserEvent::to_json`): `["open", rule]`, `["close", rule]`
and `["end"]`, the rule an object of `name`, `d`, `i`, `alt` (`p`, `r`,
`b`, `g`; left out when the state declared none), `tokens` (each `[name,
value, src, row, col]`), `forced`, `n` and, on a close pass with a scalar
node, `value`. The `passes` stage leaves `i` out, and its rows hold no
fixed token, since the engines differ on both. A token's value is
written as JSON, so its lexeme is not in the encoding; `tree.tsv`, whose
events carry it, pins it. `tree.tsv`
runs `ParserEventSource` into `TransduceTree` behind `Guarded`, as a
program's `transduce-tree` is lowered, and expects the event stream.

`scan.tsv` drives `ScanEmit` with a running sum: an integer item adds
itself and emits `"+<n>"`, `{"emit": [...]}` emits those strings,
`{"fail": CODE}` fails the step with that code, and `"finish"` calls
`finish`, which emits `"=<sum>"`. The output sink answers `Stop` for the
output named in `stop_on`. The expected value is `{"out": [...],
"flows": [...]}`, every output delivered and the `Flow` each operation
returned (`"continue"` or `"stop"`).

### The files

| File | Rows | What it pins |
|---|---|---|
| `events.tsv` | 84 | `JsonEvents/1` from the walk, and from the TypeScript and Go ports' incremental mode over the grammars their lists verify: lexemes, string decoding, repeated members, the refusals and their prefixes, the gate on unverified grammars, pruning |
| `route.tsv` | 36 | the router: the worked example, every step kind, observed nesting, overlap refusals, jq paths, the duplicates policies |
| `table.tsv` | 34 | `TableFromJson`: metadata, static and inferred schemas (inferred from a first row of every kind: an object's members, an array's positions, a scalar's `value`, and a later row of another kind projected through them), `INPUT_ORDER_VIOLATION`, `MISSING_VALUE` and the missing policies, empty tables, container cells, descriptor errors |
| `lines.tsv` | 29 | `LinesSource`: CSV on both paths, chunk sizes, headers, grammar options, a failure's file line, records cut where the grammar cuts them (a quote inside a field, the engine's own strings, the header as the first record the grammar reads, a lone CR) |
| `scan.tsv` | 12 | `scan-emit`: output order, `finish` once, `Stop`, a step's failure |
| `parser-events.tsv` | 8 | `ParserEventSource`: every field of a pass but `i`, the groups sorted, tokens given back by `b` and their positions, a token with no value, a scalar node's value, a refused document's prefix, `max_scalar_bytes` on a token, a probe's passes dropped (tabnas-abnf's compiled `top = [ X "@" ] Y`) |
| `tree.tsv` | 18 | `TransduceTree` over tabnas-json's passes: the json part's spec and its lexemes, a close pass's value, a container handed on through a replaced rule, the library's refusal of a key that is not a string, the guard in front, a rule's text and a text joined to the one around it, a member after its container's start, the refusal of a member that is not a string, a text held to `max_scalar_bytes` at each of the library's joins and not refused at the bound |
| `limits.tsv` | 62 | each `Limits` field passed and not passed, by name, with UTF-8 byte counts at multibyte boundaries, a token's source text held to `max_scalar_bytes` through a part, and a line source's limit on each record a line holds |

### What a port must reproduce exactly

- **Sizes are UTF-8 bytes.** A key or string counts its encoded length
  (`é` is 2, `日` 3, `😀` 4); a number its lexeme's length, which is 0
  to `max_scalar_bytes` and 8 to a retained value when there is none; a
  retained node adds `NODE_BYTES` (16). A port on UTF-16 strings that
  counts code units fails the multibyte rows of `limits.tsv`. Through a
  part, the source also holds each token's source text, quotes and
  punctuation included, to `max_scalar_bytes` before the part sees the
  pass: `["日本"]` is 6 bytes to the guard and 8 to the source.
- **A line source's record counts its line ending**: `\n`, `\r\n`, a lone
  `\r` or a configured separator, and under `record.empty` the whole line
  token the lexer reads. So a record that fits the limit without its
  ending fails with it (`limits.tsv` pins the bound), and a line holding
  many records is held to the limit one record at a time.
- **The lexeme rule**: a rule pass's number token, and a number the
  TypeScript and Go ports' incremental source streams, carries its source
  text only when that is an RFC 8259 number that reads as the value
  (`1e2` keeps `"1e2"`; `0x1F`, `+1`, `.5`, `5.` and YAML's `012` have
  none), and a value the incremental source walks (a YAML alias, a
  document stream with nothing streamed early) has none.
- **jq paths**: a key is written bare (`.a_1`) when it is an ASCII letter
  or `_` followed by ASCII letters, digits and `_`, and otherwise as a
  JSON string (`."odd key"`, `."é"`, `."1a"`, `.""`), escaped as RFC 8259
  requires: `\"`, `\\`, the short forms `\b \f \n \r \t`, `\u00XX` with
  lowercase hex for other control characters, and nothing else.
- **Delivery order**: matches complete in source order; observed
  captures that end at the same value are delivered innermost first, the
  later-begun first when two select the same value.
- **A container cell** is the compact JSON of the value, with each
  number's lexeme when it has one. A number without one is written as the
  runtime prints a double, which differs between runtimes outside
  `1e-6 <= |x| < 1e21`; `DIVERGENCE.md` records it, and no fixture puts
  such a number in a cell.
