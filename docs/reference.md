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
`{"a":1,"a":2}`). A repeated key in one object fails with
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
| `ParserSource::new(Tabnas, &str)` | one text | parse, then walk | the whole value |

JSON Lines and CSV are read through their parts from a reader
(`ParserEventSource::from_reader`), a window at a time: each part is
alchemy's text (tabnas-jsonl's over the library's `transduce-tree`,
tabnas-csv's over its `transduce-records`), run by alchemy's runtime in
the host, with the passes this source delivers as its input; this crate
has no implementation of a part. The line source that read them a record
or a chunk of records at a time was retired at step 12 of alchemy's
streaming plan, in the Rust crate and then, once their engines read a
reader a window at a time, in the TypeScript and Go ports.

Until step 13, `ParserSource` had a second mode in every port, which
streamed the grammars a differential suite had verified
(`capability::INCREMENTAL`) by inferring a document's events from the
value cells the grammar's rules built. Every format on that list ships a
transducer part now, which declares its events where the inference
guessed them, or reads whole as markdown's manifest declares, and a host
reads a format through its part; the Rust crate retired the inference,
its list and its suite at step 13, and the TypeScript and Go ports with
their line sources (`git log` has them).

`Source::run(self, &mut dyn Sink)` drives a borrowed sink.
`run_owned(self, sink) -> (Result<Flow, Fail>, sink)` and `run_boxed` take
the sink by value and hand it back; `ParserSource` walks its value on
both.
`ParserSource::run_owned_with_value` also hands back the grammar's value.

Failure mapping: a sink's `Fail` comes back as it was; a sink that stopped
is `Ok(Flow::Stop)`; a parse the caller's `AbortFlag` cancelled is
`ABORTED`; any other engine error is `INPUT_INVALID` with the engine's
code in the message and its row and column, from every source alike: a run that
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
engine's `unreadable` and the row and column the source failed at. The
TypeScript port's is `ParserEventSource.fromReader(parser, reader)`,
over the engine's synchronous pull reader (`{ read(buf: Uint8Array):
number }`, which writes up to `buf.length` bytes and answers how many, 0
at the end, as `fs.readSync(fd, buf)` does), read with `tn.parseReader`;
Go's is `ParserEventSourceFromReader(parser, r)`, over an `io.Reader`,
read with `ParseReader` (tabnas-parser 0.12.16, admin ADR-33). The
pruning, the abort flag, the limits and the metrics hold as over a text.
Which grammar reads a window at a time is the grammar's: in Rust
tabnas-json, tabnas-jsonc, tabnas-jsonl and tabnas-csv declare
`lex.window`; no TypeScript or Go grammar package declares it yet, so
there a grammar reads a reader whole unless its host sets the option, as
the ports' reader tests set it for JSON, JSONC and JSON Lines.

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
end of the source or a fixed token such as `[`), its source text, its
1-based row and column, and its byte offset: where it starts, in UTF-8
bytes from the document's start, the same in every runtime. Rust's engine
gives it (`site.si`) and Go's counts bytes (`SI`); TypeScript's counts
UTF-16 units, so the TypeScript source converts each position as the
stream goes, scanning the text once in order (over a reader, the text it
decodes as the engine reads it, kept from the oldest position still to be
asked, never the document). A
number token's value carries its source text as its lexeme by the lexeme
rule above, and so does a close pass's number `value`, from the rule's
first open token.

A sink that asks for them (`ParserSink::wants_tokens`; `wantsTokens` in
TypeScript; Go's `shared.TokenSink`), or a source told to send them
(`ParserEventSource::tokens()`), also gets a `ParserEvent::Token` for
each token the lexer gave and the parser did not take: one of the
grammar's ignored tokens (its `IGNORE` token set: white space, a line
break, a comment), a `PassToken` as a pass's are. The source then
subscribes to the engine's lexer (`subscribe_lex`; `sub({ lex })`; `Sub`)
and holds each ignored token from the moment the lexer cuts it until the
first pass that consumed a token after it, which it sends it before;
those still held when the parse returns go before the `End`. So a token
item is in source order among the passes, and the source holds no more
than the ignored tokens between two passes. A token lexed while a probe is
open is the document's all the same, unlike the probe's passes: a probe's
rewind replays the tokens the parser consumed from the engine's history,
and the lexer never cuts that text again, so a token item dropped there
would be lost. A token the lexer cuts again (a relex re-cuts the text
after a token it renegotiates) starts before the furthest one taken and
is dropped as the one already held. A sink that does not ask gets the
passes alone, and the source subscribes to no lexer event for it; alchemy
asks for them for a program that reads `parser-events input :tokens`.

A pass made while a probe is open is dropped: the grammar parses ahead
and rewinds past it (tabnas-bnf's probe, the engine's own for a
serialized grammar's `pd_` keys), so it is never the document's, and the
engine flags it (`RuleDone::probe`). The source holds `max_scalar_bytes`
on each token's value and source text and on a pass's value as it builds
the pass, so a huge token is refused by name before any sink sees it;
`max_depth` and `max_key_bytes` hold where a part's outputs become
`JsonEvents`. It holds `max_scalar_bytes` on a token item's value and
source text as on a pass's token, when the lexer cuts it. It polls the
`AbortFlag` per pass and counts the events it delivers, token items
among them, in `Metrics::events`.

### Pruning behind the parser's passes

The engine builds its value as it parses, so a `ParserEventSource` over a
long document leaves the engine holding every record in its tree, though
nothing a pass carries is a container of the tree (a pass's `value` is a
scalar or nothing) and nothing reads the value the parse returns.
`ParserEventSource::prune()` empties the tree behind the stream. Once the
sink has seen a close pass:

- whose rule's node is an array, the source empties that array in place
  (in Go, in every rule that builds into it, by `tabnas.Rule.SetNode`, its
  elements cleared first, since a truncated slice keeps its backing
  array), so the tree holds the elements of the passes since the last close
  on it and no more;
- whose rule's node is an element of an array its parent rule's node holds
  (directly, or as a member's or an element's value), the source sets the
  elements before it to nothing (`Undefined`, `undefined`, `nil`): an array
  no rule's node is, which the first case never reaches, such as the one a
  toml document's root holds its array of tables in. Each element keeps its
  place, so a grammar that finds a node by position still finds it. The
  node is looked for among the last eight places of the parent's node, its
  members and its elements, where a grammar appends it, and each element is
  set once, so a close costs the same however long the document grows;
- whose rule's node is a map, for a grammar `capability::PRUNES_MAPS`
  lists (`PRUNES_MAPS`, `PrunesMaps` in Go), the source deletes the map's
  members too, so an INI document's sections and a JSON object's members
  go as an array's elements do (tabnas/transduce#81).

Rust leaves a container also held elsewhere as it is, since emptying the
node's copy would free nothing and would copy it. `run_owned_with_value`
(`RunWithValue`, `runWithValue`) hands back the tree the run left, which
exists for a test to measure. What a grammar keeps outside its tree the
source cannot reach: Rust's toml finds each table by its path from the
document's root cell and keeps entries per table in the context's bag
(tabnas/toml#121), and markdown's block phase keeps every block it made
(tabnas/markdown#100).

A grammar holds the pruning off while it still needs what its tree
holds. It raises a count, `pruneHold`, in the parse context's bag
(`Context::u` in Rust, `Context.U` in Go, `ctx.u` in TypeScript; the
source exports the key as `PRUNE_HOLD`, `PruneHold` in Go), and the source
empties nothing while the count is above zero. yaml raises it when a value
with an anchor opens and lowers it once the anchor is recorded (tabnas-yaml
0.5.28), so the node an alias copies is the whole of what the document
anchored: an alias used as a key is that node's text (`1,2` for `&a [1,
2]`), pruned or not.

Pruning alters the value the engine returns, so it is sound only for a
grammar whose passes do not depend on what its tree holds, and it is a
verified capability. `capability::PRUNABLE` lists `abnf`, `css`, `csv`,
`ebnf`, `feed`, `gbnf`, `ini`, `json`, `json5`, `jsonc`, `jsonic`,
`jsonl`, `markdown`, `pgn`, `toml`, `xml`, `yaml` and `zon` in every
runtime, and `capability::PRUNES_MAPS` lists `abnf`, `css`, `csv`, `ebnf`,
`gbnf`, `ini`, `json`, `json5`, `jsonc`, `jsonic`, `jsonl`, `markdown` and
`pgn`. TypeScript's `PRUNES_MAPS` omits `abnf`, `css`, `ebnf` and `gbnf`,
whose TypeScript ports fail once a map's emptying deleted a list member
they append to (`DIVERGENCE.md` 7). `rs/tests/prune_test.rs` and
its ports run every
document each grammar reads, pruned and not, and again with its maps
emptied too (`unverified_maps`): the committed fixtures, the generated
worked-example documents (among them a JSON object of members, an INI
document of sections and a toml array of tables), and every document of
the grammar's own `test/spec` corpus. A listed grammar's pruned run relays
the same passes, pass for pass in the fixture encoding, and ends the same
way, and each list holds both ways. The grammars `PRUNES_MAPS` leaves out
read their maps back while they parse: yaml finds a key defined twice and
a merge's members, toml a key or a table defined twice and the table a
header walks through, xml (and feed, through it) the name of the element
a closing tag closes, and zon a field defined twice, each by looking it up
in the tree it is building, so with their maps emptied they pass documents
they refuse, or refuse ones they pass. One difference the contract allows:
a token whose value is a container of the tree carries what the tree holds
when its pass is made, so the arrays in it may have been emptied, unless
the grammar held the pruning while it built them, and its maps too where
the grammar's are, while its kind is as it was. yaml's
lexer resolves an alias so, to the anchored node (`a: &items [1, 2]` and
then `b: *items`), which the hold keeps whole, and yaml's part reads such
a token's kind alone, refusing an alias to a sequence or a mapping as one
whose members the passes do not show. A part that read the elements of a
token's arrays would read what pruning left wherever no hold kept them, so
none does. `toml` joined the list with tabnas-toml 0.5.20, whose message
for a key defined twice names the key and says it is already defined:
before, it quoted the earlier value, which pruning empties (`cannot define
arr, it already has the value []`). css and the grammar notations abnf,
ebnf and gbnf joined Rust's lists in 0.6.6, once its suite ran them, and
TypeScript's and Go's once theirs did, each through the parser its
package's `make` builds (`MakeJsonic` for css in Go), whose value is its
tree. Go's suite runs PGN since then too, and its lists name `pgn`. Until
tabnas/chess 0.2.0, chess's Go plugin kept a PGN database as a typed
value the source could not empty, so a pruned Go run kept every game; it
keeps the games in the engine's own list now, and a pruned Go run keeps
none, as in Rust and TypeScript (tabnas/chess#74).

### The rule history a source leaves the engine

The engine links each rule to the rule it replaced and the rule that
pushed it, and a grammar reads back through those links (`prev.n.count`,
`parent.name`). The TypeScript and Go engines keep the whole chain by
default, so a parse of a long flat document keeps every rule it made
reachable until the document ends, and the memory grows with the
document's length whatever the stream prunes: through dmog, 100,000 JSON
records from standard input held 2.8 GB in Go and ran out of TypeScript's
2 GB heap. A TypeScript or Go `ParserEventSource` that prunes a grammar
`boundsHistory` lists (`BOUNDS_HISTORY`, Go's `BoundsHistory`) therefore
bounds the history at three predecessors (`STREAM_RULE_HISTORY`,
`StreamRuleHistory`) before its parse, unless the grammar or the host
bounded it already, which it leaves as it is. Three is what the Rust engine
bounds the history at by default, so the Rust source sets nothing
(tabnas/parser `doc/rule-history-bound.md` has the design and the engine's
measurements). The same records then hold 221 MB in TypeScript, against
214 MB at 10,000, and the output is the same. `ts/test/history.test.ts` and
`go/history_test.go` hold it: ten times the records, read from a reader and
pruned, leave the heap's high water where it was (in Go 0.8 MB and 0.9 MB
over 2,000 and 20,000 records, against 28.7 MB and 281 MB unbounded). A
`ParserSource` walks the value the parse returns, which holds the whole
document anyway, and bounds nothing.

A grammar may read further back through the links than three predecessors,
so the list is a verified capability, as pruning is: the history half of the
prune suites runs every document of every grammar bounded
(`unverifiedHistory`, `UnverifiedHistory`) and not, compares the passes,
the ending and the value, and holds the list both ways. Each names every
grammar its suite runs. Go's left out yaml until 0.6.5: yaml's Go plugin
read a child's value at the end of the child's rotation chain, through the
`Next` links a bounded history cuts, until tabnas/yaml 0.5.32 read it from
the child alone, as its TypeScript does. Only a run that prunes is
bounded, since the bound buys flat memory only where the tree is emptied
too, and in Go it costs two to three times the parse's time
(tabnas/parser#339). Until 0.6.3, a TypeScript
source bounded every run, pruned or not, whatever the grammar, and a Go
source bounded none.
The suite asserts the list in both directions, and measures what pruning
is for: ten times the records of a JSON, JSON Lines, CSV, YAML, XML or PGN
document, ten times a stylesheet's rules, and in Rust and Go ten times a
grammar's in ABNF, EBNF or GBNF, leave the pruned tree holding as many
elements as
before (none for the worked example's JSON, four for its XML, and for its
YAML 15 in Rust and 6 in TypeScript and Go), where the tree without
pruning holds every record.

A run that prunes is gated on the list by the grammar's name
(`ParserEventSource::grammar(name)`): without a listed name it fails with
`STREAMABILITY_UNKNOWN` before the parse, having sent nothing. `ParserEventSource::unverified()` lifts the
gate for the suite. A run that does not prune needs no name.

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
payload bytes plus `NODE_BYTES` per node.

## Metrics

`Metrics` (shared through an `Arc`): `events`, `keys`, `scalars` (the
source), `rows` (the table transducer), `captured_bytes` and
`captured_bytes_high` (the router's one materialization at a time),
`retained_bytes_high`, `output_bytes` (a renderer's). `to_json` reports
them.

## Shared fixtures

`test/spec/*.tsv` is the behaviour every runtime of this crate must
reproduce, row for row. The Rust runners are `rs/tests/spec_<file>.rs`
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
| `input` | the source text; in the `events` mode, the events themselves, in the encoding `events.tsv` expects (below) |
| `grammar` | `json`, `jsonl`, `json5`, `jsonc`, `jsonic`, `yaml`, `zon`, `csv`, `toml` or `ini`; in `parser-events.tsv`, also `spec`, the serialized grammar in `document` |
| `document` | `parser-events.tsv`, grammar `spec`: a serialized grammar, installed with `Tabnas::grammar_json` |
| `mode` | `materialize`: `ParserSource` over the grammar, parsed and walked. `events`: the input's events, replayed through `Guarded` under the row's limits into the stage, for what no parse delivers: a member repeated in one object, which the engine's value keeps once, so the duplicates policies are pinned as a format's part streams them. `value`: the grammar's parse (an engine error is `Fail::from_tabnas`), then `ValueSource` over the value, which applies no limits. The `incremental`, `lines` and `lines-incremental` modes went with the rule-event inference and the line sources at steps 12 and 13 of alchemy's streaming plan, with the `prune` and `options` columns that only their rows had |
| `captures` | a JSON array of `{"tag", "select", "mode"}`, `select` a selector, `mode` `materialize` (the default) or `observe` |
| `binding` | `{"schema": ..., "rows": selector}`, the schema `"infer"`, `{"metadata": selector}` (each descriptor read by `column_from_meta`) or `{"static": [{"label", "source": segments, "missing"}]}`, `missing` `missing` (the default), `null` or `error` |
| `duplicates` | `reject` (the default), `last_wins` or `first_wins` |
| `stage` | `limits.tsv`: `events`, `route` or `table`, the pipeline `events.tsv`, `route.tsv` or `table.tsv` runs. `parser-events.tsv`: `passes`, every event in the encoding below; `tokens`, each pass as `["rule:open", [[name, val, src, row, col, offset]...]]`, its tokens as that encoding writes them; or `names`, each pass as `"rule:open"` or `"rule:close"`; or, with token items asked for, `trivia`, each pass as `names` writes it and each token item as `[name, src, offset]`; the end is `"end"` in all but `passes` |
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
`events.tsv` expects the whole stream; a `prefix` is the same encoding.

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
encoding (`ParserEvent::to_json`): `["open", rule]`, `["close", rule]`,
`["token", token]` and `["end"]`, the rule an object of `name`, `d`, `i`,
`alt` (`p`, `r`, `b`, `g`; left out when the state declared none),
`tokens` (each `[name, value, src, row, col, offset]`), `forced`, `n`
and, on a close pass with a scalar node, `value`, and a token item an
object of `name`, `val`, `src`, `row`, `col` and `offset`. The `passes` stage leaves `i` out, and its rows hold no
fixed token, since the engines differ on both. A token's value is
written as JSON, so its lexeme is not in the encoding; dmog's tests
of JSON's part, whose events carry it, pin it.

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
| `events.tsv` | 39 | `JsonEvents/1` from the walk of each grammar's value: root scalars, empty and nested containers, string decoding, a repeated member's survivor, a parse error's position, each grammar's shapes |
| `route.tsv` | 32 | the router: the worked example, every step kind, observed nesting, overlap refusals, jq paths, the duplicates policies |
| `table.tsv` | 30 | `TableFromJson`: metadata, static and inferred schemas (inferred from a first row of every kind: an object's members, an array's positions, a scalar's `value`, and a later row of another kind projected through them), `INPUT_ORDER_VIOLATION`, `MISSING_VALUE` and the missing policies, empty tables, container cells, descriptor errors |
| `scan.tsv` | 12 | `scan-emit`: output order, `finish` once, `Stop`, a step's failure |
| `parser-events.tsv` | 13 | `ParserEventSource`: every field of a pass but `i`, the groups sorted, tokens given back by `b` and their positions and byte offsets, a token with no value, a scalar node's value, a refused document's prefix, `max_scalar_bytes` on a token, a probe's passes dropped (tabnas-abnf's compiled `top = [ X "@" ] Y`); token items in source order among the passes, their byte offsets past multibyte characters, a token lexed inside a probe delivered, a refused document's prefix with them, and `max_scalar_bytes` on one |
| `limits.tsv` | 50 | each `Limits` field passed and not passed, by name, with UTF-8 byte counts at multibyte boundaries |

### What a port must reproduce exactly

- **Sizes are UTF-8 bytes.** A key or string counts its encoded length
  (`é` is 2, `日` 3, `😀` 4); a number its lexeme's length, which is 0
  to `max_scalar_bytes` and 8 to a retained value when there is none; a
  retained node adds `NODE_BYTES` (16). A port on UTF-16 strings that
  counts code units fails the multibyte rows of `limits.tsv`.
  `ParserEventSource` also holds each token's source text, quotes and
  punctuation included, to `max_scalar_bytes` before a sink sees the pass
  (`parser-events.tsv`): `["日本"]` is 6 bytes to the guard and 8 to the
  source.
- **The lexeme rule**: a rule pass's number token carries its source
  text only when that is an RFC 8259 number that reads as the value
  (`1e2` keeps `"1e2"`; `0x1F`, `+1`, `.5`, `5.` and YAML's `012` have
  none), and a number the walk emits has none.
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
