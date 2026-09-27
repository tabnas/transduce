# Streaming transducers and renderers for tabnas — Rust design brief

Status: implementation brief, 2026-09-27. Derived from the maintainer's design
document "Declarative Streaming Transducers and Renderers" (the "spec" below)
and the maintainer's answers:

1. The streaming SOURCE is any tabnas parser: rule events from the engine's
   debugging hooks (`subscribe_rule_done`) are turned into source events.
2. The DSL is parsed by a tabnas grammar plugin, in the `alchemy` repo, laid
   out like every other grammar repo.
3. Crate split: `transduce` (protocols, sources, matcher, captures, table
   transducer, limits), `render` (text algebra, CSV and JSON renderers),
   `alchemy` (DSL grammar, AST, checker, planner, interpreter, stdlib).
   `render` depends on `transduce`; `alchemy` depends on both.
4. Ordinary crates may be added as dependencies.
5. aless exposes it headlessly: `--alchemy PROGRAM [--render csv|json] FILES…`,
   `--alchemy-expr TEXT`, `--explain`.
6. Targets: JSON→CSV throughput measured and reported; retained state flat
   across 10× row counts; a large export with bounded resident memory.
7. Scope now: spec milestones 1–4 plus a JsonEvents renderer. XML, INI, TOML
   renderers, spooling and JSONPath are deferred.
8. Rust only for now (no ts/ or go/). The maintainer merges are delegated.

The parsed JSON data structures the grammars return are never altered: the
transducer consumes them (incrementally where verified, whole otherwise).

## 0. What was measured before designing

A prototype adapter (scratchpad `proto/`) installed a `subscribe_rule_done`
subscriber on each grammar and produced events by watching the rule's node:

- A rule whose node is a container at the end of its OPEN pass, with a cell
  (`Rc<RefCell<Value>>` pointer) not already on the frame stack, STARTS a
  container. Pushed/replaced rules share the parent's cell, so pair/elem rules
  do not start one.
- At the end of any CLOSE pass whose node cell is the top frame's, a growth in
  `len()` means new entries were inserted (maps: the last entries, insertion
  ordered; arrays: the tail). A new entry whose container `Arc` pointer equals
  the last completed frame's container was already streamed; otherwise it is
  walked (late).
- A key is announced EARLY when the rule stashes it in `rule.u["key"]` (the
  `@key$` builtin, jsonic's `@jsonic-pairkey`, YAML's own actions all do).
- The top frame ENDS when a rule at the frame's depth whose node cell is the
  frame's cell closes (rule identity `i` alone fails under `r:` replacement).
- Root scalar and end-of-document at the depth-0 rule's close.

Differential check (incremental events == full-tree walk) over aless's
fixtures: MATCH with zero late keys and zero re-walks for json, jsonl, jsonic,
jsonc, json5, zon and a small yaml; MISMATCH for toml, ini, csv, xml,
markdown, feed (imperative value construction, containers transformed before
insertion) and for a large yaml (to be investigated). Consequence: incremental
streaming is a per-grammar VERIFIED capability, never assumed.

What the differential suite then established (`rs/tests/incremental_test.rs`,
over every fixture each grammar reads, under the contract that a listed
grammar produces the walk's stream, or the walk's stream after a last-wins
router, or a documented refusal before `End`) differs from that prototype:
`yaml` is verified, once a close whose alternate replaces the rule no longer
ends a frame; `jsonic`'s container-first implicit list and yaml's `---`
document stream are refusals (`STREAMABILITY_UNKNOWN`, a root already
streamed cannot be re-wrapped), not mismatches; and `markdown`'s events
equal the walk. The verified list is `json`, `json5`, `jsonc`, `jsonic`,
`jsonl`, `markdown`, `yaml`, `zon`; `toml`, `ini`, `csv`, `xml` and `feed`
complete streams the walk contradicts and stay off it.
[`reference.md`](reference.md) and `rs/src/source/capability.rs` record the
contract and each refusal's shape. Every grammar
still works through the whole-document walker. JSONL and CSV additionally get
a line-chunked source that bounds memory regardless.

Engine facts that shape the design: the engine parses a whole `&str` (the
source is also copied into `Context.source`); values are `Arc`-shared
containers; `subscribe_rule_done` clones the finished rule per pass when any
subscriber is installed (cheap: pointer copies plus one `Value` clone).
Throughput numbers are recorded in [`BENCH.md`](BENCH.md) next to this file.

## 1. Repositories and layout

Each of `transduce`, `render`, `alchemy` (all currently LICENSE-only) gets:

```
.tabnas-kind            transduce, render: TOOL; alchemy: COMPONENT
AGENTS.md               the guide (fleet standard sections: Verify your work,
                        Error codes, Untrusted input; plus the two core
                        principles: dependencies change only on instruction,
                        transient tasks report progress)
CLAUDE.md -> AGENTS.md  symlink, as in parser
README.md               orientation hub, routes to docs/
Makefile                build / test / clean (rs only for now)
ci/rust/run.sh          the Rust gate (fmt, build, test, doc tests, clippy;
                        lock discipline as in zon's script; sibling list)
.github/workflows/rust.yml   runs ci/rust/run.sh with siblings checked out
docs/                   architecture.md (transduce carries the canonical one),
                        reference.md, and for alchemy language.md (the DSL)
test/spec/*.tsv         alchemy only: shared fixtures (layout -> canonical)
rs/Cargo.toml           path deps on siblings: tabnas = { path = "../../parser/rs" }
rs/Cargo.lock
rs/README.md
rs/src, rs/tests, rs/benches
```

Crate names: `tabnas-transduce` (lib `tabnas_transduce`), `tabnas-render`
(`tabnas_render`), `tabnas-alchemy` (`tabnas_alchemy`, plus bin `alchemy`).
Versions start at 0.1.0, edition 2021, rust-version 1.85 (the fleet's).
Committed manifests are path-only (admin ADR-21). Dev-deps allowed: the
grammar crates (json, jsonl, csv, yaml, toml, xml, ini, zon, markdown, feed,
jsonic, jsonc, json5) by path for tests; `tabnas-support` for TSV runners;
`criterion` for benches; `serde_json` for oracles.

Branch in every repo: `claude/streaming-transducer-render-kzyqua`. PRs ready
for review, never draft. Conventional commit subjects.

## 2. transduce — `tabnas-transduce`

Push-based, synchronous pipeline. The parser drives events from inside its
own callback; stages are `Sink`s; the writer at the end blocks the parser, so
backpressure is inherent. Cancellation is a shared `AbortFlag` polled by a
parse guard and by long loops. A pull adapter (thread + bounded channel) can
be added later at the boundary; nothing inside needs it.

### 2.1 Modules and public surface

```rust
// event.rs — JsonEvents/1
pub enum JsonEvent<'a> {
    ObjectStart, ObjectEnd, ArrayStart, ArrayEnd,
    Key(&'a str),
    Null, Bool(bool),
    Number(Number<'a>),          // value: f64, lexeme: Option<&'a str>
    String(&'a str),
    End,                         // exactly once, after the root value
}
pub struct Number<'a> { pub value: f64, pub lexeme: Option<&'a str> }
pub enum OwnedJsonEvent { … }  // to_owned(); used by recorders and tests

// sink.rs
pub enum Flow { Continue, Stop }     // Stop: downstream is finished (take)
pub trait Sink { fn event(&mut self, ev: JsonEvent<'_>) -> Result<Flow, Fail>; }
impl Sink for Vec<OwnedJsonEvent>;   // recorder
pub struct FnSink<F>(F);             // FnMut(JsonEvent) -> Result<Flow, Fail>

// error.rs — the spec's stable codes
pub enum Code { DslParseError, DslTypeError, StreamReused, StreamabilityUnknown,
    InputOrderViolation, CaptureOverlapUnsupported, MissingValue, DuplicateMember,
    InvalidNumber, ProtocolOrderError, TargetValueUnrepresentable,
    ResourceLimitExceeded, InputInvalid, OutputFailed, Aborted }
impl Code { pub fn as_str(&self) -> &'static str }   // "INPUT_ORDER_VIOLATION"
pub struct Fail { pub code: Code, pub message: String, pub path: Option<String>,
    pub limit: Option<Limit>, pub committed_output: bool,
    pub row: Option<u64>, pub column: Option<u64> }
pub struct Limit { pub name: &'static str, pub value: u64 }

// limits.rs
pub struct Limits { pub max_depth: usize, pub max_key_bytes: usize,
    pub max_scalar_bytes: usize, pub max_metadata_bytes: usize, pub max_columns: usize,
    pub max_record_bytes: usize, pub max_capture_bytes: usize,
    pub max_output_bytes: Option<u64> }           // Default: 256, 64 KiB, 16 MiB, 16 MiB,
                                                  // 10_000, 64 MiB, 64 MiB, None
pub struct Metrics { … }   // Arc-shareable atomics: events, keys, scalars, rows,
                           // captured_bytes (gauge + high-water), retained_bytes
                           // high-water, output_bytes; fn report() -> String
pub struct AbortFlag(Arc<AtomicBool>);   // abort(), is_aborted()

// datum.rs — the retained value type (materialized captures, cells)
pub enum Datum { Null, Bool(bool), Number { value: f64, lexeme: Option<Box<str>> },
    String(Box<str>), Array(Vec<Datum>), Object(IndexMap<Box<str>, Datum>) }
impl Datum { pub fn byte_size(&self) -> usize; pub fn get_path(&self, &[Segment]) -> Option<&Datum>;
    pub fn from_tabnas(&tabnas::Value) -> Datum; pub fn to_json(&self) -> serde_json::Value }
pub fn walk_datum(&Datum, &mut dyn Sink) -> Result<Flow, Fail>;

// selector.rs — the native selector algebra (spec §11.1)
pub enum Step { Property(Box<str>), Index(usize), EachIndex, EachMember }
pub struct Selector(Vec<Step>);          // root() is the empty selector
impl Selector { root, property, index, each_index, each_member, compose,
    pub fn from_segments(&[Segment]) -> Selector,   // as-path: data, never code
    pub fn display(&self) -> String }            // jq-like: .response.records[*]
pub enum Segment { Key(Box<str>), Index(usize) }   // a concrete path
pub struct Path(Vec<Segment>);  impl Display (jq syntax, as aless prints)

// matcher.rs — shared-prefix trie + scope stack, one pass, N selectors
pub struct Matcher { … }   // new(&[Selector]) ; fn enter_key / enter_index / start_value /
                           // end_value ; active terminals as SmallVec<CaptureId>

// route.rs — capture and deliver completed, non-overlapping matches in order
pub enum CaptureMode { Materialize, Observe }
pub struct CaptureSpec { pub tag: Box<str> /* settled on `Arc<str>` */, pub selector: Selector, pub mode: CaptureMode }
pub struct Selected { pub tag: Box<str> /* settled on `Arc<str>` */, pub path: Path, pub value: Option<Datum> }
pub struct Router<F: FnMut(Selected) -> Result<Flow, Fail>> { … }   // impl Sink
// Rejects overlap (CAPTURE_OVERLAP_UNSUPPORTED); enforces max_capture_bytes and
// max_depth (RESOURCE_LIMIT_EXCEEDED); DUPLICATE_MEMBER in a captured scope
// under the strict profile; delivers in source order.

// table.rs — TableRows/1
pub enum Cell { Null, Bool(bool), Number { value: f64, lexeme: Option<Box<str>> },
    String(Box<str>), Missing }
pub struct PublicColumn { pub label: Box<str> }
pub enum TableEvent<'a> { Schema(&'a [PublicColumn]), Row(&'a [Cell]), End }
pub trait TableSink { fn table_event(&mut self, ev: TableEvent<'_>) -> Result<Flow, Fail>; }
pub struct BoundColumn { pub label: Box<str>, pub source: Vec<Segment>, pub missing: MissingPolicy }
pub enum MissingPolicy { Missing, Null, Error }   // Missing is the default
pub enum Schema { Static(Vec<BoundColumn>),
    FromMetadata { columns: Selector, column: Box<dyn Fn(&Datum) -> Result<BoundColumn, Fail>> },
    Infer { from_first_row: bool } }   // labels = keys of the first row; documented as data-dependent
pub struct TableBinding { pub schema: Schema, pub rows: Selector }
pub struct TableFromJson<S: TableSink> { … }   // impl Sink; the spec's table-from-json
// Contract: metadata-first. A row before the schema is INPUT_ORDER_VIOLATION
// (detected at row start where possible). Metadata selected twice is an
// error. Zero rows is an empty table. Row cells follow schema order, Missing
// for absent paths, exact number lexemes preserved when the source has them.

// scan.rs — generic scan-emit for the interpreter
pub struct Transition<S, O> { pub state: S, pub outputs: Vec<O> }
pub struct ScanEmit<S, I, O, Step, Finish> { … }

// source/mod.rs
pub trait Source { fn run(self, sink: &mut dyn Sink) -> Result<(), Fail>; }
pub struct ValueSource<'v>(pub &'v tabnas::Value);       // whole-value walker
pub struct ParserSource<'s> { parser: Tabnas, text: &'s str, mode: SourceMode,
    limits: Limits, abort: AbortFlag, metrics: Arc<Metrics> }
pub enum SourceMode { Materialize, Incremental { prune: Prune } }
pub enum Prune { Never, Under(Selector), AllArrays }   // drop streamed array elements
pub struct LinesSource<R: BufRead> { … }   // Jsonl: one JSON document per line, parsed
                                           // with tabnas-json; Csv { header }: header +
                                           // chunks of whole records (quote-aware split)
pub mod capability { pub fn incremental(grammar: &str) -> bool }  // the verified list
```

### 2.2 The rule-event adapter (source/rule_events.rs)

Exactly the prototype's algorithm (section 0), productionized:

- Installed on a `Tabnas` with `subscribe_rule_done`; state behind
  `Arc<Mutex<…>>` because the subscriber must be `Fn + Send + Sync`. The parse
  runs on one thread, so the lock is uncontended.
- Number lexemes: at the CLOSE of a rule whose node is a `Number`, remember
  `o0().src` when its parsed value equals the node; attach it when that scalar
  is inserted. Best-effort, documented.
- Limits: depth (frame stack), key bytes, scalar bytes, checked as events are
  produced; a breach fails the parse through the abort flag with
  RESOURCE_LIMIT_EXCEEDED.
- Pruning: after the entries of an array frame selected by `Prune` are
  emitted, `truncate` the array to the previous length through the shared
  cell. Only in Incremental mode, only for grammars in the verified list, and
  never touching the grammar's result otherwise.
- `capability::incremental(name)` is a table maintained by
  `tests/incremental_test.rs`: every fixture of every grammar crate in
  dev-deps runs both ways and must MATCH for the grammar to be listed;
  a listed grammar that mismatches fails the test, an unlisted grammar that
  matches everywhere fails too (the list cannot rot either way).
- Errors from the engine (parse errors) are `INPUT_INVALID` with the engine's
  code, row and column carried in the message and fields.

### 2.3 Tests

Unit tests beside the code; `tests/`: protocol validation, selector display
and `from_segments`, matcher over hand-written event streams (including `[0,0]`
style duplicates rejected as unsupported), router overlap rejection, capture
limits, table-from-json on the spec's worked example (metadata first; rows
before metadata rejected; member order within a row irrelevant; missing vs
null; zero rows), lines sources with quotes and CRLF, chunk boundaries at every
byte for the line sources, and the incremental differential suite above. A
retention test: rows × 10 with fixed row size leaves the retained-bytes
high-water flat.

### 2.4 Benches (criterion)

`benches/throughput.rs`: parse-only vs events vs events+table+CSV (with render
as a dev-dep would create a cycle; the CSV bench lives in render) on generated
30 MB inputs; report MB/s. Each bench prints a progress line.

## 3. render — `tabnas-render`

```rust
// text.rs
pub trait TextOut { fn write_str(&mut self, s: &str) -> Result<(), Fail>; fn flush(&mut self) -> Result<(), Fail>; }
pub struct WriteOut<W: io::Write> { … }   // coalesces to a byte budget (32 KiB default), counts output_bytes
pub struct Join<O: TextOut> { … }          // separator between logical items, not chunks
pub struct ReplaceText<O: TextOut> { … }   // fixed literal, carry-over across chunk boundaries
pub struct Concat …                        // helpers used by the interpreter's text algebra

// csv.rs — the spec's always-quoted profile (Appendix A semantics)
pub enum Newline { Lf, CrLf }
pub enum Quoting { Always, Minimal }       // Always is the standard profile
pub enum MissingText { Error, Text(Box<str>) }
pub struct CsvOptions { pub delimiter: char, pub newline: Newline, pub header: bool,
    pub null_text: Box<str>, pub missing: MissingText, pub quoting: Quoting }   // Default: ',', CrLf, true, "", Error, Always
pub struct CsvRenderer<O: TextOut> { … }   // impl TableSink; validates schema-once, widths,
                                           // one End, zero columns rejected, delimiter validity,
                                           // number lexemes (INVALID_NUMBER), bool as true/false
// json.rs — JsonEvents/1 to text
pub struct JsonOptions { pub indent: Option<usize>, pub trailing_newline: bool }
pub struct JsonRenderer<O: TextOut> { … }  // impl Sink; RFC 8259 escaping, lexeme or shortest
                                           // round-trip f64, NaN/Inf -> TARGET_VALUE_UNREPRESENTABLE,
                                           // single root, PROTOCOL_ORDER_ERROR on bad sequences
// records.rs — TableRows -> JsonEvents (array of objects keyed by label)
pub struct RecordsToJson<S: Sink> { … }    // impl TableSink
```

Tests: every Appendix A case (quoting, empty fields, commas, quotes, CR/LF,
Unicode, false, zero, null policy, missing policy, big lexemes, duplicate
labels allowed, empty row sequence, width errors, final line ending); parse
the CSV back with an independent reader (the `csv` crate as a dev-dep) and
compare; JSON output re-parsed by serde_json equals the input document for
every aless fixture (through transduce's walker); chunk-boundary tests for
`ReplaceText` and `Join`. Bench: JSON→CSV end to end.

## 4. alchemy — `tabnas-alchemy`

### 4.1 The grammar plugin (src/grammar.rs, src/lex.rs)

A tabnas grammar, installed by `pub fn alchemy(parser: &mut Tabnas)`, with
`make()` and `parse(src) -> Result<Vec<Expr>, Fail>`. The lexer: strings with
JSON escapes (`#ST`), JSON numbers (`#NR`; hex/oct/bin/separators off),
`true false null` values (`#VL`), symbols as text tokens (`#TX`) over the
symbol alphabet `[A-Za-z0-9_\-?!*+/<>=.$%&|^~@]`, keywords `:name` (a lex
matcher emitting `#KW` with the name as value), `;` comments to end of line,
fixed tokens `(`→`#OP`, `)`→`#CP`, `[`→`#OS`, `]`→`#CS`. Layout: an
imperative lex matcher at line starts computes indentation and emits `#IN`
(one level deeper: exactly two spaces), `#DE` (one token per level closed,
issued one at a time by keeping the pending count in the context bag), and
`#NL` (a new logical line at the same level). Tabs in indentation and dedents
to a non-existent level are `DSL_PARSE_ERROR`. Layout is suspended while
`(`/`[` depth is non-zero (tracked in the context bag).

Rules build a tagged `Value` the `ast` module converts to `Expr` with spans:
`{"$":"list","items":[…],"span":[si,ei]}`, `{"$":"vector",…}`,
`{"$":"sym","name":…}`, `{"$":"kw","name":…}`, `{"$":"str","value":…}`,
`{"$":"num","lexeme":…}`, `{"$":"bool"}`, `{"$":"null"}`. Reader rules (spec
§9.1): a layout line with several inline forms or with children is a list
(inline forms first, children after); a line with exactly one form and no
children is that form; explicit parens are lists, brackets are vectors; blank
lines and comments are ignored; a dedent determines structure.

`pub fn canonical(&[Expr]) -> String` prints fully parenthesized forms;
`pub fn format(&[Expr]) -> String` prints layout form. Test:
`test/spec/reader.tsv` rows `input<TAB>canonical` run by `tabnas-support`'s
runner (the tabnas fixture convention), covering every `dsl` block in the
spec, singleton values, zero-argument calls, nested vectors, mixed forms,
comments, escapes, blank lines, invalid indentation (`ERROR:DSL_PARSE_ERROR`).

### 4.2 AST, desugaring, resolution

```rust
pub struct SourceSpan { pub file: Arc<str>, pub start: usize, pub end: usize }  // byte offsets; row/col derivable
pub enum Expr { Symbol{name,span}, Keyword{name,span}, Str{value,span}, Num{lexeme,span},
    Bool{value,span}, Null{span}, List{items,span}, Vector{items,span} }
```
Desugar (spec §9.3, §9.4, Appendix B): `(def name [params] body)` →
`(def name (fn [params] body))`; `(pipe init step…)` → nested data-last
application, a bare symbol step is a unary call, a non-empty list step gets
the threaded value appended, anything else is `DSL_PARSE_ERROR`. `let` has one
binding and one body. `if` two branches. `match value (case pattern body)…`.
Spans survive desugaring (generated nodes carry the span of the form they
came from).

### 4.3 Values, types, ownership, effects

Runtime values: `Null, Bool, Num{value,lexeme}, Str, Keyword, Vector(Rc<[Val]>),
Record(Rc<IndexMap<Keyword, Val>>), Fn(Closure | Native | Partial), Selector,
CaptureSpec, Tagged{tag, fields}` (constructors: `schema`, `row`, `table-end`,
`no-schema`, `ready`, `selected`, `transition`), `Stream(Plan)`, `Text(Plan)`.
Streams and texts are PLANS, built lazily and executed once by the runtime.

Checker (conservative, spec §15): types `Value | Vector<T> | Stream<T> |
JsonEvents | Selector | Text | String | Record | Fn`. A binding of stream type
may be consumed at most once in a scope (`STREAM_REUSED`); a `fn` body may not
capture a one-shot stream from an enclosing scope; unknown or dynamic
higher-order functions are `STREAMABILITY_UNKNOWN` in strict mode (default
for `aless`, `alchemy check`). Stdlib operators carry signatures and effect
metadata in a registry (`stdlib/registry.rs`): retention scope, readiness,
order constraints, how callback effects propagate. `explain()` prints the
spec §15.5 report from the `EffectSummary`.

### 4.4 Interpreter and stdlib

Tree-walking evaluator. Stream plans lower to transduce/render sinks:
`route` → `Router`; `scan-emit` → `ScanEmit` calling the DSL step per item;
`csv` → `CsvRenderer`; `map/filter/concat-map/join/concat/replace-text` →
their sinks/text combinators. The two standard compositions are recognized
and run natively for speed: `table-from-json BINDING input` → `TableFromJson`,
and `csv OPTIONS table-events` → `CsvRenderer`. The DSL sources of both live in
`stdlib/*.alc`, are embedded with `include_str!`, are parsed and checked in
tests, and a differential test proves the interpreted definition and the
native fast path produce identical bytes on the fixtures.

Stdlib natives (data last): `get`, `get-path`, `as-path`, `as-vector`,
`record`, `entry`, `vector`, `path`, `each-index`, `each-member`, `root`,
`property`, `index`, `compose`, `capture`, `route`, `scan-emit`, `transition`,
`partial`, `map`, `filter`, `concat-map`, `join`, `concat`, `text`,
`replace-text`, `scalar-text`, `fail`, `is-ready`, `require-columns`,
`public-column`, `schema`, `row`, `table-end`, `no-schema`, `ready`, `csv`,
`csv-options`, `table-from-json`, `json` (JsonEvents renderer), `records`
(TableRows → JsonEvents), `select` (alias of a single-capture route yielding
values).

Program entry: `def export [input] …` is the convention; `run(program,
source, out)` applies `export` to the source stream and drives the resulting
Text or protocol stream to `out`. `alchemy` bin: `check`, `explain`, `run`,
`format`, `canon`.

## 5. aless

- Options: `--alchemy PROGRAM_FILE`, `--alchemy-expr TEXT`, `--render csv|json`,
  `--explain`. Headless only (they imply no viewer). Any input format aless
  already reads; the transducer source is `ParserSource` in incremental mode
  for verified grammars (with pruning under the program's row selector), the
  whole-value walker otherwise, and `LinesSource` for JSONL and CSV inputs
  when the program's row selector is the root's elements (bounded memory).
- With `--render` and no program: the built-in default program — rows are the
  root array's elements (or the JSONL lines / CSV records), schema inferred
  from the first row, CSV out; `--render json` echoes the document as compact
  JSON events (a streaming alternative to `--json`).
- Errors keep aless's `{"error": {kind, code, message, file, …}}` shape:
  `kind: "transduce"` with `code` one of the spec codes, plus `path`, `limit`,
  and `output: "partial" | "none"`. Statuses: DSL errors → 2 (usage);
  INPUT_* / PROTOCOL / TARGET / MISSING / DUPLICATE / INVALID_NUMBER / ORDER →
  1; RESOURCE_LIMIT_EXCEEDED → 5; ABORTED (timeout) → 6; OUTPUT_FAILED → 3.
- Streaming output goes to standard output through a `WriteOut` with a
  32 KiB budget; `--timeout` and `--max-size` keep their meaning; the line
  sources are exempt from the whole-file size limit (per-record limits apply)
  and say so in the docs.
- Docs to update: README "Scripts and agents", `--help`'s WITHOUT A SCREEN,
  `skills/aless/SKILL.md`, `tests/agent.rs`, `src/headless.rs` tests.
- Dependencies: `tabnas-transduce`, `tabnas-render`, `tabnas-alchemy` as git
  deps pinned by Cargo.lock, with `[patch]` tables for their path siblings, as
  the existing grammar deps are wired.

## 6. Engine (parser)

No change is required for the milestones above. Candidates recorded for a
later PR, each with a measurement first: sharing the source instead of
copying it into `Context.source` (2× source memory today); a lighter
`rule_done` notification without the per-pass `Rule` clone; a chunked source
for the lexer (the only route to bounded memory on one huge JSON document).

## 7. Acceptance

1. `ci/rust/run.sh` green in transduce, render, alchemy; aless gates green.
2. Differential suites: incremental vs walk per grammar (transduce);
   interpreted stdlib vs native (alchemy); rendered CSV re-parsed by an
   independent reader (render); rendered JSON re-parsed equals input.
3. The spec's worked example runs end to end through aless with the spec's
   `api-binding` program and prints the spec's CSV bytes (CRLF, all quoted).
4. Retention high-water flat across 10× rows; pruned incremental JSON export
   of a 30 MB document holds RSS near source size + O(record); JSONL/CSV via
   line sources hold RSS flat regardless of file size; throughput reported
   in BENCH.md and in the PR descriptions.
