# Any format to any other: the design

How a tabnas host translates a document of one format into another
without a translator per pair. Background: [`architecture.md`](architecture.md)
sections 2 to 5 (the transducer, the renderers, alchemy, aless),
[`reference.md`](reference.md) for the protocols, and
[the alchemy reference](https://github.com/tabnas/alchemy/blob/main/docs/language.md).

## The problem

The fleet reads N formats. A viewer that can also write a document out
as any of them needs, written naively, N × (N − 1) translators, each a
program somebody has to write, test and keep in step with two grammars.
Nobody will write them and nobody could keep them green.

## What already exists

Reading is a hub. Every grammar in the fleet, whatever its syntax,
builds its value through the engine, and the transducer turns that into
one event stream (`JsonEvents/1`: start and end of objects and arrays,
keys, scalars) either incrementally, for the grammars the differential
suite has verified, or by walking the whole value. That is the N side of
the translation, done: a CSV file, a YAML file and a Markdown file all
arrive as the same kind of stream. The only question left is writing.

Writing has three shapes. Two are protocols the transducer names; the
third is the render crate's text output, which alchemy types as `Text`:

| Shape | Protocol | What it holds | Examples |
|---|---|---|---|
| text | `Text` | characters in order (the render crate writes text, never bytes) | any textual format, at the end |
| records | `TableRows/1` (one schema, rows as wide as it) | a table | CSV, JSON Lines of flat objects, a Markdown table |
| tree | `JsonEvents/1` | nested containers and scalars | JSON, YAML, TOML, ZON, XML's element tree |

A renderer is a function from one of the last two to the first. A tree
is a value's events: one root value, each key once per object.
`JsonEvents/1` can carry more, since an incremental source streams
every occurrence of a repeated member, so the host checks a stream
against that before a render that writes from a tree takes it ([In the
host](#where-the-parts-live-and-how-they-reach-the-host)). The
render crate ships two natively, CSV from records and JSON from a tree.
alchemy expresses the first: its standard library's `csv` is alchemy
text, held to the native renderer's bytes by a differential test. Its
`json` is a native, a call into the render crate, and no alchemy text
renders a tree today; the pilot says why, and what changes that.

## The design

Each format's repository ships, beside its grammar, what a host needs to
write the format and nothing that concerns any other format:

1. **Shapes**: what the format's documents read as, one or two of
   `text`, `records` and `tree` in order of preference, and what its
   render writes from, one of them. A tree format reads as a tree and
   writes from one. CSV reads as a tree (its grammar builds one object
   per row, keyed by the header, so its events carry its records in a
   tree's shape) and writes from records; a records target reaches its
   rows through the adapter below. A format with a lift reads as
   records first and as a tree second. JSON Lines reads as a tree, since its source
   wraps the lines in one array whatever they hold, and records are
   reached through the adapter below with its typed refusal; it writes
   from a tree.
2. **A lift**, optional: an alchemy definition from the format's event
   stream to its first read shape's protocol, for a shape the events do
   not carry as they are. A Markdown table's tree is a block with a
   header row and rows of cells; its lift takes the header row's cells
   as the schema and each later row as a record. A tree format needs
   no lift: its events are its shape.
3. **A render**: an alchemy definition from the write shape's protocol
   to text in the format. CSV's render is the standard library's `csv`; YAML's
   is a state machine over the events, emitting block style.
4. **A loss declaration**: what the target cannot carry, and what a
   successful write changes, so the host can say so before it writes.
   YAML carries any tree and loses comments, anchors and styles; CSV
   carries records whose cells are scalars and writes a nested cell as
   JSON text; a Markdown table carries records and loses nothing a
   screen can show.

The host composes `render_bar ∘ adapt(read_foo → write_bar) ∘
lift_foo`. When the target's write shape is among the source's read
shapes, the host takes the source's events in that shape (through the
lift when it is the first shape and a lift exists, as they are
otherwise) and needs no adapter: CSV to YAML runs YAML's render over
CSV's events as they are. Otherwise the adapter is chosen from the
source's first read shape to the target's write shape, and two do
anything, one in the standard library today and one the pilot adds:

| From \ to | text | records | tree |
|---|---|---|---|
| records | render | identity | `records` (one object per row, keyed by label): a missing cell is an absent member, and of two columns with one label the last contributes the member, the render crate's `RecordsToJson` policies |
| tree | render | the inferred table (pilot step 4), the policy aless's own CSV export runs today: the root must be an array and its elements are the rows; an object row's members are cells and a scalar row is one cell in a column named `value`; the first row's member names are the schema; an absent member is an empty field and a repeated member keeps its last value; an array row, or a table of mixed rows, is refused with the reason | identity |

No format reads as text: text is where a translation ends, never where
one starts, so the table has no text row.

Refusal is typed: a tree whose root is not an array, or whose rows are
arrays or a mixture, has no records shape, and the host says which
shape it saw rather than guess a column layout. It comes before any
output where the shape is decided before any, at the root's first event
and at the first row; a later row of another kind is refused where it
is met, after the rows before it have been written, and the failure
says so (`output: "partial"`), as an incremental export cannot take
bytes back and `--render csv` reports today. The refusal that names the
shape is the host's: aless keeps its row check (`Rows`) in front of the
program's sink whenever a tree is adapted to records, as it stands in
front of `--render csv` today; the transducer's own refusals (a first
row that is not an object, a table of no columns) stay behind it and
name the position.

An adapter's losses are the host's, printed only when that adapter
runs: a format's `loss` says what its own render loses, and a
translation that composes no adapter prints that alone.

Cost: N shapes, N renders, at most N lifts, one adapter per pair of
shapes (two that do anything). Testing stays at N: each repository tests
its own lift and render against its own fixtures, and the host tests the
adapters once.

## Where the parts live and how they reach the host

Every grammar repository already carries a manifest, `tabnas.plugin.json`
(`name` and `extensions` everywhere; `languageId` and `pluginKind` in
most, which markdown and feed lack; `grammar` where the grammar is a
file, which json, jsonl, jsonic, markdown and feed have none of, since
they build their values in code). Most crates that have a grammar file
embed its text as a generated string, regenerated by a script and held
to the file by a test, and some (csv, toml, jsonc, css) expose it as
`grammar_text()`; tabnas/c embeds its grammar with `include_str!`, as
alchemy embeds its standard library. The translation parts take the
second route, since they are text alchemy reads and nothing generates
them:

- **In the manifest**, a `translate` object: `reads` (a shape, or an
  array in order of preference), `writes` (one shape), `lift` and
  `render` (paths to the alchemy files, relative to the repository
  root, or for `render` the name of a render alchemy carries, `json`
  or `csv`; `lift` absent where the events carry the shape), and `loss`
  (an array of short sentences the host prints verbatim when it warns
  or refuses, about what this format's render loses and nothing else).
  JSON's and CSV's renders live in alchemy and the render crate, not in
  their repositories, so in the pilot the host owns their two entries
  as built-ins (JSON writes from a tree through the `json` native, CSV
  from records through the library's `csv`, with their loss lines in
  aless), and their manifests take the object, naming those renders,
  when they next change. No manifest schema is published (the
  `$schema` URL every manifest names answers 404, tabnas/admin#94), so
  the admin repository's descriptor task, which regenerates manifests,
  keeps the object and checks its shape (tabnas/admin#93); a manifest
  without it means a format that is read and not written. The language
  server's fleet registry is generated from these manifests and copies
  named fields only, so the object does not reach it, and nothing there
  needs it: the host reads the parts from the crates, below.
- **In the repository**, the files: `alchemy/lift.alc` and
  `alchemy/render.alc`, each a library of definitions with no `export`.
  Every definition's name is prefixed by the format's name
  (`markdown-lift`, `yaml-render`, `yaml-scalar`), so that no two formats'
  helpers collide when the host links them into one program; the entry
  points are `<format>-lift` and `<format>-render`, and nothing else in
  a file is the host's to call. A lift takes the format's `JsonEvents`
  and answers its first read shape's protocol; a render takes the write
  shape's protocol and answers `Text`. The parts declare no types: they are compiled by the
  program path, whose definitions the checker infers from use (the
  standard library's are declared in Rust, in `check::stdlib_signature`,
  and the parts do not go that way), and a render handed the wrong shape
  fails the composed program's check with `protocol_mismatch` at the
  render's file and line.
- **In the Rust crate**, the parts embedded with `include_str!` and
  exposed as `lift_text()` and `render_text()`, and the manifest itself
  as `manifest_text()`, so that the shapes and the loss declaration
  reach the host from the one place they are written and nothing is
  copied into the host. A crate packaged for crates.io holds nothing
  outside its own directory, so `include_str!` of a file at the
  repository's root compiles in a checkout and fails the verification
  build `cargo publish` runs. The crate embeds copies instead, under
  `rs/translate/` (`manifest.json`, and a file per part: `render.alc`
  for yaml), as it embeds its grammar's copy, and a change to a part is
  made at the root and copied there. One test holds the copies to the
  files: the embedded manifest is the repository's, and each path it
  names, read from the repository, is the accessor's text, so that a
  renamed file, or a part or a manifest edited alone, fails the crate's
  own gate before a port could load one part and the host another. yaml,
  whose grammar text is private today, gains these accessors and
  nothing else. The manifest names the files so that
  the TypeScript and Go ports, when they run alchemy, find the same
  text; until then translation is the Rust host's.
- **In alchemy**, the linking: `compile_sources`, taking several named
  sources and resolving them into one namespace (a program's is today's
  `compile`, with one source). A span already carries its file
  (`SourceSpan.file`); what positions it, the checker and the runtime,
  holds one source text today and must hold one per file. A `Fail`
  carries a row and a column and no file, and aless's headless error
  object carries a `file` field that names the program's file today, so
  the file must travel structurally: `Fail` gains a `file` field (a
  field added, nothing renamed: transduce's change, step 2's first
  half), alchemy fills it from the span when the program has several
  sources, `Display` writes it before the row and column
  (`(render.alc:3:5)`), and `to_json` carries it as `file`, which
  aless's error object takes over the program's file as it takes the
  failure's other fields. The message is left as it is, so the file is
  written once; only a failure in the standard library, which has no
  row of the program's, names its file in the message
  (`at stdlib/table.alc:38:5`). A collision is `duplicate_def`, as
  within one file. The
  standard library stays what it is: process-wide, loaded once, and
  fatal if its own text fails to load. A format's parts are not loaded
  that way: they are compiled per translation, and a part that fails to
  compile is a refusal naming the format and the file, never an abort.
- **In the host**, a registry from format to parts, keyed by the
  manifest's `languageId` (added to the two manifests that lack it;
  aless maps its `Format` to it), filled from the crates' functions
  (the manifest's `translate` object for the shapes and the loss, the
  two texts; aless depends on the thirteen grammar crates it reads
  already, and a format outside that set reaches it only by a new
  dependency, on instruction). The registry lives in aless: no other
  host translates until one asks, and the alternative, a fleet crate
  (alchemy with a feature per grammar, as the language server's `fleet`
  feature is built), would cost alchemy a dependency on every grammar.
  Then
  a `--render <format>` that composes the program from the registry and
  runs it through the same plumbing as `--alchemy`: the input read as
  the source's plan says, the parse pruned under the program's rows, the
  host's limits and timeout on the run. Every host keeps a tree's
  contract in front of a render that writes from one: a walked value
  keeps it by construction, and a host that streams a parse checks the
  stream natively, a lookup per key, refusing a repeated member
  (`DUPLICATE_MEMBER`) and events no tree has (`STREAMABILITY_UNKNOWN`),
  and falls back to the parsed value when nothing has been written. The
  render checks only what its state can see, which is nesting, not
  keys: keeping each mapping's keys made its time grow with the square
  of a mapping's width ([What the pilot found](#what-the-pilot-found)).
  The check is this crate's `TreeContract` sink (it began as aless's
  `export::UniqueMembers`, and moved here with the lift side so that no
  host writes its own); the fallback stays the host's. `--render json` and
  `--render csv` keep their native renderers; the registry's JSON entry
  names the `json` native, which is the render crate's renderer, and its
  CSV entry names the library's `csv`, which runs natively when its
  options match a renderer dialect, so a program that renders JSON or
  CSV through a part runs natively too. A composed CSV render runs under
  the export's policies, an absent member as an empty field
  (`:missing ""`) and a repeated member's last value
  (`Program::with_duplicates`), which the host sets: a program compiles
  under `Duplicates::Reject`, and the library's `csv-options` map a
  missing cell to `:error`. Under `--alchemy`, where `--render` names
  the renderer for the program's result today, the program's output
  shape takes the source's place in the composition: JSON events are a
  tree, a table is records, and a text is written as it is and refuses
  a renderer, as it does now. The loss declaration goes to standard
  error on a write that succeeds, since standard output carries the
  answer alone, and into a field of the `{"error": …}` object on a
  refusal, which the contract lets a field join.

## The pilot

CSV to YAML, through `aless --render yaml FILE.csv`, and YAML to CSV
where the tree is an array of objects, through the same route and held
to the native bytes `--render csv` writes today. The pilot proves the
render side with a format alchemy cannot render yet (a tree of any
nesting) and the adapter side with the inferred table. Each step is one pull request, in
this order (the dependencies are below the list):

0. **admin: the descriptor task.** The task that regenerates manifests
   learns to keep the `translate` object (`reads`, `writes`, `lift`,
   `render`, `loss`) and to check its shape, a part's path included, so
   that the first manifest to carry it survives regeneration. The design
   first had `plugin.schema.json` gain the object, but no schema is
   published (tabnas/admin#94), so the task is the one check there is.
1. **alchemy: `events`.** A native, `events input -> Stream<Event>`,
   turning `JsonEvents` into items a `scan-emit` can read: `object-start`,
   `object-end`, `array-start`, `array-end`, `(key name)` and
   `(scalar value)`, as constructor tags a `match` takes. Nothing today
   lets a program see an event: a program that reads the input item by
   item (`map`, `filter` or `scan-emit` over it) is refused by the
   checker with `protocol_mismatch` ("select or route what the stream
   should yield"), `route` delivers whole selected values, and alchemy has no fold over a value, no
   iteration over a record's members and refuses recursion, so a
   renderer of arbitrary nesting cannot be written. The four container
   events are native constants, matched as `table-end` is; `key` and
   `scalar` are constructors of one field, matched as `(key name)` and
   `(scalar value)`. With `events`, the renderer is a state machine
   whose state is the container stack. alchemy builds a vector only
   whole (`vector`), takes one apart only by a pattern of fixed length,
   and has no push, pop, top or count, so the same pull request adds
   four bounded value operators over a vector, `push`, `pop`, `top` and
   `count`; `quoted string -> String`, the double-quoted form of a
   string with `"`, `\`, U+0000 to U+001F and U+007F to U+009F
   escaped, which JSON and YAML both read; and `repeat count string ->
   String`, the string that many times over, refused past
   `max_scalar_bytes`, which is how a line's indentation (`count` of
   the stack, two spaces each) is made; a keyword literal in a `match`
   pattern is the discrimination a marker needs. A step answers several
   items for one event (the indentation, the quoted key, the colon, the
   value, the line's end), and `join ""` downstream writes them: no
   operator joins strings into a string, and none is needed. The stack is
   then flat, one keyword per open container and the held line, its
   length the document's nesting, and it is what the stage retains,
   measured under `max_metadata_bytes` on every change; `explain`
   reports the stage as conditional, as it reports every `scan-emit`.
2. **transduce, then alchemy: the file on a failure and
   `compile_sources`.** The `file` field on `Fail`, then the linking
   above, with the diagnostics test: an error in the second source
   names the second file, in the failure's display and in the field.
3. **yaml: the render.** `yaml-render`, block style, in an always-quoted
   profile that parallels the CSV renderer's: strings and keys
   double-quoted through `quoted` (JSON's escapes, which YAML's
   double-quoted form takes, and the C1 controls YAML's printable set
   excludes), so that no string is read back as one of the forty
   spellings of true, false, null and the non-finite numbers, as a
   number, or as a nested mapping at its first `: `; numbers by their
   lexeme, which reaches a render only when it is a JSON number (the
   rule-event adapter keeps no other token text, and a walked value
   carries none), and every JSON number is a YAML 1.2 core-schema
   number, so a JSON5 `Infinity` or `0xFF` never arrives as its
   spelling; a non-finite number (which the reader builds from `.inf`
   and `.nan`, and JSON5 from its own spellings) as `.inf`, `-.inf` or
   `.nan`; `null`, `true` and
   `false`; empty containers as `{}` and `[]`, a mapping under a key on the next lines two spaces in, a
   sequence item as `- `, a mapping as a sequence item with its first
   key on the item's line, a root scalar alone, one document and a
   trailing newline. A key longer than 1024 characters, past which YAML
   1.2 stops reading an implicit key, is written in the explicit form,
   `? "key"` on its own line and `: value` on the next; a sequence under
   a key, and a sequence inside a sequence, start on the next line two
   spaces in. A number with no lexeme, which is every number from a
   walked value (CSV at the root is read a record at a time and walked,
   so CSV to YAML carries none), is written as the JSON renderer writes
   it, and the crate's test pins that with a lexeme-less input. The
   render takes a tree's events, each key once per mapping: YAML 1.2
   forbids a repeated key, and a mapping written with one would be read
   by a conforming reader as nothing, or as the last, not as the
   document. A walked value keeps that contract by construction; an
   incremental source streams a repeated member as it was read, so the
   host refuses one before the render sees it (see [What the pilot
   found](#what-the-pilot-found), which says why the render does not
   keep the keys itself). The state is one marker per open container,
   so it grows with a document's nesting alone, measured under
   `max_metadata_bytes`. A container's opening line is held until its
   first child or its end, so that an empty container writes as `{}` or
   `[]` on the key's line. The crate's test reads every YAML fixture, writes
   it through the render and reads it back: the value is the same,
   compared as JSON text with the non-finite numbers by their YAML
   spellings, and
   the loss declaration says what is not (comments, anchors and
   aliases, which the reader resolves by copying, tags, styles, and a
   stream of several documents, which the reader builds as one sequence
   and the render writes back as one). The manifest gains its
   `translate` object; the crate exposes `render_text()` and
   `manifest_text()`.
4. **alchemy: the inferred binding.** `table-from-json` with `:columns`
   given as the keyword `:infer`, lowered to the transducer's
   `Schema::Infer` natively, and, for the interpreted twin the
   differential test needs, a value operator `keys record -> Vector`,
   which the library's `table-step` uses on the first row. The row
   policy (a scalar row as a `value` column, an array row refused) stays
   the host's `Rows` sink in front of the program, as the adapter's
   description says, so the native binding, which refuses a first row
   that is not an object, and the interpreted twin see object rows
   alone and agree. `keys` is a bounded operation over one record, not
   a fold.
5. **aless: the wiring.** The registry, `--render yaml`, the shape
   choice (CSV reads as a tree and YAML writes from one, so CSV to YAML
   composes no adapter and runs the render over the source's events as
   they are),
   the refusal for a format with no render, and the tests in
   `tests/agent.rs`: the CSV fixture's YAML bytes pinned, and a YAML
   fixture whose root is an array of objects (the one aless has is a
   mapping, which both paths refuse; the step adds `records.yaml`)
   written as CSV equal to the native path's bytes, under the export's
   policies above. The source plan is the format's and the path's, not
   the program's (a program runs at the root, and CSV at the root is
   read a record at a time), so CSV to YAML streams; a retention test
   pins it, ten times the rows leaving the retained high-water mark
   flat, as transduce's own does. The README's "Scripts and agents"
   section, `--help` and the skill say what `--render` now takes.

A lift is not in the pilot. CSV's events carry its records already,
one object per row keyed by the header, so a records target reaches
them through the adapter and a lift would be the adapter under another
name. The lift side waits for the first format whose events do not
carry its shape: a Markdown table, whose tree is a block with a header
row and rows of cells.

The steps depend on each other so: 3 needs 1 (and 2, for the file its
crate's diagnostics name); 4 needs nothing but alchemy; 5 needs 2, 3
and 4. Step 3 tests a part in a repository that does not depend on
alchemy today (yaml's crate has `tabnas-support` alone, and its
`ci/rust/run.sh` clones four siblings), so it needs the maintainer's
instruction for that repository's dependencies (alchemy, transduce and
render by path, and the sibling list in the gate) before its own test
can run; until it is given, the test lives in aless's suite, which has
every crate.

What is deliberately not in the pilot: a native YAML renderer. The
snippet is the specification; if its per-event cost through the
interpreter (a closure applied per event) is more than a viewer's export
should pay, a native renderer in the render crate takes over as the fast
path the way `csv` runs natively when its options match the dialect, and
the differential test holds the two equal. The measurement comes first.

## What the pilot found

The pilot landed as tabnas/admin#93, tabnas/alchemy#9, #10, #11 and #12,
tabnas/transduce#5 and #6, tabnas/yaml#87 and rjrodger/aless#21. Where it
departs from the steps above, and what it measured:

- **No manifest schema is published.** The `$schema` URL every manifest
  names answers 404 (tabnas/admin#94), so step 0 changed the descriptor
  task alone: it keeps the `translate` object and checks its shape, a
  part's path included, which must stay inside the repository, by its
  spelling and through any symlink.
- **The render needed three more natives**: `length` and `compare`, for
  the 1024-character key, and `number-class`, for `.inf`, `-.inf` and
  `.nan` (tabnas/alchemy#12).
- **The render keeps no keys.** Step 3 first had each open mapping keep
  the keys it had written, to refuse a repeat. That state grows with a
  mapping's width, and since `put` copies a record and a scan-emit state
  is measured again on every change, the render's time grew with the
  square of one mapping's width: 10.9 s for 16,000 keys in a release
  build, against 0.34 s once the state was one marker per open
  container. So the render takes a tree's events, each key once per
  mapping, one root value, and the check that matters is the host's,
  which every host keeps ([In the
  host](#where-the-parts-live-and-how-they-reach-the-host)). aless
  holds a stream to the contract natively: a member the parse streams
  twice (JSON's `{"a":1,"a":2}`, which the incremental source does
  stream) is `DUPLICATE_MEMBER`, and events no tree has are
  `STREAMABILITY_UNKNOWN`. Either falls back once to the parsed value,
  as `--json` reads it, when nothing had been written. The second case
  was real: for a YAML key that is itself a mapping, the incremental
  source streamed the member's value before its key
  (tabnas/transduce#7); the rule-event adapter now refuses that shape
  itself, with `STREAMABILITY_UNKNOWN` when the value opens, and the
  host's check stands behind it for whatever else a grammar can
  produce. Behind the host's check, the render fails a
  stream that breaks nesting, naming the break, with `INPUT_INVALID`:
  that is the one code alchemy's `fail` raises. `PROTOCOL_ORDER_ERROR`
  would name a broken event stream better, and waits on `fail` taking a
  code.
- **The crate embeds copies.** Step 3 had the crate `include_str!` the
  files the manifest names, and a crate packaged for crates.io holds
  nothing outside `rs/`, so the verification build `cargo publish` runs
  would have failed. The crate embeds copies under `rs/translate/`,
  held to the files by its test, as [where the parts
  live](#where-the-parts-live-and-how-they-reach-the-host) now says.
- **The round trip runs in aless** (`tests/yaml_render.rs`), not in
  the crate's test step 3 names: running the render needs alchemy,
  which tabnas-yaml does not depend on, and adding that dependency is
  the maintainer's call. aless has both crates, so the test runs there,
  through the path `--render yaml` takes, over the pinned tabnas-yaml
  checkout's fixtures, found through `cargo metadata`, each case once as
  tabnas-yaml's own runner gathers them. 673 of the 694 inputs the
  reader reads come back as the same value. The other 21 meet two
  reader defects, which a checked ledger names: a quoted key at the
  start of a line after a block sequence is read into the sequence
  (tabnas/yaml#86), and a flow sequence first in an indented block
  sequence replaces it (tabnas/yaml#88, which also fails
  `matrix:\n  - [1, 2]\n  - [3, 4]`). The render's output for each is
  valid YAML 1.2.
- **`--alchemy` with `--render yaml` was refused**, since composing a
  program's JSON events with a part's render needed a way to feed one
  program's events into another program's sink. alchemy has it now:
  `Source::export_as` links a source's `export` under another name
  (tabnas/alchemy#26), so the host links the user's program as
  `program-export` and writes the `export` that calls it, in one plan
  under one set of limits, and the program's output shape takes the
  source's place in the composition as [the design](#the-design) says
  (rjrodger/aless#32).
- **A render that writes from records was read and not run.** The host
  composes the inferred table in front of one now, behind the row check
  `--render csv` has, and the first such render is Markdown's pipe table
  (tabnas/markdown#77, which also ships the fleet's first lift, from a
  table's tree to its rows); a lifted format reaches alchemy's `csv` the
  same way, composed, where a tree reaches the native CSV export as
  before (rjrodger/aless#32).
- **The adapter's loss is printed with the render's.** The host's note
  carries the adapter's sentences after the format's and names the
  adapter (`adapter`: the inferred table, or `records`), as [the
  design](#the-design) has it; the `{"error": …}` object carries the
  same `loss`.
- **A cell from several strings needed a native.** The Markdown lift
  assembles a cell from the runs a cell holds, and the renders name the
  key a typed failure concerns; `concat` answers a text and `fail` takes
  a string, so alchemy gained `string-join` (tabnas/alchemy#26).
- **The adapter refuses two more shapes it cannot follow** (tabnas/transduce#12):
  a container the grammar opens inside a map before the member's key
  (#7's case) and a container it streamed that the grammar never stored
  (a jsonic pair inside a list, dropped when `list.pair` is off), both
  `STREAMABILITY_UNKNOWN` before anything wrong leaves, so the host's
  one fallback covers them; the tree contract moved into this crate as
  `TreeContract` (#11).
- **The loss declaration** reaches standard error on a write that
  succeeds, as a JSON object whose `warning` member holds `kind`
  (`"loss"`), `message`, `file`, `render` and `loss`, the render's
  sentences; an error met while writing carries the sentences as
  `loss`. It is YAML's alone: `--render json` and `--render csv` write
  nothing new on standard error.
- **The cost, measured** on 200,000 generated records (33 MB of JSON)
  with a release build on a shared four-core container:

  | `--render` | Time | Peak memory |
  |---|---|---|
  | `json` | 25.9 s | 2.09 GB |
  | `csv` | 16.6 s | 2.09 GB |
  | `yaml` | 51.8 s | 2.09 GB |

  The peak is the parse's in all three (tabnas/parser#252); the render
  retains nothing a row adds, as its retention test holds. The
  interpreted render adds about 26 s, some 8 µs an event, so YAML costs
  twice the native JSON export. Whether that is past what an export
  should pay, which would bring the native renderer above, is the
  maintainer's call.

## What this does not solve

- **Schema-first targets.** A format whose writer needs a schema the
  document does not carry (a typed CSV, a fixed-width record) needs the
  schema from somewhere; the loss declaration says so and the host asks
  for it as an option.
- **Binary targets.** `Text` is characters, and the render crate's
  output is text: a binary format (Protocol Buffers' wire form, a
  packed table) needs a byte protocol that does not exist, and is out
  of scope. The design is for textual formats, which is every format
  the fleet reads.
- **Grammars with no writer.** A format that is read but not written (a
  log format, a hand-me-down config) ships no `translate` object; the
  host refuses to write it with the reason.
- **Round trips.** Lift and render are not inverses: a CSV read and
  written back has its quoting normalised, a YAML read and written back
  has its comments gone. Fidelity is the render's documented contract,
  per format, and the loss declaration is where it is written.
- **Formats whose value is not a tree.** Markdown's grammar builds a
  tree of blocks and inlines; a Markdown render is a render of that
  tree, not of any tree, and its loss declaration says which trees it
  carries. The shape names the protocol, not the schema.

## Risks

- **Interpreted per-event cost.** A render over `events` applies a
  closure per event through the interpreter, which locks a cache on
  every lookup of a definition and allocates a frame per call; the standard
  library's `csv` would pay the same per row, and that cost is
  unmeasured: the one figure on record, about half again the native
  renderer's time on JSON for the program path, was taken with the
  native fast path on (`compile` turns it on and aless never turns it
  off), so it is the program plumbing's overhead and not the
  interpreter's per row. Per event is more often than per row. The pilot measures the YAML render against the native
  JSON renderer on the transducer's generated documents before the
  wiring lands in aless, and the native fallback above is the answer if
  the ratio is past what an export should pay. The measurement is in
  [What the pilot found](#what-the-pilot-found): twice the JSON export's
  time.
- **A part is code from another repository.** A grammar's parts run
  under the host's limits like any program, with no I/O and no access
  beyond the events; they are embedded at build time from a crate the
  host already trusts for its grammar, so a translation runs as
  `--render csv` runs today: under `--timeout` and `--max-size` when
  the caller sets them, and with no output bound (aless passes the
  transducer's default limits, whose output bound is none). A host
  running a program it did not write sets both, which is alchemy's
  guidance and is unchanged. A part must never be read from the
  document, or from a path a document names.
- **The dependency principle.** A grammar repository that tests its own
  parts needs alchemy as a development dependency, and alchemy depends
  on that grammar's sibling for its fixtures already; both are
  dependency changes, made only on the maintainer's instruction, per
  repository, when it adopts a `translate` object. Until then the host's
  tests cover the part. aless adds no dependency: it has every grammar
  crate it reads, and alchemy.
- **The inferred schema is data-dependent.** A later row's extra
  members are dropped and its absent ones are missing; that is the
  transducer's documented `Infer` and the policy `--render csv` runs
  today, and the host says it whenever the adapter runs.
- **Snippets and the ports.** TypeScript and Go do not run alchemy;
  the manifest carries the file names so that they can, and until they
  do a translation is the Rust host's. The parts are text, so the ports
  add nothing to the grammar repositories when they arrive.
- **Sequencing across repositories.** The pilot planned seven pull
  requests in five repositories and took nine, in the order it gives:
  the render needed three more natives (tabnas/alchemy#12), and the
  inferred binding a bound in the transducer (#6). The coupling this
  risk foresaw did not arise. Within the fleet a grammar repository
  would take alchemy by sibling path (admin ADR-21), so that a change
  to `events` or to the linking broke a grammar's own test the moment
  the sibling checkout moved; but tabnas-yaml does not take alchemy at
  all, since its crate embeds the render as text and the round trip that
  compiles it runs in aless, which moves by `Cargo.lock` pin. A change
  to `events` or to the linking meets the render first in aless's
  tests. A format whose own tests compile its render takes alchemy as a
  dependency, which is the maintainer's call.

## Proposed decision (for admin `DECISIONS.md`)

**ADR: translation between formats is composed from per-format parts.**
A format's repository ships its shapes, an optional lift, a render and
a loss declaration, as alchemy text named in `tabnas.plugin.json` and
embedded in its crate. The host composes lift, adapter and render into
one program; the two adapters are shared; a pair of formats is never
written by hand. A render that writes from a tree takes a tree's
events, one root value with each key once per object, and the host
holds a stream to that in front of it. What a format cannot carry is
declared, and
the host refuses where the shape is decided, before any output when it
is decided before any. The cost of N formats is one set of parts per format and two adapters,
never a program per pair.
