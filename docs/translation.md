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

A renderer is a function from one of the last two to the first. The
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
   writes from one. CSV reads as records first (a table, through its
   lift) and as a tree second (its grammar builds one object per row,
   keyed by the header, so its events are a tree as they stand), and
   writes from records. JSON Lines reads as a tree, since its source
   wraps the lines in one array whatever they hold, and records are
   reached through the adapter below with its typed refusal; it writes
   from a tree.
2. **A lift**, optional: an alchemy definition from the format's event
   stream to its first read shape's protocol, for a shape the events do
   not carry as they are. CSV's lift takes the first row's member names as the
   schema and reads every row, the first included, as wide as it. A tree format needs
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
| records | render | identity | `records` (one object per row, keyed by label): a missing cell is an absent member, and of two columns with one label the last contributes the member, the render crate's `RecordsToJson` policies, which the loss declaration of every tree target names |
| tree | render | the inferred table (pilot step 5), the policy aless's own CSV export runs today: the root must be an array and its elements are the rows; an object row's members are cells and a scalar row is one cell in a column named `value`; the first row's member names are the schema; an absent member is an empty field and a repeated member keeps its last value; an array row, or a table of mixed rows, is refused with the reason | identity |

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

Cost: N shapes, N renders, at most N lifts, one adapter per pair of
shapes (two that do anything). Testing stays at N: each repository tests
its own lift and render against its own fixtures, and the host tests the
adapters once.

## Where the parts live and how they reach the host

Every grammar repository already carries a manifest, `tabnas.plugin.json`
(`name`, `extensions`, `languageId`, `pluginKind`, and `grammar` where
the grammar is a file: the json family, markdown and feed build their
values in code and name none). The crates that have a grammar file embed
its text as a generated string, regenerated by a script and held to the
file by a test, and three of them (csv, toml, jsonc) expose it as
`grammar_text()`. The translation parts take a shorter route, since they
are text alchemy reads and nothing generates them:

- **In the manifest**, a `translate` object: `shape` (a string, or an
  array in order of preference), `lift` and `render` (paths to the
  alchemy files, relative to the repository root; `lift` absent for a
  tree format), and `loss` (an array of short sentences the host prints
  verbatim when it warns or refuses). The manifest schema at
  `tabnas.dev/schema/plugin.schema.json` gains the object; that is the
  admin repository's change, and a manifest without it means a format
  that is read and not written. The language server's fleet registry is
  generated from these manifests and copies named fields only, so the
  object does not reach it, and nothing there needs it: the host reads
  the parts from the crates, below.
- **In the repository**, the files: `alchemy/lift.alc` and
  `alchemy/render.alc`, each a library of definitions with no `export`.
  Every definition's name is prefixed by the format's name
  (`csv-lift`, `yaml-render`, `yaml-scalar`), so that no two formats'
  helpers collide when the host links them into one program; the entry
  points are `<format>-lift` and `<format>-render`, and nothing else in
  a file is the host's to call. A lift takes the format's `JsonEvents`
  and answers its shape's protocol; a render takes the shape's protocol
  and answers `Text`. The parts declare no types: they are compiled by the
  program path, whose definitions the checker infers from use (the
  standard library's are declared in Rust, in `check::stdlib_signature`,
  and the parts do not go that way), and a render handed the wrong shape
  fails the composed program's check with `protocol_mismatch` at the
  render's file and line.
- **In the Rust crate**, `include_str!` of each file (the fleet's first:
  the compiler reads the file, so the embedded text and the file cannot
  differ and no test holds them equal), exposed as `lift_text()` and
  `render_text()`, and `include_str!` of the manifest itself, exposed
  as `manifest_text()`, so that the shapes and the loss declaration
  reach the host from the one place they are written and nothing is
  copied into it; yaml, whose grammar text is private today, gains
  these accessors and nothing else. The manifest names the files so that
  the TypeScript and Go ports, when they run alchemy, find the same
  text; until then translation is the Rust host's.
- **In alchemy**, the linking: `compile_sources`, taking several named
  sources and resolving them into one namespace (a program's is today's
  `compile`, with one source). A span already carries its file
  (`SourceSpan.file`); what positions it, the checker and the runtime,
  holds one source text today and must hold one per file, and the file
  goes into the message as the standard library's does today
  (`at stdlib/table.alc:38:5`), since a `Fail` carries a row and a
  column and no file. A file field on `Fail` is transduce's change and
  the pilot does not need it. A collision is `duplicate_def`, as within
  one file. The
  standard library stays what it is: process-wide, loaded once, and
  fatal if its own text fails to load. A format's parts are not loaded
  that way: they are compiled per translation, and a part that fails to
  compile is a refusal naming the format and the file, never an abort.
- **In the host**, a registry from format to parts, filled from the
  crates' functions (the manifest's `translate` object for the shapes
  and the loss, the two texts; aless depends on every grammar crate it
  reads already), and
  a `--render <format>` that composes the program from the registry and
  runs it through the same plumbing as `--alchemy`: the input read as
  the source's plan says, the parse pruned under the program's rows, the
  host's limits and timeout on the run. `--render json` and
  `--render csv` keep their native renderers; the registry's JSON entry
  names the `json` native, which is the render crate's renderer, and its
  CSV entry names the library's `csv`, which runs natively when its
  options match a renderer dialect, so a program that renders JSON or
  CSV through a part runs natively too. A composed CSV render runs under
  the export's policies, an absent member as an empty field
  (`:missing ""`) and a repeated member's last value
  (`Program::with_duplicates`), which the host sets: a program compiles
  under `Duplicates::Reject`, and the library's `csv-options` map a
  missing cell to `:error`.

## The pilot

CSV to YAML, through `aless --render yaml FILE.csv`, and YAML to CSV
where the tree is an array of objects, through the same route and held
to the native bytes `--render csv` writes today. The pilot proves the
render side with a format alchemy cannot render yet (a tree of any
nesting) and the lift side with the format whose lift is the standard
table binding under a new policy. Each step is one pull request, in
this order, because each needs the one before:

0. **admin: the manifest schema.** `plugin.schema.json` gains the
   `translate` object (`shape`, `lift`, `render`, `loss`), so that the
   first manifest to carry it validates.
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
   `count`, and `quoted string -> String`, the double-quoted form of a
   string with `"`, `\`, U+0000 to U+001F and U+007F to U+009F
   escaped, which JSON and YAML both read; a keyword literal in a
   `match` pattern is the discrimination a marker needs. The stack is
   then flat, one keyword per open container and the held line, its
   length the document's nesting, and it is what the stage retains,
   measured under `max_metadata_bytes` on every change; `explain`
   reports the stage as conditional, as it reports every `scan-emit`.
2. **alchemy: `compile_sources`.** The linking above, with the
   diagnostics test: an error in the second source names the second
   file.
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
   spaces in. A number with no lexeme is written as the JSON renderer
   writes it. A repeated key in one mapping, which the incremental
   source streams as it was read, is written as it was read: YAML 1.2
   forbids it, tabnas-yaml reads it and keeps the last, and the loss
   declaration says so. A container's opening line is held until its first
   child or its end, so that an empty container writes as `{}` or `[]`
   on the key's line. The crate's test reads every YAML fixture, writes
   it through the render and reads it back: the value is the same,
   compared as JSON text with the non-finite numbers by their YAML
   spellings, and
   the loss declaration says what is not (comments, anchors and
   aliases, which the reader resolves by copying, tags, styles, and a
   stream of several documents, which the reader builds as one sequence
   and the render writes back as one). The manifest gains its
   `translate` object; the crate exposes `render_text()`.
4. **aless: the wiring.** The registry, `--render yaml`, the shared-shape
   choice (CSV's second shape is a tree, so CSV to YAML composes no
   adapter and runs the render over the source's events as they are),
   the refusal for a format with no render, and the tests in
   `tests/agent.rs`: the CSV fixture's YAML bytes pinned, and the YAML
   fixture written as CSV equal to the native path's bytes, under the
   export's policies above. The README's
   "Scripts and agents" section, `--help` and the skill say what
   `--render` now takes.
5. **alchemy: the inferred binding.** `table-from-json` with `:columns`
   given as the keyword `:infer`, lowered to the transducer's
   `Schema::Infer` natively, and, for the interpreted twin the
   differential test needs, a value operator `keys record -> Vector`,
   which the library's `table-step` uses on the first row, with the
   export's row policy (a scalar row as a `value` column, an array row
   refused) in the same text, so that the adapter and `--render csv`
   are one definition. `keys` is a bounded operation over one record,
   not a fold.
6. **csv: the lift.** `csv-lift`: the inferred binding over the root's
   elements. Its use is a target of shape records (a Markdown table, a
   CSV with another dialect); CSV to YAML does not need it. The crate's
   test holds the lift's rows on every CSV fixture to the native
   inferred table's.

What is deliberately not in the pilot: a native YAML renderer. The
snippet is the specification; if its per-event cost through the
interpreter (a closure applied per event) is more than a viewer's export
should pay, a native renderer in the render crate takes over as the fast
path the way `csv` runs natively when its options match the dialect, and
the differential test holds the two equal. The measurement comes first.

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
  the ratio is past what an export should pay.
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
  today, and the loss declaration of every records target says it.
- **Snippets and the ports.** TypeScript and Go do not run alchemy;
  the manifest carries the file names so that they can, and until they
  do a translation is the Rust host's. The parts are text, so the ports
  add nothing to the grammar repositories when they arrive.
- **Sequencing across repositories.** Seven pull requests in five
  repositories, each depending on the one before; within the fleet a
  grammar repository takes alchemy by sibling path (admin ADR-21), so a
  change to `events` or to the linking reaches a grammar's own test the
  moment the sibling checkout moves, and breaks it at once; only aless
  moves by `Cargo.lock` pin.

## Proposed decision (for admin `DECISIONS.md`)

**ADR: translation between formats is composed from per-format parts.**
A format's repository ships its shape, an optional lift, a render and a
loss declaration, as alchemy text named in `tabnas.plugin.json` and
embedded in its crate. The host composes lift, adapter and render into
one program; the adapters are the standard library's; a pair of formats
is never written by hand. What a format cannot carry is declared, and
the host refuses before it writes. The cost of N formats is N parts.
