# Measurements

Numbers this design was made against, taken on 2026-09-27 on one core of
the development container (an ordinary cloud VM, no tuning). They are
here so that the streaming claims stay honest; re-measure before quoting
them elsewhere.

## The engine, alone

A prototype driver parsed generated inputs with each grammar's `make()`
parser and nothing else (`plain`), then with a `subscribe_rule_done`
adapter producing events (`events`), then the same adapter dropping each
streamed array element from the tree (`prune`). Peak resident memory is
`VmHWM` for the process, which held the source text (read once) plus the
parse.

| Input | Size | Mode | Time | Throughput | Peak RSS |
|---|---|---|---|---|---|
| records JSON (150k records) | 24.6 MB | plain | 23.9 s | 1.0 MB/s | 1640 MB |
| | | events | 27.3 s | 0.9 MB/s | 1640 MB |
| | | prune | 19.7 s | 1.3 MB/s | 1618 MB |
| records JSON Lines (150k lines) | 24.6 MB | plain | 20.3 s | 1.2 MB/s | 1641 MB |
| | | events | 20.5 s | 1.2 MB/s | 1640 MB |
| | | prune | 19.3 s | 1.3 MB/s | 1618 MB |
| CSV (400k rows) | 30.9 MB | plain | 43.6 s | 0.7 MB/s | 2834 MB |
| | | events | 48.4 s | 0.6 MB/s | 2832 MB |
| | | prune | 35.1 s | 0.9 MB/s | 2249 MB |
| YAML (150k items) | 17.4 MB | plain | 16.0 s | 1.1 MB/s | 1309 MB |
| | | events | 19.3 s | 0.9 MB/s | 1309 MB |
| flat array, 1.5M numbers | 10.9 MB | plain | 29.3 s | 0.4 MB/s | 2760 MB |
| flat array, 1M strings | 9.9 MB | plain | 7.4 s | 1.3 MB/s | 1976 MB |
| flat object, 1M members | 16.8 MB | plain | 16.3 s | 1.0 MB/s | 2975 MB |
| one 20 MB string | 20.0 MB | plain | 0.38 s | 52.8 MB/s | 367 MB |

The canonical TypeScript engine (`@tabnas/json` from the registry, Node
22) on the same files: the records JSON in 22.6 s (1.1 MB/s, about 4.0 GB
resident); the flat array of numbers in 6.7 s (1.6 MB/s, about 4.2 GB).

What the table says:

- **Cost is per rule, not per byte.** One 20 MB string is 53 MB/s; 1.5M
  small numbers are 0.4 MB/s. About 20 µs and about 1.8 KB of peak memory
  go to each array element.
- **The retention is the engine's rule history, not the value tree.**
  Pruning the streamed elements from the tree (the `prune` rows) recovers
  little. Each rule keeps snapshots of the rule it replaced and of its
  children until the enclosing container closes, and the links are
  transitive: bounding one link (an experiment that kept one step of
  `prev_rule`) changed the peak by nothing, because the child snapshots'
  `parent_rule` links reach the same chain.
- **Both ports agree.** The Rust port is not slower or larger than the
  canonical engine; this is the design.
- **The adapter itself is cheap.** `events` costs 0–15% over `plain`, and
  `prune` is faster than `plain` (less to drop at the end).

## What follows for this crate

- Bounded-memory streaming of one large document needs the engine to
  bound its rule history. It can since engine 0.12.6
  (`options.rule.history`, tabnas/parser#252), which a host sets on the
  parser it hands to a source; aless sets 3 (rjrodger/aless#33, and
  "Through aless" below). Without it a streaming source bounds what the
  transducer retains, and the engine's own retention stands.
- JSON Lines and CSV, which `LinesSource` read a record or a chunk of
  records at a time until step 12, are read through their parts from a
  reader, a window at a time, so their memory is bounded whatever the
  file size: JSON Lines through `TransduceTree` ("From a reader" below),
  CSV through `TransduceRecords` ("Records, natively").
- Throughput through this crate is the engine's: about 1 MB/s for
  record-shaped data. The stages downstream (events, router, table, CSV)
  are measured separately by `cargo bench` and are not the bottleneck.

## The stages after the engine

Measured by `tabnas-render`'s criterion bench (`cargo bench` in
`render/rs`), one core of a shared two-vCPU VM (Arm Neoverse-N1),
synthetic input, 2026-10-23:

| Stage | Input | Rate |
|---|---|---|
| `CsvRenderer` over a discarding writer (renderer only, no parse) | 20k rows × 5 cells of `TableRows/1` events | about 3.5 M rows/s, about 245 MiB/s of CSV out |
| `json_to_csv/text_to_csv_through_the_part`: JSON text → `ParserEventSource` (pruned) → JSON's part (`TransduceTree`) → `TableFromJson` → `CsvRenderer` → `WriteOut` | the 20k-record worked-example document (1.66 MB) | 1.86 s, 0.85 MiB/s of JSON in, 0.42 MiB/s of CSV out |
| `ValueSource` walk + `JsonRenderer` | a parsed 1.66 MB, 20k-record document | about 46 MiB/s of source (about 34 ms) |

So the pipeline downstream of the parse runs two orders of magnitude
faster than the parse itself, and the parse through the part is the
throughput of the whole. The numbers are also recorded in render's
`docs/reference.md`, beside the first measurement, on 2026-09-27 on a
2.1 GHz Xeon vCPU, when the chain read through the incremental source.

## This crate's stages

`cargo bench` in `rs/` (`benches/throughput.rs`), release build, one core
of a shared two-vCPU VM (Arm Neoverse-N1), 2026-10-23, on the generated
worked-example document of 20,000 records (1.7 MB; `tests/support`
generates it). Criterion's mean of 10 samples for the parse-bound
groups. The last three rows are `main`'s groups before step 13 of
alchemy's streaming plan retired the rule-event adapter, run just after
the rest on the same machine, with their own `parse_only` (1.21 s).

| Group | What runs | Time | Throughput |
|---|---|---|---|
| `parse_only/json` | `tabnas_json::make().parse` alone | 1.22 s | 1.30 MiB/s |
| `part/events_into_count_sink` | JSON's part, natively (`TransduceTree`), over `ParserEventSource`'s passes, behind `Guarded`, into `CountSink` | 1.90 s | 0.83 MiB/s |
| `part/events_pruned_into_count_sink` | the same, the engine's arrays emptied behind the stream | 1.79 s | 0.89 MiB/s |
| `walk/value_source_into_count_sink` | `ValueSource` over the parsed value | 8.6 ms | 183 MiB/s |
| `table_from_recording/router_and_table_into_count_table` | `Router` + `TableFromJson` from a recording, no parse | 33.5 ms | 47.4 MiB/s |
| `table_from_text/part_pruned_into_table` | text to table rows through JSON's part, pruned | 1.87 s | 0.85 MiB/s |
| `incremental/events_into_count_sink` (retired) | the rule-event adapter, no pruning, into `CountSink` | 1.58 s | 1.00 MiB/s |
| `incremental/events_pruned_into_count_sink` (retired) | the same with `Prune::Under(records)` | 1.51 s | 1.05 MiB/s |
| `table_from_text/incremental_pruned_into_table` (retired) | text to table rows, incremental and pruned | 1.58 s | 1.01 MiB/s |

What the table says, in the terms of the engine table above:

- **The part costs about half again the bare parse** (1.90 s against
  1.22 s): the source copies each pass the engine completes, its rule's
  name, alternate and tokens with their text, and the native
  `transduce-tree` makes the document's events of it. Pruning gives some
  of it back (1.79 s). That is the price of streaming a format through
  the events its package declares rather than parsing it whole.
- **The part is about a fifth slower than the inference it replaced.**
  The rule-event adapter cost about 30% over the parse here (1.58 s
  against 1.21 s; about a fifth on the Xeon, 1.35 s against 1.10 s, on
  2026-09-27), and text to table rows took 1.58 s where the part takes
  1.87 s. aless has read JSON through its part since rjrodger/aless#82
  (step 9), and alchemy-cli since tabnas/alchemy-cli#50, so step 13
  removes a route no host took; the hosts have paid this since then.
- **The stages downstream are not where the time goes.** Walking the
  parsed value is 183 MiB/s and the router plus the table transducer,
  materializing every record and projecting three columns, is 47 MiB/s:
  about 2% of the chain from text to rows. The chain runs at the speed
  of the parse through the part, 0.85 MiB/s here.
- **Memory is the other axis, and it is the engine's.** With pruning the
  transducer retains one record at a time (`captured_bytes_high` says so,
  and `tests/retention_test.rs` checks the engine's tree itself holds no
  streamed row once the run is over), while the engine's rule history
  grows with the document unless the host bounds it (`options.rule.history`,
  "What follows for this crate").

## Through aless

`aless --render` and `aless --paths` (release builds of aless `main` and
of rjrodger/aless#33, this container, 2026-10-01) on generated record
documents: the median of three interleaved runs each, and the peak
resident set of the aless process from `wait4`, which was the same in
every round. #33 sets the engine's `options.rule.history` to 3 on every
parser aless makes; `main` keeps the whole history. Both wrote the same
output every time.

| Input | Run | `main` | history 3 |
|---|---|---|---|
| JSON Lines, 22.2 MB | `--render csv`, `LinesSource` | 21.0 s, 11 MiB | 20.9 s, 11 MiB |
| JSON Lines, 44.4 MB (the same file twice) | `--render csv`, `LinesSource` | 41.4 s, 11 MiB | 41.9 s, 11 MiB |
| CSV, 27.5 MB | `--render csv`, `LinesSource` | 30.9 s, 46 MiB | 31.4 s, 46 MiB |
| CSV, 55.0 MB (twice) | `--render csv`, `LinesSource` | 64.1 s, 46 MiB | 62.8 s, 46 MiB |
| JSON, 22.6 MB, `--path` to the records | `--render csv`, incremental and pruned | 22.8 s, 1580 MiB | 20.3 s, 311 MiB |
| JSON, 22.6 MB | `--render json`, incremental | 23.1 s, 1588 MiB | 21.6 s, 617 MiB |
| YAML, 19.6 MB, `--path` to the records | `--render csv`, incremental, unpruned | 23.7 s, 1286 MiB | 22.2 s, 439 MiB |
| JSON, 60.5 MB of records | `--paths`, the whole document parsed | 67.2 s, 4675 MiB | 44.6 s, 2103 MiB |
| JSON, a flat array of 1.5M numbers, 11.8 MB | `--paths` | 14.6 s, 2858 MiB | 10.0 s, 327 MiB |

The line sources hold memory flat whatever the file size, and the bound
does not apply to them: they parse a record at a time. A single JSON or
YAML document cost what the bare engine parse of it costs, about 1.6 GB
for 23 MB of records (the first table), because the engine's rule
history was what was retained. With three steps of it kept, the same
document takes a fifth of the memory (pruned, to CSV) to two fifths (to
JSON), and 6 to 11 per cent less time; a whole-document parse of 60 MB
takes a third less. What remains is the parse's own working set and the
transducer's one record. Throughput is the engine's parse rate
throughout.

## From a reader

`aless --render csv` again (release builds, this container, 2026-10-22)
on documents in the shape `rs/tests/support` generates: the median of
three interleaved runs, within 3% of each other, and the peak resident
set of the aless process from `wait4` (`/usr/bin/time`), within 2%.
Every pair wrote the same output in every round.

The JSON rows select the records (`--path .response.payload.deep.records`)
and read them through tabnas-json's part, pruned. aless 0.4.8 hands the
part the file's text, read whole. Since rjrodger/aless#89 aless hands it
a reader (`ParserEventSource::from_reader`), which the parser reads a
window at a time, since tabnas-json declares `lex.window` (tabnas-parser
0.12.15, admin ADR-30). The JSON Lines and CSV rows set `LinesSource`,
which aless reads both with, beside the general path that step 12 of
alchemy's streaming plan would leave them on: a local build of aless
`main` that reads each through its crate's part, from a reader, a window
at a time, pruned. (aless reads no CSV through a part today, so that
build adds tabnas-csv's part to the formats it reads through one.)

| Input | Read | Time | Peak RSS |
|---|---|---|---|
| JSON, 21.6 MB, 250,000 records | the text, whole (aless 0.4.8) | 23.7 s | 136 MiB |
| | from a reader (aless#89) | 23.6 s | 12 MiB |
| JSON, 43.6 MB, 500,000 records | the text, whole | 47.0 s | 261 MiB |
| | from a reader | 47.0 s | 12 MiB |
| JSON Lines, 21.6 MB, 250,000 records | `LinesSource` | 19.4 s | 11 MiB |
| | tabnas-jsonl's part, from a reader | 23.2 s | 12 MiB |
| JSON Lines, 43.6 MB, 500,000 records | `LinesSource` | 38.2 s | 11 MiB |
| | tabnas-jsonl's part, from a reader | 46.5 s | 12 MiB |
| CSV, 26.9 MB, 700,000 rows | `LinesSource` | 27.1 s | 17 MiB |
| | tabnas-csv's part, from a reader | 112.3 s | 12 MiB |
| CSV, 55.0 MB, 1,400,000 rows | `LinesSource` | 53.9 s | 18 MiB |
| | tabnas-csv's part, from a reader | 223.7 s | 12 MiB |

What the table says:

- **A document read from a reader holds a window of it, not its text.**
  Read whole, the JSON cost about six bytes a byte of the document (its
  text, and the lexer's copy of it): 136 MiB for 21.6 MB, and twice that
  for twice the file. From a reader the run holds 12 MiB whatever the
  file's size, in the same time to within 0.5%.
- **On the general path, JSON Lines and CSV hold memory flat as well.**
  Each is read a window at a time and pruned, so step 12 costs no memory:
  12 MiB, beside `LinesSource`'s 11 MiB to 18 MiB.
- **The general path costs time, and the part decides how much.**
  `LinesSource` parses a record at a time and makes its events natively.
  tabnas-jsonl's part is a rule table for transduce's native
  `transduce-tree`, a fifth slower than that. tabnas-csv's part is
  hand-written alchemy, two `scan-emit` scans whose steps alchemy's
  evaluator runs on every pass, and it takes about four times as long.
  The maintainer ruled on 2026-10-23 that step 12 retires `LinesSource`
  for JSON Lines, and that CSV keeps it until its part has a native
  route, which is a step of its own.

## Records, natively

A probe wrote tabnas-csv's transducer part natively, the state machine
`TransduceRecords` now is. It read the generated CSV (`tests/support`'s
`records_csv`) from a file, pruned, into a table (`TableFromJson`, its
schema inferred). These are the median of three interleaved runs on a
shared two-vCPU VM (Arm Neoverse-N1), 2026-10-23. Instructions are from
`perf stat` at 100,000 rows.

| Route | 100,000 rows (3.6 MB) | 700,000 rows (26.9 MB) | Peak | Instructions |
|---|---|---|---|---|
| `LinesSource` | 3.93 s | 27.8 s | 12.3 MB | 24.0 G |
| the native records route | 5.25 s | 36.9 s | 6.8 MB | 32.6 G |
| `transduce-tree` over the passes, no header | 5.35 s | 37.6 s | 6.8 MB | 32.4 G |
| the engine alone, `parse_reader`, the tree kept | 3.75 s | | 73 MB | 22.7 G |
| `ParserEventSource`, into a sink that keeps nothing | 5.00 s | | 6.8 MB | 31.5 G |

What the table says:

- **The native route costs a third more than the line source** and
  holds half its memory, flat with the file's size. csv's interpreted
  part took four times as long ("From a reader").
- **The third is the source's passes, not the records.**
  `ParserEventSource` alone takes 5.00 s, and the records and the table
  add a quarter of a second. In a profile of the source, the engine's
  notification and this crate's pass building take 23% of the run, and
  allocation about 29%. tabnas/parser#258 is that cost.
