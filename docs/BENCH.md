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
  bound its rule history; until then the incremental source bounds what
  the transducer retains, and the engine's own retention stands.
- JSON Lines and CSV are parsed a record (or a chunk of records) at a
  time by `LinesSource`, so their memory is bounded whatever the file
  size, at the engine's per-record cost.
- Throughput through this crate is the engine's: about 1 MB/s for
  record-shaped data. The stages downstream (events, router, table, CSV)
  are measured separately by `cargo bench` and are not the bottleneck.

## The stages after the engine

Measured by `tabnas-render`'s criterion bench (`cargo bench` in
`render/rs`), one core, synthetic input, 2026-09-27:

| Stage | Input | Rate |
|---|---|---|
| `CsvRenderer` over a discarding writer | 20k rows × 5 cells of `TableRows/1` events | about 4.3 M rows/s, about 290 MiB/s of CSV out |
| `ValueSource` walk + `JsonRenderer` | a parsed 1.66 MB, 20k-record document | about 78 MiB/s of source (about 20 ms) |

So the pipeline downstream of the parse runs two orders of magnitude
faster than the parse itself, and the engine's per-rule cost is the
throughput of the whole. The numbers are also recorded in render's
`docs/reference.md`.

## This crate's stages

`cargo bench` in `rs/` (`benches/throughput.rs`), release build, one core
of the same kind of container (a 2.1 GHz Xeon vCPU), on the generated
worked-example document of 20,000 records (1.7 MB; `tests/support`
generates it). Criterion's mean of 10 samples for the parse-bound
groups.

| Group | What runs | Time | Throughput |
|---|---|---|---|
| `parse_only/json` | `tabnas_json::make().parse` alone | 1.10 s | 1.45 MiB/s |
| `incremental/events_into_count_sink` | the rule-event adapter, no pruning, into `CountSink` | 1.35 s | 1.18 MiB/s |
| `incremental/events_pruned_into_count_sink` | the same with `Prune::Under(records)` | 1.32 s | 1.21 MiB/s |
| `walk/value_source_into_count_sink` | `ValueSource` over the parsed value | 4.1 ms | 385 MiB/s |
| `table_from_recording/router_and_table_into_count_table` | `Router` + `TableFromJson` from a recording, no parse | 41.8 ms | 37.9 MiB/s |
| `table_from_text/incremental_pruned_into_table` | text to table rows, incremental and pruned | 1.32 s | 1.20 MiB/s |

What the table says, in the terms of the engine table above:

- **The adapter costs about 20% over the bare parse** (1.35 s against
  1.10 s), the per-pass `Rule` clone and the subscriber's lock included,
  and pruning gives a little of it back. That is the whole price of
  streaming a verified grammar instead of parsing it whole.
- **The stages downstream are not where the time goes.** Walking the
  parsed value is 385 MiB/s and the router plus the table transducer,
  materializing every record and projecting three columns, is 38 MiB/s:
  about 3% of the chain from text to rows. The chain runs at the engine's
  speed, 1.2 MiB/s here.
- **Memory is the other axis, and it is the engine's.** With pruning the
  transducer retains one record at a time (`captured_bytes_high` says so),
  while the engine's rule history grows with the document, as the first
  table shows; the line sources sidestep that for JSON Lines and CSV by
  parsing a record or a chunk at a time.
