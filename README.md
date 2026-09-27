# tabnas-transduce

Streaming transducers over any [tabnas](https://github.com/tabnas/parser)
parser: a parse becomes a stream of source events; selectors pick out the
scopes that matter; the table transducer turns selected metadata and rows
into a flat, budgeted table protocol that a renderer such as
[tabnas-render](https://github.com/tabnas/render) writes as CSV or JSON.
The declarative language that composes all of this is
[tabnas-alchemy](https://github.com/tabnas/alchemy), and
[aless](https://github.com/rjrodger/aless) exposes it from the command
line.

```
tabnas parse ──rule events──▶ JsonEvents/1 ──▶ router/captures ──▶ TableRows/1 ──▶ renderer ──▶ text
```

- **Any parser.** The source events come from the engine's rule
  subscribers, so every tabnas grammar (JSON, JSON Lines, jsonic, YAML,
  TOML, CSV, XML, …) feeds the same transducers. Grammars the
  differential suite has verified (`json`, `json5`, `jsonc`, `jsonl`,
  `yaml`, `zon`) stream incrementally, with the streamed rows pruned from
  the engine's tree on request; the rest are walked whole. JSON Lines and
  CSV also have a line source that holds one record or one chunk at a
  time, whatever the file's size.
- **Bounded by contract.** Retained values are measured against named
  limits as they are built; a breach fails with the limit's name, never
  a silent fallback to a bigger algorithm.
- **Synchronous and push-based.** Stages are sinks called from inside the
  parse; a slow writer slows the parser. No queues, no threads, no
  hidden buffers.

Rust only for now; TypeScript and Go ports follow the fleet's usual path
later.

## Layout

| Path | What it is |
|---|---|
| [`rs/`](rs/) | the `tabnas-transduce` crate (library `tabnas_transduce`) |
| [`docs/architecture.md`](docs/architecture.md) | the design, and what was measured before it |
| [`docs/reference.md`](docs/reference.md) | the protocols, types and codes |
| [`ci/rust/run.sh`](ci/rust/run.sh) | the gate CI runs |

## Build and test

The engine and the grammars are sibling checkouts (`../parser`,
`../json`, …); `rs/Cargo.toml` names each by path.

```bash
make build
make test
```

Contributors and agents: read [`AGENTS.md`](AGENTS.md).

## License

MIT. Copyright (c) Richard Rodger.
