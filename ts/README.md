# @tabnas/transduce

The TypeScript implementation of Tabnas's streaming structured-data
transducer, including event sources, routing, captures, and protocol checks.

```sh
npm install @tabnas/transduce
```

`@tabnas/parser`, the engine, is a required peer. `ParserSource` and
`ParserEventSource` take a parser made with the engine or with a grammar
package, and their declarations type it as the engine's `Tabnas`, as the
Go and Rust ports name the engine's types in theirs.
`ParserEventSource.fromReader(parser, reader)` reads its source from the
engine's synchronous pull reader (`{ read(buf: Uint8Array): number }`, 0
at the end, as `fs.readSync(fd, buf)` gives), a window at a time where
the grammar declares `lex.window`. The grammars name the engine as a peer
too, so one copy of it serves them all, and TypeScript reads their
`Tabnas` and this package's as the same class.

The protocol types (`JsonEvent`, `Sink`, `TableEvent`, `Selector`, `Datum`,
`Fail` and its codes, `Limits`, `Metrics` and the captures' types) are
`@tabnas/alchemy`'s shared types, imported from `@tabnas/alchemy/shared`, a
peer, and re-exported here under the same names. `routers` is this
package's stages as alchemy's `Routers`, which a host passes to alchemy's
`compile` with `@tabnas/render`'s `renderers`.

See the [project README](https://github.com/tabnas/transduce#readme) for the
architecture, API examples, limits, and protocol documentation.

This package includes its TypeScript sources under `src/` alongside the
compiled JavaScript and declarations under `dist/`.
