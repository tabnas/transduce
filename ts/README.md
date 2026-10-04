# @tabnas/transduce

The TypeScript implementation of Tabnas's streaming structured-data
transducer, including event sources, routing, captures, and protocol checks.

```sh
npm install @tabnas/transduce
```

`LinesSource` parses JSON Lines with `@tabnas/json` and CSV with
`@tabnas/csv`. Both are optional peers, loaded when a line source starts,
so install the ones your line sources read:

```sh
npm install @tabnas/transduce @tabnas/json @tabnas/csv
```

A line source whose grammar is not installed fails with a `TypeError` that
names the package.

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
