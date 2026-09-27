# tabnas-transduce (Rust)

The `tabnas-transduce` crate, library `tabnas_transduce`. See the
repository [README](../README.md) and [AGENTS.md](../AGENTS.md), and
[`docs/architecture.md`](../docs/architecture.md) for the design.

```rust
use tabnas_transduce::{CountSink, Source, ValueSource};

let value = tabnas_json::parse(r#"{"a":[1,2,3]}"#)?;
let mut count = CountSink::default();
ValueSource(&value).run(&mut count)?;
assert_eq!(count.events, 9); // { key [ 1 2 3 ] } end
# Ok::<(), Box<dyn std::error::Error>>(())
```

The whole chain, from a text to table rows, with the rows streamed from
the parse and pruned from the engine's tree as they go:

```rust
use tabnas_transduce::{
    column_from_meta, Duplicates, Limits, Metrics, ParserSource, Prune, Schema, Selector,
    SourceMode, Table, TableBinding, TableFromJson,
};

let text = r#"{"meta":[{"title":"Id","path":["id"]}],"rows":[{"id":1},{"id":2}]}"#;
let rows = Selector::root().property("rows").each_index();
let binding = TableBinding {
    schema: Schema::FromMetadata {
        columns: Selector::root().property("meta"),
        column: Box::new(column_from_meta),
    },
    rows: rows.clone(),
};
let table = TableFromJson::new(binding, &Limits::default(), Duplicates::Reject, Metrics::new(), Table::default())?;
let (outcome, table) = ParserSource::new(tabnas_json::make(), text)
    .mode(SourceMode::Incremental { prune: Prune::Under(rows) })
    .run_owned(table);
outcome?;
let table = table.into_inner();
assert_eq!(table.columns.len(), 1);
assert_eq!(table.rows.len(), 2);
assert!(table.ended);
# Ok::<(), Box<dyn std::error::Error>>(())
```

The engine and the grammars are sibling checkouts named by path in
`Cargo.toml`. From this directory: `cargo test --all-targets`,
`cargo test --doc`, `cargo clippy --all-targets --all-features -- -D warnings`.
