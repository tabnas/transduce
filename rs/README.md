# tabnas-transduce (Rust)

The `tabnas-transduce` crate, library `tabnas_transduce`. See the
repository [README](../README.md) and [AGENTS.md](../AGENTS.md), and
[`docs/architecture.md`](../docs/architecture.md) for the design.

The protocol types (events, sinks, tables, failures, limits, selectors,
datums) are tabnas-alchemy's shared types, `tabnas_alchemy::shared`,
re-exported here at the paths they have always had; `routers()` answers
this crate's implementation of alchemy's `Routers`, which a host passes
to `tabnas_alchemy::compile`.

```rust
use tabnas_transduce::{CountSink, Source, ValueSource};

let value = tabnas_json::parse(r#"{"a":[1,2,3]}"#)?;
let mut count = CountSink::default();
ValueSource(&value).run(&mut count)?;
assert_eq!(count.events, 9); // { key [ 1 2 3 ] } end
# Ok::<(), Box<dyn std::error::Error>>(())
```

The whole chain, from a text to table rows: the parse, its value walked
into the source's guard, the table transducer and a table:

```rust
use tabnas_transduce::{
    column_from_meta, AbortFlag, Duplicates, Guarded, Limits, Metrics, ParserSource, Schema,
    Selector, Table, TableBinding, TableFromJson,
};

let text = r#"{"meta":[{"title":"Id","path":["id"]}],"rows":[{"id":1},{"id":2}]}"#;
let binding = TableBinding {
    schema: Schema::FromMetadata {
        columns: Selector::root().property("meta"),
        column: Box::new(column_from_meta),
    },
    rows: Selector::root().property("rows").each_index(),
};
let limits = Limits::default();
let table = TableFromJson::new(binding, &limits, Duplicates::Reject, Metrics::new(), Table::default())?;
let guarded = Guarded::new(table, &limits, AbortFlag::new(), Metrics::new());
let (outcome, guarded) = ParserSource::new(tabnas_json::make(), text).run_owned(guarded);
outcome?;
let table = guarded.into_inner().into_inner();
assert_eq!(table.columns.len(), 1);
assert_eq!(table.rows.len(), 2);
assert!(table.ended);
# Ok::<(), Box<dyn std::error::Error>>(())
```

A format with a transducer part streams instead: the part is alchemy's
text, which alchemy's runtime runs over the parser's rule passes
(`ParserEventSource`, pruned), into the same stages; dmog composes that
chain. A format with no part is parsed whole and its value
walked (`ParserSource`), as above.

alchemy's shared types, the engine and the grammars are sibling
checkouts named by path in `Cargo.toml`. From this directory:
`cargo test --all-targets`, `cargo test --doc`,
`cargo clippy --all-targets --all-features -- -D warnings`.
