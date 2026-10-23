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

The whole chain, from a text to table rows: the parser's rule passes
through JSON's transducer part, natively, with the rows streamed from the
parse and the engine's arrays emptied behind them as they go:

```rust
use tabnas_transduce::{
    column_from_meta, AbortFlag, Duplicates, Guarded, Limits, Metrics, ParserEventSource, Schema,
    Selector, Table, TableBinding, TableFromJson, TransduceTree, TreeContainer, TreeKey, TreeRule,
    TreeScalar, TreeSpec,
};

// JSON's part, as tabnas-json spells it: what each rule's passes make.
let rule = |container, key, scalar| TreeRule { container, key, scalar, ..TreeRule::default() };
let json = TreeSpec::new()
    .with("map", rule(Some(TreeContainer::Object), None, None))
    .with("list", rule(Some(TreeContainer::Array), None, None))
    .with("pair", rule(None, Some(TreeKey::Token(0)), None))
    .with("val", rule(None, None, Some(TreeScalar::Token(0))));

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
let (outcome, tree) = ParserEventSource::new(tabnas_json::make(), text)
    .grammar("json")
    .prune()
    .run_owned(TransduceTree::new(json, guarded));
outcome?;
let table = tree.into_inner().into_inner().into_inner();
assert_eq!(table.columns.len(), 1);
assert_eq!(table.rows.len(), 2);
assert!(table.ended);
# Ok::<(), Box<dyn std::error::Error>>(())
```

A format with no part is parsed whole and its value walked
(`ParserSource`), into the same chain.

alchemy's shared types, the engine and the grammars are sibling
checkouts named by path in `Cargo.toml`. From this directory:
`cargo test --all-targets`, `cargo test --doc`,
`cargo clippy --all-targets --all-features -- -D warnings`.
