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

The engine and the grammars are sibling checkouts named by path in
`Cargo.toml`. From this directory: `cargo test --all-targets`,
`cargo test --doc`, `cargo clippy --all-targets --all-features -- -D warnings`.
