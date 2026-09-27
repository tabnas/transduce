# Reference

The types are documented in the crate; `cargo doc --open` from `rs/` is
the reference. This page lists the protocols and the contract each keeps.

## JsonEvents/1

`JsonEvent`: `ObjectStart`, `ObjectEnd`, `ArrayStart`, `ArrayEnd`,
`Key(&str)`, `Null`, `Bool`, `Number { value, lexeme }`, `String(&str)`,
`End`. One document: a root value, then exactly one `End`, issued only
after the whole source validated. Keys and scalars are whole. Events
borrow from the source for one `Sink::event` call.

## TableRows/1

`TableEvent`: `Schema(&[PublicColumn])` once and first, `Row(&[Cell])`
as many as there are rows and each exactly as wide as the schema, `End`
once and last. Cells are `Null`, `Bool`, `Number { value, lexeme }`,
`String`, `Missing`. A renderer sees labels and cells, never source paths.

## Selectors

`Selector` steps: `Property(name)`, `Index(n)`, `EachIndex`, `EachMember`.
`Selector::from_segments` builds one from data. Display is jq syntax:
`.response.records[*]`, `."odd key"`, `[3]`, `[]` for every member.

## Failure codes

See `Code::ALL` in `rs/src/error.rs` and the table in `AGENTS.md`. A
`Fail` carries `code`, `message`, and when they apply `path`, `limit`
(`{name, value}`), `row`, `col`, and `output` (`"partial"` or `"none"`).

## Limits

`Limits` fields, each named in a failure as written: `max_depth`,
`max_key_bytes`, `max_scalar_bytes`, `max_metadata_bytes`, `max_columns`,
`max_record_bytes`, `max_capture_bytes`, `max_output_bytes`. Sizes are
payload bytes plus `NODE_BYTES` per node.
