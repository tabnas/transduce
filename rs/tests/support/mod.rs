//! Generated inputs in the spec's worked-example shape, shared by the
//! integration tests and (by `#[path]` include) the benches, so every
//! measurement and every differential run is over the same documents.
//!
//! A record is `{"id":i,"person":{"name":"Person number i"},"account":
//! {"balance":D.CC}}`, the JSON document wraps them under
//! `response.payload.deep.records` behind `response.metadata.fields`, the
//! JSON Lines variant is one record per line, the CSV variant is the
//! flattened columns `id,name,balance`, and the YAML variant is the block
//! form of the JSON document (a sequence of block mappings, the shape the
//! prototype's large YAML input had).

#![allow(dead_code)]

use std::fmt::Write as _;

/// The metadata the worked example carries: three columns by path.
pub const METADATA: &str = r#"{"fields":[{"title":"Identifier","path":["id"]},{"title":"Full name","path":["person","name"]},{"title":"Balance","path":["account","balance"]}]}"#;

/// One record, as compact JSON.
pub fn record(i: usize) -> String {
    format!(
        r#"{{"id":{i},"person":{{"name":"Person number {i}"}},"account":{{"balance":{}.{:02}}}}}"#,
        i * 7,
        i % 100
    )
}

/// The worked-example document with `records` records.
pub fn records_json(records: usize) -> String {
    let mut s = format!(r#"{{"response":{{"metadata":{METADATA},"payload":{{"deep":{{"records":["#);
    for i in 0..records {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&record(i));
    }
    s.push_str("]}}}}");
    s
}

/// The records alone, one JSON document per line.
pub fn records_jsonl(records: usize) -> String {
    let mut s = String::new();
    for i in 0..records {
        s.push_str(&record(i));
        s.push('\n');
    }
    s
}

/// The records flattened to `id,name,balance`, with a header line.
pub fn records_csv(records: usize) -> String {
    let mut s = String::from("id,name,balance\n");
    for i in 0..records {
        let _ = writeln!(s, "{i},Person number {i},{}.{:02}", i * 7, i % 100);
    }
    s
}

/// The worked-example document as block YAML.
pub fn records_yaml(records: usize) -> String {
    let mut s = String::from(
        "response:\n  metadata:\n    fields:\n      - title: Identifier\n        path: [id]\n      - title: Full name\n        path: [person, name]\n      - title: Balance\n        path: [account, balance]\n  payload:\n    deep:\n      records:\n",
    );
    for i in 0..records {
        let _ = writeln!(
            s,
            "        - id: {i}\n          person:\n            name: Person number {i}\n          account:\n            balance: {}.{:02}",
            i * 7,
            i % 100
        );
    }
    s
}
