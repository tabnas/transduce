//! Throughput of the sources and stages on generated inputs. Run with
//! `cargo bench`; each measurement prints a line so a long run is never
//! silent.

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use tabnas_transduce::{CountSink, Source, ValueSource};

/// The spec's worked example shape, `records` records long.
pub fn records_json(records: usize) -> String {
    let mut s = String::from(
        r#"{"response":{"metadata":{"fields":[{"title":"Identifier","path":["id"]},{"title":"Full name","path":["person","name"]},{"title":"Balance","path":["account","balance"]}]},"payload":{"deep":{"records":["#,
    );
    for i in 0..records {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!(
            r#"{{"id":{i},"person":{{"name":"Person number {i}"}},"account":{{"balance":{}.{:02}}}}}"#,
            i * 7,
            i % 100
        ));
    }
    s.push_str("]}}}}");
    s
}

fn walk_parsed_value(c: &mut Criterion) {
    let src = records_json(20_000);
    let value = tabnas_json::parse(&src).expect("the generated document parses");
    let mut group = c.benchmark_group("value_source");
    group.throughput(Throughput::Bytes(src.len() as u64));
    group.bench_function("walk", |b| {
        b.iter(|| {
            let mut count = CountSink::default();
            ValueSource(&value).run(&mut count).unwrap();
            count.events
        })
    });
    group.finish();
}

criterion_group!(benches, walk_parsed_value);
criterion_main!(benches);
