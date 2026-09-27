//! Throughput of the sources and stages on the generated worked-example
//! document. Run with `cargo bench`; each group prints a line before it
//! starts so a long run is never silent.
//!
//! The groups separate the costs: the engine alone (`parse_only`), the
//! rule-event adapter on top of it (`incremental`, with and without
//! pruning), the walk over an already parsed value (`walk`), the router
//! and table transducer fed from a recording (`table_from_recording`: the
//! stages without the parse), and the whole chain from text to table rows
//! (`table_from_text`). Throughput is bytes of source per second, so the
//! groups compare directly with BENCH.md.

#[path = "../tests/support/mod.rs"]
mod support;

use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use tabnas_transduce::{
    column_from_meta, replay, Cell, CountSink, Duplicates, Fail, Flow, Limits, Metrics,
    OwnedJsonEvent, ParserSource, Prune, Schema, Selector, Source, SourceMode, TableBinding,
    TableEvent, TableFromJson, TableSink, ValueSource,
};

/// 20k records in a release measurement; under `cargo test` (a debug
/// build running each bench once) a tenth of that keeps the gate quick.
const RECORDS: usize = if cfg!(debug_assertions) {
    2_000
} else {
    20_000
};

/// Counts table events and drops them: the cheapest table consumer.
#[derive(Default)]
struct CountTable {
    rows: u64,
    cells: u64,
}

impl TableSink for CountTable {
    fn table_event(&mut self, ev: TableEvent<'_>) -> Result<Flow, Fail> {
        if let TableEvent::Row(cells) = ev {
            self.rows += 1;
            self.cells += cells.iter().filter(|c| !matches!(c, Cell::Missing)).count() as u64;
        }
        Ok(Flow::Continue)
    }
}

fn binding() -> TableBinding {
    TableBinding {
        schema: Schema::FromMetadata {
            columns: Selector::root()
                .property("response")
                .property("metadata")
                .property("fields"),
            column: Box::new(column_from_meta),
        },
        rows: records_selector(),
    }
}

fn records_selector() -> Selector {
    Selector::root()
        .property("response")
        .property("payload")
        .property("deep")
        .property("records")
        .each_index()
}

fn table(sink: CountTable) -> TableFromJson<CountTable> {
    TableFromJson::new(
        binding(),
        &Limits::default(),
        Duplicates::LastWins,
        Metrics::new(),
        sink,
    )
    .expect("the worked-example binding is valid")
}

fn progress(group: &str, src: &str) {
    eprintln!(
        "bench: {group} on {RECORDS} records ({:.1} MB)",
        src.len() as f64 / 1e6
    );
}

fn parse_only(c: &mut Criterion) {
    let src = support::records_json(RECORDS);
    progress("parse_only", &src);
    let parser = tabnas_json::make();
    let mut group = c.benchmark_group("parse_only");
    group.sample_size(10);
    group.throughput(Throughput::Bytes(src.len() as u64));
    group.bench_function("json", |b| {
        b.iter(|| parser.parse(&src).expect("the generated document parses"))
    });
    group.finish();
}

fn incremental(c: &mut Criterion) {
    let src = support::records_json(RECORDS);
    progress("incremental", &src);
    let parser = tabnas_json::make();
    let mut group = c.benchmark_group("incremental");
    group.sample_size(10);
    group.throughput(Throughput::Bytes(src.len() as u64));
    group.bench_function("events_into_count_sink", |b| {
        b.iter(|| {
            let (r, count) = ParserSource::new(parser.clone(), &src)
                .mode(SourceMode::Incremental {
                    prune: Prune::Never,
                })
                .run_owned(CountSink::default());
            r.expect("the run succeeds");
            count.events
        })
    });
    group.bench_function("events_pruned_into_count_sink", |b| {
        b.iter(|| {
            let (r, count) = ParserSource::new(parser.clone(), &src)
                .mode(SourceMode::Incremental {
                    prune: Prune::Under(records_selector()),
                })
                .run_owned(CountSink::default());
            r.expect("the run succeeds");
            count.events
        })
    });
    group.finish();
}

fn walk_parsed_value(c: &mut Criterion) {
    let src = support::records_json(RECORDS);
    progress("walk", &src);
    let value = tabnas_json::parse(&src).expect("the generated document parses");
    let mut group = c.benchmark_group("walk");
    group.throughput(Throughput::Bytes(src.len() as u64));
    group.bench_function("value_source_into_count_sink", |b| {
        b.iter(|| {
            let mut count = CountSink::default();
            ValueSource(&value)
                .run(&mut count)
                .expect("the walk succeeds");
            count.events
        })
    });
    group.finish();
}

fn table_from_recording(c: &mut Criterion) {
    let src = support::records_json(RECORDS);
    progress("table_from_recording", &src);
    let value = tabnas_json::parse(&src).expect("the generated document parses");
    let mut events: Vec<OwnedJsonEvent> = Vec::new();
    ValueSource(&value)
        .run(&mut events)
        .expect("the recording succeeds");
    let events = Arc::new(events);
    let mut group = c.benchmark_group("table_from_recording");
    group.throughput(Throughput::Bytes(src.len() as u64));
    group.bench_function("router_and_table_into_count_table", |b| {
        b.iter(|| {
            let mut t = table(CountTable::default());
            replay(&events, &mut t).expect("the table run succeeds");
            t.into_inner().rows
        })
    });
    group.finish();
}

fn table_from_text(c: &mut Criterion) {
    let src = support::records_json(RECORDS);
    progress("table_from_text", &src);
    let parser = tabnas_json::make();
    let mut group = c.benchmark_group("table_from_text");
    group.sample_size(10);
    group.throughput(Throughput::Bytes(src.len() as u64));
    group.bench_function("incremental_pruned_into_table", |b| {
        b.iter(|| {
            let (r, t) = ParserSource::new(parser.clone(), &src)
                .mode(SourceMode::Incremental {
                    prune: Prune::Under(records_selector()),
                })
                .run_owned(table(CountTable::default()));
            r.expect("the run succeeds");
            t.into_inner().rows
        })
    });
    group.finish();
}

criterion_group!(
    benches,
    parse_only,
    incremental,
    walk_parsed_value,
    table_from_recording,
    table_from_text
);
criterion_main!(benches);
