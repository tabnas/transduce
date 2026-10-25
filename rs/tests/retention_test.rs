//! Retention does not grow with the number of rows.
//!
//! The table transducer holds one row at a time and the router one
//! capture at a time, so the bytes retained at their peak depend on the
//! largest row, never on how many rows there are. Ten times the rows of
//! the same size must leave `captured_bytes_high` (and so
//! `retained_bytes_high`) exactly where it was; this is acceptance
//! criterion 4 of the design brief, measured rather than asserted in
//! prose.
//!
//! The rows reach the table from the parse of a JSON document, its value
//! walked (`ParserSource`), through the source's guard, which counts the
//! events as a source's guard counts them. The transducer's peak does not
//! depend on how the events were made: a host that streams a format
//! through its transducer part (alchemy's text over `ParserEventSource`'s
//! passes, pruned) hands the same events to the same stages. What pruning
//! leaves of the engine's tree is `tests/prune_test.rs`'s to hold flat,
//! for every grammar; dmog, which runs the parts, holds a part's
//! run to it.

mod support;

use tabnas_transduce::{
    column_from_meta, AbortFlag, Duplicates, Guarded, Limits, Metrics, OwnedJsonEvent,
    ParserSource, Schema, Selector, Table, TableBinding, TableFromJson,
};

/// The worked example with `rows` copies of one record, so every row has
/// the same size and the peak is the row's, not the count's.
fn same_rows(rows: usize) -> String {
    let record = support::record(123_456);
    let mut s = format!(
        r#"{{"response":{{"metadata":{},"payload":{{"deep":{{"records":["#,
        support::METADATA
    );
    for i in 0..rows {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&record);
    }
    s.push_str("]}}}}");
    s
}

/// What one run left behind.
struct Left {
    captured_high: u64,
    retained_high: u64,
    rows: usize,
}

/// The table over the document's events, the parsed value walked;
/// `metrics` counts the events once, at the guard in front of the table,
/// as a source's guard counts them.
fn table_run(text: &str, table: TableFromJson<Table>, metrics: std::sync::Arc<Metrics>) -> Table {
    let guarded = Guarded::new(table, &Limits::default(), AbortFlag::new(), metrics);
    let (outcome, guarded) = ParserSource::new(tabnas_json::make(), text).run_owned(guarded);
    outcome.expect("the run succeeds");
    guarded.into_inner().into_inner()
}

fn run(rows: usize) -> Left {
    let text = same_rows(rows);
    let metrics = Metrics::new();
    let selector = Selector::root()
        .property("response")
        .property("payload")
        .property("deep")
        .property("records")
        .each_index();
    let table = TableFromJson::new(
        TableBinding {
            schema: Schema::FromMetadata {
                columns: Selector::root()
                    .property("response")
                    .property("metadata")
                    .property("fields"),
                column: Box::new(column_from_meta),
            },
            rows: selector,
        },
        &Limits::default(),
        Duplicates::Reject,
        metrics.clone(),
        Table::default(),
    )
    .expect("the binding is valid");
    let table = table_run(&text, table, metrics.clone());
    assert!(table.ended);
    Left {
        captured_high: Metrics::get(&metrics.captured_bytes_high),
        retained_high: Metrics::get(&metrics.retained_bytes_high),
        rows: table.rows.len(),
    }
}

#[test]
fn ten_times_the_rows_leave_the_retained_high_water_flat() {
    let one = run(200);
    println!(
        "retention: 200 rows, captured high-water {} bytes",
        one.captured_high
    );
    let ten = run(2000);
    println!(
        "retention: 2000 rows, captured high-water {} bytes",
        ten.captured_high
    );
    assert_eq!((one.rows, ten.rows), (200, 2000));
    assert!(one.captured_high > 0);
    assert_eq!(
        ten.captured_high, one.captured_high,
        "the peak is one row's, not the count's"
    );
    assert_eq!(ten.retained_high, one.retained_high);
}

/// The README's chain shares one `Metrics` between the source and the
/// table transducer; the source counts are the source's, once.
#[test]
fn a_chain_sharing_one_metrics_counts_the_source_events_once() {
    let text = r#"{"meta":[{"title":"Id","path":["id"]}],"rows":[{"id":1},{"id":2}]}"#;
    let rows = Selector::root().property("rows").each_index();
    let metrics = Metrics::new();
    let table = TableFromJson::new(
        TableBinding {
            schema: Schema::FromMetadata {
                columns: Selector::root().property("meta"),
                column: Box::new(column_from_meta),
            },
            rows,
        },
        &Limits::default(),
        Duplicates::Reject,
        metrics.clone(),
        Table::default(),
    )
    .expect("the binding is valid");
    table_run(text, table, metrics.clone());
    let (_, recorded) =
        ParserSource::new(tabnas_json::make(), text).run_owned(Vec::<OwnedJsonEvent>::new());
    assert_eq!(Metrics::get(&metrics.events), recorded.len() as u64);
    assert_eq!(
        Metrics::get(&metrics.keys),
        recorded
            .iter()
            .filter(|e| matches!(e, OwnedJsonEvent::Key(_)))
            .count() as u64
    );
    assert_eq!(
        Metrics::get(&metrics.scalars),
        recorded.iter().filter(|e| e.as_event().is_scalar()).count() as u64
    );
    assert_eq!(Metrics::get(&metrics.rows), 2);
}
