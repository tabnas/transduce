//! Retention does not grow with the number of rows.
//!
//! The table transducer holds one row at a time and the router one
//! capture at a time, so the bytes retained at their peak depend on the
//! largest row, never on how many rows there are. Ten times the rows of
//! the same size must leave `captured_bytes_high` (and so
//! `retained_bytes_high`) exactly where it was; this is acceptance
//! criterion 4 of the design brief, measured rather than asserted in
//! prose.

mod support;

use tabnas_transduce::{
    column_from_meta, Duplicates, Limits, Metrics, OwnedJsonEvent, ParserSource, Prune, Schema,
    Selector, SourceMode, Table, TableBinding, TableFromJson,
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

fn high_water(rows: usize) -> (u64, u64, usize) {
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
            rows: selector.clone(),
        },
        &Limits::default(),
        Duplicates::Reject,
        metrics.clone(),
        Table::default(),
    )
    .expect("the binding is valid");
    let (outcome, table) = ParserSource::new(tabnas_json::make(), &text)
        .grammar("json")
        .mode(SourceMode::Incremental {
            prune: Prune::Under(selector),
        })
        .metrics(metrics.clone())
        .run_owned(table);
    outcome.expect("the run succeeds");
    let table = table.into_inner();
    assert!(table.ended);
    (
        Metrics::get(&metrics.captured_bytes_high),
        Metrics::get(&metrics.retained_bytes_high),
        table.rows.len(),
    )
}

#[test]
fn ten_times_the_rows_leave_the_retained_high_water_flat() {
    let (captured_1, retained_1, rows_1) = high_water(200);
    println!("retention: 200 rows, captured high-water {captured_1} bytes");
    let (captured_10, retained_10, rows_10) = high_water(2000);
    println!("retention: 2000 rows, captured high-water {captured_10} bytes");
    assert_eq!((rows_1, rows_10), (200, 2000));
    assert!(captured_1 > 0);
    assert_eq!(
        captured_10, captured_1,
        "the peak is one row's, not the count's"
    );
    assert_eq!(retained_10, retained_1);
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
            rows: rows.clone(),
        },
        &Limits::default(),
        Duplicates::Reject,
        metrics.clone(),
        Table::default(),
    )
    .expect("the binding is valid");
    let (outcome, _) = ParserSource::new(tabnas_json::make(), text)
        .grammar("json")
        .mode(SourceMode::Incremental {
            prune: Prune::Under(rows),
        })
        .metrics(metrics.clone())
        .run_owned(table);
    outcome.expect("the run succeeds");
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
