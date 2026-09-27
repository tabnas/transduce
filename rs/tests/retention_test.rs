//! Retention does not grow with the number of rows.
//!
//! The table transducer holds one row at a time and the router one
//! capture at a time, so the bytes retained at their peak depend on the
//! largest row, never on how many rows there are. Ten times the rows of
//! the same size must leave `captured_bytes_high` (and so
//! `retained_bytes_high`) exactly where it was; this is acceptance
//! criterion 4 of the design brief, measured rather than asserted in
//! prose. That metric is the router's own accounting, so the test also
//! looks where pruning acts: the engine's tree after the run
//! (`ParserSource::run_owned_with_value`) must hold no row for 200 rows
//! and none for 2000, and the same bytes for both, while the same run
//! without pruning holds every row. A pruning that stopped truncating
//! would leave the metric flat and fail here.

mod support;

use tabnas_transduce::{
    column_from_meta, Datum, Duplicates, Limits, Metrics, OwnedJsonEvent, ParserSource, Prune,
    Schema, Segment, Selector, SourceMode, Table, TableBinding, TableFromJson,
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
    /// Rows still in the engine's tree after the run.
    tree_rows: usize,
    /// The whole tree's size, on the limits' measure.
    tree_bytes: usize,
}

fn records() -> Vec<Segment> {
    vec![
        Segment::key("response"),
        Segment::key("payload"),
        Segment::key("deep"),
        Segment::key("records"),
    ]
}

fn run(rows: usize, prune: Prune) -> Left {
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
    let (outcome, table, value) = ParserSource::new(tabnas_json::make(), &text)
        .grammar("json")
        .mode(SourceMode::Incremental { prune })
        .metrics(metrics.clone())
        .run_owned_with_value(table);
    outcome.expect("the run succeeds");
    let table = table.into_inner();
    assert!(table.ended);
    let tree = Datum::from_tabnas(&value.expect("the parse returned"));
    let tree_rows = tree
        .get_path(&records())
        .and_then(Datum::as_array)
        .expect("the records array is in the tree")
        .len();
    Left {
        captured_high: Metrics::get(&metrics.captured_bytes_high),
        retained_high: Metrics::get(&metrics.retained_bytes_high),
        rows: table.rows.len(),
        tree_rows,
        tree_bytes: tree.byte_size(),
    }
}

#[test]
fn ten_times_the_rows_leave_the_retained_high_water_flat() {
    let prune = || {
        Prune::Under(
            Selector::root()
                .property("response")
                .property("payload")
                .property("deep")
                .property("records")
                .each_index(),
        )
    };
    let one = run(200, prune());
    println!(
        "retention: 200 rows, captured high-water {} bytes, tree {} bytes",
        one.captured_high, one.tree_bytes
    );
    let ten = run(2000, prune());
    println!(
        "retention: 2000 rows, captured high-water {} bytes, tree {} bytes",
        ten.captured_high, ten.tree_bytes
    );
    assert_eq!((one.rows, ten.rows), (200, 2000));
    assert!(one.captured_high > 0);
    assert_eq!(
        ten.captured_high, one.captured_high,
        "the peak is one row's, not the count's"
    );
    assert_eq!(ten.retained_high, one.retained_high);
    assert_eq!(
        (one.tree_rows, ten.tree_rows),
        (0, 0),
        "every streamed row was dropped from the engine's tree"
    );
    assert_eq!(
        ten.tree_bytes, one.tree_bytes,
        "the tree left behind does not grow with the rows"
    );
}

/// The control: the same runs without pruning keep every row in the tree,
/// so the assertion above can fail if pruning stops.
#[test]
fn without_pruning_the_engines_tree_holds_every_row() {
    let one = run(200, Prune::Never);
    let ten = run(2000, Prune::Never);
    assert_eq!((one.tree_rows, ten.tree_rows), (200, 2000));
    assert!(ten.tree_bytes > 9 * one.tree_bytes);
    assert_eq!(
        ten.captured_high, one.captured_high,
        "the router's peak is one row with or without pruning"
    );
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
