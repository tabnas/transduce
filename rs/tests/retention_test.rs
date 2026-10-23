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
//! (`ParserEventSource::run_owned_with_value`) must hold no row for 200
//! rows and none for 2000, and the same bytes for both, while the same
//! run without pruning holds every row. A pruning that stopped emptying
//! the tree would leave the metric flat and fail here.
//!
//! The rows reach the table as a host reads a JSON document: JSON's
//! transducer part, natively (`TransduceTree`), over the passes
//! `ParserEventSource` delivers, the engine's arrays emptied behind the
//! stream. Every other grammar's pruned tree is held flat by
//! `tests/prune_test.rs`, and the router's peak does not depend on the
//! format the rows came from.

mod support;

use tabnas_transduce::{
    column_from_meta, AbortFlag, Datum, Duplicates, Guarded, Limits, Metrics, OwnedJsonEvent,
    ParserEventSource, ParserSource, Schema, Segment, Selector, Table, TableBinding, TableFromJson,
    TransduceTree,
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

/// The table over the document's events, from JSON's part over the
/// passes, pruned or not; `metrics` counts the events once, at the guard
/// in front of the table, as a source's guard counts them.
fn table_run(
    text: &str,
    table: TableFromJson<Table>,
    prune: bool,
    metrics: std::sync::Arc<Metrics>,
) -> (Table, Option<tabnas::Value>) {
    let guarded = Guarded::new(table, &Limits::default(), AbortFlag::new(), metrics);
    let source = ParserEventSource::new(tabnas_json::make(), text).grammar("json");
    let source = if prune { source.prune() } else { source };
    let (outcome, tree, value) =
        source.run_owned_with_value(TransduceTree::new(support::json_part(), guarded));
    outcome.expect("the run succeeds");
    (tree.into_inner().into_inner().into_inner(), value)
}

fn run(rows: usize, prune: bool) -> Left {
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
    let (table, value) = table_run(&text, table, prune, metrics.clone());
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
    let one = run(200, true);
    println!(
        "retention: 200 rows, captured high-water {} bytes, tree {} bytes",
        one.captured_high, one.tree_bytes
    );
    let ten = run(2000, true);
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
    let one = run(200, false);
    let ten = run(2000, false);
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
            rows,
        },
        &Limits::default(),
        Duplicates::Reject,
        metrics.clone(),
        Table::default(),
    )
    .expect("the binding is valid");
    table_run(text, table, true, metrics.clone());
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
