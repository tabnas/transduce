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

/// The worked example as XML with `rows` copies of one record and nothing
/// between them, so every row is one `<record>` element of the same size,
/// and the records element's children are the rows alone.
fn same_rows_xml(rows: usize) -> String {
    let record = "<record id=\"123456\"><person><name>Person number 123456</name></person>\
                  <account><balance>864192.56</balance></account></record>";
    format!(
        "<?xml version=\"1.0\"?>\n<response><records>{}</records></response>\n",
        record.repeat(rows)
    )
}

/// The records element's children: the root's first child's `children`.
fn xml_rows() -> Selector {
    Selector::root()
        .property("children")
        .index(0)
        .property("children")
        .each_index()
}

/// One run over the XML worked example, a table inferred from the rows.
fn run_xml(rows: usize, prune: Prune) -> Left {
    let text = same_rows_xml(rows);
    let metrics = Metrics::new();
    let table = TableFromJson::new(
        TableBinding {
            schema: Schema::Infer,
            rows: xml_rows(),
        },
        &Limits::default(),
        Duplicates::Reject,
        metrics.clone(),
        Table::default(),
    )
    .expect("the binding is valid");
    let (outcome, table, value) = ParserSource::new(tabnas_xml::make(), &text)
        .grammar("xml")
        .mode(SourceMode::Incremental { prune })
        .metrics(metrics.clone())
        .run_owned_with_value(table);
    outcome.expect("the run succeeds");
    let table = table.into_inner();
    assert!(table.ended);
    let tree = Datum::from_tabnas(&value.expect("the parse returned"));
    let tree_rows = tree
        .get_path(&[
            Segment::key("children"),
            Segment::Index(0),
            Segment::key("children"),
        ])
        .and_then(Datum::as_array)
        .expect("the records element's children are in the tree")
        .len();
    Left {
        captured_high: Metrics::get(&metrics.captured_bytes_high),
        retained_high: Metrics::get(&metrics.retained_bytes_high),
        rows: table.rows.len(),
        tree_rows,
        tree_bytes: tree.byte_size(),
    }
}

/// xml streams too: its rules build each element in document order, so
/// the rows of an XML document reach the table one at a time, and with
/// the records element's children pruned the engine's tree holds none of
/// them after the run. Ten times the rows leave the high-water marks and
/// the tree where they were.
#[test]
fn ten_times_the_rows_of_an_xml_document_leave_the_retained_high_water_flat() {
    let one = run_xml(200, Prune::Under(xml_rows()));
    println!(
        "retention xml: 200 rows, captured high-water {} bytes, tree {} bytes",
        one.captured_high, one.tree_bytes
    );
    let ten = run_xml(2000, Prune::Under(xml_rows()));
    println!(
        "retention xml: 2000 rows, captured high-water {} bytes, tree {} bytes",
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
    // The control: without pruning the tree keeps every row.
    let kept = run_xml(2000, Prune::Never);
    assert_eq!(kept.tree_rows, 2000);
    assert_eq!(kept.captured_high, one.captured_high);
}

/// A PGN database of `games` copies of one game, so every game is the same
/// size and the database's elements are the games alone.
fn same_games_pgn(games: usize) -> String {
    "[Event \"Records\"]\n[Result \"1-0\"]\n\n\
     1. e4 e5 2. Nf3 {Person number 123456} Nc6 $1 (2... d6 3. d4) 3. Bb5 1-0\n\n"
        .repeat(games)
}

/// One run over a PGN database, a table inferred from its games.
fn run_pgn(games: usize, prune: Prune) -> Left {
    let text = same_games_pgn(games);
    let metrics = Metrics::new();
    let table = TableFromJson::new(
        TableBinding {
            schema: Schema::Infer,
            rows: Selector::root().each_index(),
        },
        &Limits::default(),
        Duplicates::Reject,
        metrics.clone(),
        Table::default(),
    )
    .expect("the binding is valid");
    let pgn = tabnas_chess::make(&tabnas_chess::ChessOptions::default())
        .expect("the PGN grammar installs");
    let (outcome, table, value) = ParserSource::new(pgn, &text)
        .grammar("pgn")
        .mode(SourceMode::Incremental { prune })
        .metrics(metrics.clone())
        .run_owned_with_value(table);
    outcome.expect("the run succeeds");
    let table = table.into_inner();
    assert!(table.ended);
    let tree = Datum::from_tabnas(&value.expect("the parse returned"));
    let tree_rows = tree.as_array().expect("the database is in the tree").len();
    Left {
        captured_high: Metrics::get(&metrics.captured_bytes_high),
        retained_high: Metrics::get(&metrics.retained_bytes_high),
        rows: table.rows.len(),
        tree_rows,
        tree_bytes: tree.byte_size(),
    }
}

/// pgn streams a game at a time: tabnas-chess stores a game in the database
/// only once the game is complete, so each game reaches the table whole,
/// and with the database's games pruned the engine's tree holds none of
/// them after the run. Ten times the games leave the high-water marks and
/// the tree where they were.
#[test]
fn ten_times_the_games_of_a_pgn_database_leave_the_retained_high_water_flat() {
    let games = || Prune::Under(Selector::root().each_index());
    let one = run_pgn(200, games());
    println!(
        "retention pgn: 200 games, captured high-water {} bytes, tree {} bytes",
        one.captured_high, one.tree_bytes
    );
    let ten = run_pgn(2000, games());
    println!(
        "retention pgn: 2000 games, captured high-water {} bytes, tree {} bytes",
        ten.captured_high, ten.tree_bytes
    );
    assert_eq!((one.rows, ten.rows), (200, 2000));
    assert!(one.captured_high > 0);
    assert_eq!(
        ten.captured_high, one.captured_high,
        "the peak is one game's, not the count's"
    );
    assert_eq!(ten.retained_high, one.retained_high);
    assert_eq!(
        (one.tree_rows, ten.tree_rows),
        (0, 0),
        "every streamed game was dropped from the engine's tree"
    );
    assert_eq!(
        ten.tree_bytes, one.tree_bytes,
        "the tree left behind does not grow with the games"
    );
    // The control: without pruning the tree keeps every game.
    let kept = run_pgn(2000, Prune::Never);
    assert_eq!(kept.tree_rows, 2000);
    assert_eq!(kept.captured_high, one.captured_high);
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
