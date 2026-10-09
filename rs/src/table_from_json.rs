//! The metadata-first table transducer: `JsonEvents/1` in, `TableRows/1`
//! out.
//!
//! [`TableFromJson`] is a [`Sink`] built on a [`Router`] with at most two
//! captures: the column metadata, when the schema comes from the
//! document, and the rows. The contract the spec sets and the reason for
//! the shape: the schema must be known before the first row is emitted,
//! and the transducer holds one row at a time. So metadata that has not
//! completed when a row BEGINS is an `INPUT_ORDER_VIOLATION` raised at the
//! row's start, through the router's begin hook, before a byte of the row
//! is retained; metadata that arrives twice is the same failure; and a
//! document with no rows is a valid empty table, its schema emitted just
//! before `End`. Rows are projected into schema order by path, so the
//! order of members inside a row never matters, and a number keeps the
//! lexeme the source events carried. An inferred schema is the first row's,
//! by its kind: an object's member names, an array's positions, or the one
//! column `value` of a scalar; a later row of another kind projects through
//! those paths and lands empty where they miss.

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use crate::datum::{Datum, Duplicates};
use crate::error::{Code, Fail};
use crate::event::JsonEvent;
use crate::limits::{Limits, Metrics, NODE_BYTES};
use crate::matcher::CaptureId;
use crate::route::{CaptureSpec, RouteSink, Router, Selected};
use crate::selector::{Path, Segment, Selector};
use crate::sink::{Flow, Sink};
use crate::table::{
    BoundColumn, Cell, ColumnMapper, MissingPolicy, PublicColumn, Schema, TableBinding, TableEvent,
    TableSink,
};

/// Where the columns are, once known.
enum Columns {
    /// Declared by the binding.
    Static(Vec<BoundColumn>),
    /// Selected from the document; `None` until the metadata completes.
    FromMetadata {
        selector: Selector,
        column: ColumnMapper,
        bound: Option<Vec<BoundColumn>>,
    },
    /// Taken from the first row, by its kind (`infer_columns`).
    Infer(Option<Vec<BoundColumn>>),
}

impl Columns {
    fn bound(&self) -> Option<&[BoundColumn]> {
        match self {
            Columns::Static(c) => Some(c),
            Columns::FromMetadata { bound, .. } => bound.as_deref(),
            Columns::Infer(c) => c.as_deref(),
        }
    }
}

/// The route sink behind the transducer: it owns the schema state, the
/// projection buffer and the table sink.
struct Core<S: TableSink> {
    sink: S,
    /// The capture ids the router was built with.
    row_id: CaptureId,
    meta_id: Option<CaptureId>,
    columns: Columns,
    public: Vec<PublicColumn>,
    schema_sent: bool,
    cells: Vec<Cell>,
    /// Whether no column's path is another's prefix, so each cell can be
    /// moved out of the row instead of copied.
    disjoint: bool,
    max_columns: usize,
    /// The bound on the inferred columns' names, the table's metadata
    /// when the first row supplies it.
    max_metadata_bytes: usize,
    metrics: Arc<Metrics>,
}

impl<S: TableSink> Core<S> {
    fn bind(&mut self, columns: Vec<BoundColumn>, from: &str) -> Result<(), Fail> {
        if columns.len() > self.max_columns {
            return Err(Fail::limit(
                "max_columns",
                self.max_columns as u64,
                format!(
                    "{from} declares {} columns, more than {}",
                    columns.len(),
                    self.max_columns
                ),
            ));
        }
        self.public = columns.iter().map(BoundColumn::public).collect();
        self.disjoint = paths_are_disjoint(&columns);
        match &mut self.columns {
            Columns::Static(c) => *c = columns,
            Columns::FromMetadata { bound, .. } => *bound = Some(columns),
            Columns::Infer(c) => *c = Some(columns),
        }
        Ok(())
    }

    fn send_schema(&mut self) -> Result<Flow, Fail> {
        self.schema_sent = true;
        self.sink.table_event(TableEvent::Schema(&self.public))
    }

    fn metadata(&mut self, selected: Selected) -> Result<(), Fail> {
        let (column, bound) = match &self.columns {
            Columns::FromMetadata { column, bound, .. } => (column, bound),
            // The router only has a metadata capture when the binding
            // selects one.
            _ => return Err(Fail::protocol("metadata was delivered to a static schema")),
        };
        if bound.is_some() {
            return Err(Fail::new(
                Code::InputOrderViolation,
                format!(
                    "the column metadata at {} was selected twice; a table has one schema",
                    selected.path
                ),
            )
            .at_path(selected.path.to_string()));
        }
        let value = selected.value.unwrap_or(Datum::Null);
        let descriptors = value.as_array().ok_or_else(|| {
            Fail::input(format!(
                "the column metadata at {} is not an array",
                selected.path
            ))
            .at_path(selected.path.to_string())
        })?;
        if descriptors.len() > self.max_columns {
            return Err(Fail::limit(
                "max_columns",
                self.max_columns as u64,
                format!(
                    "the metadata at {} declares {} columns, more than {}",
                    selected.path,
                    descriptors.len(),
                    self.max_columns
                ),
            )
            .at_path(selected.path.to_string()));
        }
        let mut columns = Vec::with_capacity(descriptors.len());
        for (i, d) in descriptors.iter().enumerate() {
            let col = column(d).map_err(|mut f| {
                if f.path.is_none() {
                    let mut p = selected.path.clone();
                    p.push(Segment::Index(i));
                    f.path = Some(p.to_string());
                }
                f
            })?;
            columns.push(col);
        }
        self.bind(columns, "the metadata")
    }

    fn row(&mut self, selected: Selected) -> Result<Flow, Fail> {
        let mut row = selected.value.unwrap_or(Datum::Null);
        if !self.schema_sent {
            if let Columns::Infer(None) = &self.columns {
                let columns = infer_columns(&row, self.max_columns, self.max_metadata_bytes)
                    .map_err(|f| f.at_path(selected.path.to_string()))?;
                self.bind(columns, "the first row")?;
            }
            if self.send_schema()? == Flow::Stop {
                return Ok(Flow::Stop);
            }
        }
        let columns = self
            .columns
            .bound()
            .ok_or_else(|| Fail::protocol("a row was projected before its schema was bound"))?;
        self.cells.clear();
        for col in columns {
            let found = if self.disjoint {
                row.take_path(&col.source).map(Cell::from_owned)
            } else {
                row.get_path(&col.source).map(Cell::from_datum)
            };
            let cell = match found {
                Some(cell) => cell,
                None => match col.missing {
                    MissingPolicy::Missing => Cell::Missing,
                    MissingPolicy::Null => Cell::Null,
                    MissingPolicy::Error => {
                        let at = cell_path(&selected.path, &col.source);
                        return Err(Fail::new(
                            Code::MissingValue,
                            format!("column {:?} has no value at {at}", col.label),
                        )
                        .at_path(at.to_string()));
                    }
                },
            };
            self.cells.push(cell);
        }
        Metrics::add(&self.metrics.rows, 1);
        self.sink.table_event(TableEvent::Row(&self.cells))
    }
}

impl<S: TableSink> RouteSink for Core<S> {
    fn began(&mut self, id: CaptureId, _tag: &str) -> Result<(), Fail> {
        if id == self.row_id {
            if let Columns::FromMetadata {
                selector,
                bound: None,
                ..
            } = &self.columns
            {
                return Err(Fail::new(
                    Code::InputOrderViolation,
                    format!(
                        "a row began before the column metadata at {selector} had completed; rows must follow their metadata"
                    ),
                ));
            }
        }
        Ok(())
    }

    fn selected(&mut self, selected: Selected) -> Result<Flow, Fail> {
        if Some(selected.id) == self.meta_id {
            self.metadata(selected).map(|()| Flow::Continue)
        } else {
            self.row(selected)
        }
    }

    fn end(&mut self) -> Result<Flow, Fail> {
        if !self.schema_sent {
            match &mut self.columns {
                Columns::FromMetadata {
                    selector,
                    bound: None,
                    ..
                } => {
                    return Err(Fail::input(format!(
                        "the document has no column metadata at {selector}"
                    ))
                    .at_path(selector.to_string()));
                }
                // No rows: nothing to infer from, so the table is empty.
                Columns::Infer(None) => self.bind(Vec::new(), "the binding")?,
                _ => {}
            }
            if self.send_schema()? == Flow::Stop {
                return Ok(Flow::Stop);
            }
        }
        self.sink.table_event(TableEvent::End)
    }
}

/// The table transducer. A [`Sink`] for one document's events; the table
/// events go to the wrapped [`TableSink`] as the rows arrive.
pub struct TableFromJson<S: TableSink> {
    router: Router<Core<S>>,
}

impl<S: TableSink> fmt::Debug for TableFromJson<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableFromJson")
            .field("router", &self.router)
            .field("schema_sent", &self.router.downstream().schema_sent)
            .finish()
    }
}

impl<S: TableSink> TableFromJson<S> {
    /// Build the transducer. Rows are materialized under
    /// `max_record_bytes`, metadata under `max_metadata_bytes`; a rows
    /// selector that may overlap the metadata selector is refused as the
    /// router refuses any overlapping materializations.
    pub fn new(
        binding: TableBinding,
        limits: &Limits,
        duplicates: Duplicates,
        metrics: Arc<Metrics>,
        sink: S,
    ) -> Result<TableFromJson<S>, Fail> {
        let TableBinding { schema, rows } = binding;
        let row_spec = CaptureSpec::materialize("row", rows)
            .budget(limits.max_record_bytes, "max_record_bytes");
        let (columns, specs, meta_id) = match schema {
            Schema::Static(c) => (Columns::Static(c), vec![row_spec], None),
            Schema::FromMetadata { columns, column } => {
                let meta_spec = CaptureSpec::materialize("metadata", columns.clone())
                    .budget(limits.max_metadata_bytes, "max_metadata_bytes");
                (
                    Columns::FromMetadata {
                        selector: columns,
                        column,
                        bound: None,
                    },
                    vec![meta_spec, row_spec],
                    Some(0),
                )
            }
            Schema::Infer => (Columns::Infer(None), vec![row_spec], None),
        };
        let mut core = Core {
            sink,
            row_id: specs.len() - 1,
            meta_id,
            columns,
            public: Vec::new(),
            schema_sent: false,
            cells: Vec::new(),
            disjoint: false,
            max_columns: limits.max_columns,
            max_metadata_bytes: limits.max_metadata_bytes,
            metrics: metrics.clone(),
        };
        if let Columns::Static(c) = &core.columns {
            let c = c.clone();
            core.bind(c, "the binding")?;
        }
        Ok(TableFromJson {
            router: Router::new(specs, limits, duplicates, metrics, core)?,
        })
    }

    pub fn sink(&self) -> &S {
        &self.router.downstream().sink
    }

    pub fn sink_mut(&mut self) -> &mut S {
        &mut self.router.downstream_mut().sink
    }

    /// Whether the table's `End` has been emitted.
    pub fn ended(&self) -> bool {
        self.router.ended()
    }

    pub fn into_inner(self) -> S {
        self.router.into_inner().sink
    }
}

impl<S: TableSink> Sink for TableFromJson<S> {
    fn event(&mut self, ev: JsonEvent<'_>) -> Result<Flow, Fail> {
        self.router.event(ev)
    }
}

/// Whether every column's path can be moved out of a row without robbing
/// another column: no path is another's prefix, and none repeats. The
/// projection then takes each cell instead of copying its text.
fn paths_are_disjoint(columns: &[BoundColumn]) -> bool {
    let mut seen: HashSet<&[Segment]> = HashSet::with_capacity(columns.len());
    for column in columns {
        if !seen.insert(column.source.as_slice()) {
            return false;
        }
    }
    columns
        .iter()
        .all(|column| (0..column.source.len()).all(|n| !seen.contains(&column.source[..n])))
}

/// The path of a value within a row, for messages.
fn cell_path(row: &Path, source: &[Segment]) -> Path {
    let mut p = row.clone();
    p.0.extend(source.iter().cloned());
    p
}

/// The columns the first row implies, by its kind: an object's member
/// names, in its order, each sourced at its key; an array's positions,
/// labelled `0`, `1`, ... up to its length, each sourced at its index; and
/// for a scalar one column, `value`, sourced at the row itself. A later
/// row of any kind projects through these paths, and lands empty where
/// they miss.
fn infer_columns(
    row: &Datum,
    max_columns: usize,
    max_metadata_bytes: usize,
) -> Result<Vec<BoundColumn>, Fail> {
    // The bounds are checked before a column is built: the labels are the
    // table's metadata for as long as it lasts, so they are held to the
    // bound a metadata capture is, measured as the array of their strings
    // would be, and counted from the row itself, so a first row wider than
    // either bound costs a walk of what it already holds and allocates
    // nothing.
    let (count, labels) = match row {
        Datum::Object(members) => (members.len(), members.keys().map(|k| k.len()).sum()),
        Datum::Array(items) => (items.len(), (0..items.len()).map(decimal_digits).sum()),
        _ => (1, "value".len()),
    };
    let bytes = (count + 1) * NODE_BYTES + labels;
    if bytes > max_metadata_bytes {
        return Err(Fail::limit(
            "max_metadata_bytes",
            max_metadata_bytes as u64,
            format!(
                "the first row's {count} column labels take {bytes} bytes as the table's metadata, more than {max_metadata_bytes}"
            ),
        ));
    }
    if count > max_columns {
        return Err(Fail::limit(
            "max_columns",
            max_columns as u64,
            format!("the first row declares {count} columns, more than {max_columns}"),
        ));
    }
    Ok(match row {
        Datum::Object(members) => members
            .keys()
            .map(|k| BoundColumn::new(k.clone(), vec![Segment::Key(k.clone())]))
            .collect(),
        Datum::Array(items) => (0..items.len())
            .map(|i| BoundColumn::new(i.to_string(), vec![Segment::Index(i)]))
            .collect(),
        _ => vec![BoundColumn::new("value", Vec::new())],
    })
}

/// The length of `i` written in decimal: the bytes of an array row's label.
fn decimal_digits(i: usize) -> usize {
    i.checked_ilog10().map_or(1, |d| d as usize + 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::OwnedJsonEvent;
    use crate::route::tests::{metadata_selector, records_selector, worked_example};
    use crate::sink::replay;
    use crate::table::{column_from_meta, Table};

    fn from_metadata() -> TableBinding {
        TableBinding {
            schema: Schema::FromMetadata {
                columns: metadata_selector(),
                column: Box::new(column_from_meta),
            },
            rows: records_selector(),
        }
    }

    fn run(binding: TableBinding, events: &[OwnedJsonEvent]) -> Result<Table, Fail> {
        run_with(binding, &Limits::default(), events)
    }

    fn run_with(
        binding: TableBinding,
        limits: &Limits,
        events: &[OwnedJsonEvent],
    ) -> Result<Table, Fail> {
        let mut t = TableFromJson::new(
            binding,
            limits,
            Duplicates::Reject,
            Metrics::new(),
            Table::default(),
        )?;
        replay(events, &mut t)?;
        Ok(t.into_inner())
    }

    fn labels(t: &Table) -> Vec<&str> {
        t.columns.iter().map(|c| &*c.label).collect()
    }

    fn rows(t: &Table) -> Vec<Vec<String>> {
        t.rows
            .iter()
            .map(|r| r.iter().map(Cell::to_string).collect())
            .collect()
    }

    fn doc(json: &str) -> Vec<OwnedJsonEvent> {
        let d = Datum::from_json(&serde_json::from_str(json).unwrap());
        let mut rec: Vec<OwnedJsonEvent> = Vec::new();
        crate::walk_datum(&d, &mut rec).unwrap();
        rec.push(OwnedJsonEvent::End);
        rec
    }

    #[test]
    fn the_worked_example_yields_the_spec_table_with_lexemes_kept() {
        let t = run(from_metadata(), &worked_example()).unwrap();
        assert_eq!(labels(&t), ["Identifier", "Full name", "Balance"]);
        assert_eq!(
            rows(&t),
            vec![
                vec!["123", "\"Alice\"", "50.25"],
                vec!["456", "\"Bob\"", "72"],
            ]
        );
        assert_eq!(
            t.rows[0][2],
            Cell::Number {
                value: 50.25,
                lexeme: Some("50.25".into())
            }
        );
        assert_eq!(
            t.rows[1][2],
            Cell::Number {
                value: 72.0,
                lexeme: None
            }
        );
        assert!(t.ended);
    }

    #[test]
    fn a_row_before_the_metadata_is_refused_at_its_start() {
        let events = doc(
            r#"{"response":{"payload":{"deep":{"records":[{"id":1}]}},"metadata":{"fields":[{"title":"Id","path":["id"]}]}}}"#,
        );
        let err = run(from_metadata(), &events).unwrap_err();
        assert_eq!(err.code, Code::InputOrderViolation);
        assert_eq!(
            err.path.as_deref(),
            Some(".response.payload.deep.records[0]")
        );
    }

    #[test]
    fn metadata_selected_twice_is_an_order_violation() {
        let events = doc(
            r#"[{"response":{"metadata":{"fields":[]}}},{"response":{"metadata":{"fields":[]}}}]"#,
        );
        let binding = TableBinding {
            schema: Schema::FromMetadata {
                columns: Selector::root()
                    .each_index()
                    .property("response")
                    .property("metadata")
                    .property("fields"),
                column: Box::new(column_from_meta),
            },
            rows: Selector::root().property("rows").each_index(),
        };
        let err = run(binding, &events).unwrap_err();
        assert_eq!(err.code, Code::InputOrderViolation);
        assert_eq!(err.path.as_deref(), Some("[1].response.metadata.fields"));
    }

    #[test]
    fn missing_and_explicit_null_are_told_apart_by_policy() {
        let events = doc(r#"{"rows":[{"a":null},{}]}"#);
        let binding = |policy| TableBinding {
            schema: Schema::Static(vec![BoundColumn {
                label: "A".into(),
                source: vec![Segment::key("a")],
                missing: policy,
            }]),
            rows: Selector::root().property("rows").each_index(),
        };
        let t = run(binding(MissingPolicy::Missing), &events).unwrap();
        assert_eq!(t.rows, vec![vec![Cell::Null], vec![Cell::Missing]]);
        let t = run(binding(MissingPolicy::Null), &events).unwrap();
        assert_eq!(t.rows, vec![vec![Cell::Null], vec![Cell::Null]]);
        let err = run(binding(MissingPolicy::Error), &events).unwrap_err();
        assert_eq!(err.code, Code::MissingValue);
        assert_eq!(err.path.as_deref(), Some(".rows[1].a"));
    }

    #[test]
    fn zero_rows_is_an_empty_table_with_its_schema() {
        let events = doc(
            r#"{"response":{"metadata":{"fields":[{"title":"Id","path":["id"]}]},"payload":{"deep":{"records":[]}}}}"#,
        );
        let t = run(from_metadata(), &events).unwrap();
        assert_eq!(labels(&t), ["Id"]);
        assert!(t.rows.is_empty());
        assert!(t.ended);

        let t = run(
            TableBinding {
                schema: Schema::Static(vec![BoundColumn::new("x", vec![Segment::key("x")])]),
                rows: Selector::root().each_index(),
            },
            &doc("[]"),
        )
        .unwrap();
        assert_eq!(labels(&t), ["x"]);
        assert!(t.rows.is_empty() && t.ended);

        let t = run(
            TableBinding {
                schema: Schema::Infer,
                rows: Selector::root().each_index(),
            },
            &doc("[]"),
        )
        .unwrap();
        assert!(t.columns.is_empty() && t.rows.is_empty() && t.ended);
    }

    #[test]
    fn metadata_that_never_arrives_is_invalid_input() {
        let err = run(from_metadata(), &doc(r#"{"other":1}"#)).unwrap_err();
        assert_eq!(err.code, Code::InputInvalid);
        assert_eq!(err.path.as_deref(), Some(".response.metadata.fields"));
    }

    #[test]
    fn infer_takes_the_first_rows_members_in_its_order() {
        let events = doc(r#"[{"b":1,"a":"x"},{"a":"y","c":true},{"b":3}]"#);
        let t = run(
            TableBinding {
                schema: Schema::Infer,
                rows: Selector::root().each_index(),
            },
            &events,
        )
        .unwrap();
        assert_eq!(labels(&t), ["b", "a"]);
        assert_eq!(
            rows(&t),
            vec![
                vec!["1", "\"x\""],
                vec!["missing", "\"y\""],
                vec!["3", "missing"]
            ]
        );
    }

    #[test]
    fn infer_labels_an_array_row_by_position() {
        let infer = || TableBinding {
            schema: Schema::Infer,
            rows: Selector::root().each_index(),
        };
        let t = run(infer(), &doc(r#"[[1,"x"],["y",true,3],[2]]"#)).unwrap();
        assert_eq!(labels(&t), ["0", "1"]);
        assert_eq!(
            rows(&t),
            vec![
                vec!["1", "\"x\""],
                vec!["\"y\"", "true"],
                vec!["2", "missing"]
            ]
        );
        // An empty array row is a table of no columns, as no rows is.
        let t = run(infer(), &doc("[[],[1]]")).unwrap();
        assert!(t.columns.is_empty());
        assert_eq!(rows(&t), vec![Vec::<&str>::new(), Vec::new()]);
        assert!(t.ended);
    }

    #[test]
    fn infer_gives_a_scalar_row_one_value_column() {
        let t = run(
            TableBinding {
                schema: Schema::Infer,
                rows: Selector::root().each_index(),
            },
            &doc(r#"[1,"s",true,null]"#),
        )
        .unwrap();
        assert_eq!(labels(&t), ["value"]);
        assert_eq!(
            rows(&t),
            vec![vec!["1"], vec!["\"s\""], vec!["true"], vec!["null"]]
        );
    }

    /// A later row of another kind than the first projects through the
    /// first row's paths: a key path on an array or a scalar, and an index
    /// path on an object or a scalar, miss, so the cell is missing under
    /// the column's policy; the empty path of a `value` column finds every
    /// row, a container as its compact JSON text.
    #[test]
    fn infer_projects_a_later_row_of_another_kind_through_the_first_rows_paths() {
        let infer = || TableBinding {
            schema: Schema::Infer,
            rows: Selector::root().each_index(),
        };
        let t = run(infer(), &doc(r#"[{"a":1},[2],3]"#)).unwrap();
        assert_eq!(labels(&t), ["a"]);
        assert_eq!(rows(&t), vec![vec!["1"], vec!["missing"], vec!["missing"]]);
        let t = run(infer(), &doc(r#"[[1],{"0":2},3]"#)).unwrap();
        assert_eq!(labels(&t), ["0"]);
        assert_eq!(rows(&t), vec![vec!["1"], vec!["missing"], vec!["missing"]]);
        let t = run(infer(), &doc(r#"[1,{"a":2},[3]]"#)).unwrap();
        assert_eq!(labels(&t), ["value"]);
        assert_eq!(
            rows(&t),
            vec![vec!["1"], vec![r#""{\"a\":2}""#], vec![r#""[3]""#]]
        );
    }

    /// The bounds are held before a column is built: a first row wider
    /// than `max_columns`, or whose labels pass `max_metadata_bytes`, is
    /// refused from its own width and keys, naming the row, whatever its
    /// kind; the label bytes are counted as the columns would be.
    #[test]
    fn infer_holds_the_bounds_before_it_builds_a_column() {
        let infer = || TableBinding {
            schema: Schema::Infer,
            rows: Selector::root().each_index(),
        };
        let wide = format!("[[{}]]", vec!["0"; 100_000].join(","));
        let limits = Limits {
            max_columns: 1,
            ..Limits::default()
        };
        let fail = run_with(infer(), &limits, &doc(&wide)).unwrap_err();
        assert_eq!(
            fail.limit.as_ref().map(|l| l.name),
            Some("max_columns"),
            "{fail}"
        );
        assert_eq!(fail.path.as_deref(), Some("[0]"), "{fail}");
        assert!(fail.message.contains("100000 columns"), "{fail}");
        // Ten labels, "0" to "9", each a node and a byte, in an array of
        // them: one byte short of what they take is refused.
        let ten = format!("[[{}]]", ["0"; 10].join(","));
        let bytes = 11 * NODE_BYTES + 10;
        let limits = Limits {
            max_metadata_bytes: bytes - 1,
            ..Limits::default()
        };
        let fail = run_with(infer(), &limits, &doc(&ten)).unwrap_err();
        assert_eq!(
            fail.limit.as_ref().map(|l| l.name),
            Some("max_metadata_bytes"),
            "{fail}"
        );
        let limits = Limits {
            max_metadata_bytes: bytes,
            ..Limits::default()
        };
        assert!(run_with(infer(), &limits, &doc(&ten)).is_ok());
        assert_eq!(decimal_digits(0), 1);
        assert_eq!(decimal_digits(9), 1);
        assert_eq!(decimal_digits(10), 2);
        assert_eq!(decimal_digits(99_999), 5);
    }

    #[test]
    fn a_static_schema_projects_nested_paths_and_ignores_member_order() {
        let events = doc(
            r#"{"rows":[{"p":{"n":"a"},"id":1,"tags":["t0","t1"]},{"tags":["u0"],"id":2,"p":{"n":"b"}}]}"#,
        );
        let t = run(
            TableBinding {
                schema: Schema::Static(vec![
                    BoundColumn::new("Id", vec![Segment::key("id")]),
                    BoundColumn::new("Name", vec![Segment::key("p"), Segment::key("n")]),
                    BoundColumn::new("First tag", vec![Segment::key("tags"), Segment::Index(0)]),
                    BoundColumn::new("Second tag", vec![Segment::key("tags"), Segment::Index(1)]),
                ]),
                rows: Selector::root().property("rows").each_index(),
            },
            &events,
        )
        .unwrap();
        assert_eq!(labels(&t), ["Id", "Name", "First tag", "Second tag"]);
        assert_eq!(
            rows(&t),
            vec![
                vec!["1", "\"a\"", "\"t0\"", "\"t1\""],
                vec!["2", "\"b\"", "\"u0\"", "missing"],
            ]
        );
    }

    /// Cells are moved out of an owned row when no column's path is
    /// another's prefix; when one is, the row is read in place so the
    /// second column still finds its value.
    #[test]
    fn a_column_whose_path_is_another_columns_prefix_still_gets_its_value() {
        let events = doc(r#"{"rows":[{"p":{"n":"a"},"id":1},{"p":{"n":"b"},"id":1}]}"#);
        let overlapping = vec![
            BoundColumn::new("P", vec![Segment::key("p")]),
            BoundColumn::new("N", vec![Segment::key("p"), Segment::key("n")]),
            BoundColumn::new("Id", vec![Segment::key("id")]),
            BoundColumn::new("Id again", vec![Segment::key("id")]),
        ];
        assert!(!paths_are_disjoint(&overlapping));
        let t = run(
            TableBinding {
                schema: Schema::Static(overlapping),
                rows: Selector::root().property("rows").each_index(),
            },
            &events,
        )
        .unwrap();
        assert_eq!(
            rows(&t),
            vec![
                vec![r#""{\"n\":\"a\"}""#, "\"a\"", "1", "1"],
                vec![r#""{\"n\":\"b\"}""#, "\"b\"", "1", "1"],
            ]
        );
        let disjoint = vec![
            BoundColumn::new("N", vec![Segment::key("p"), Segment::key("n")]),
            BoundColumn::new("Id", vec![Segment::key("id")]),
        ];
        assert!(paths_are_disjoint(&disjoint));
        assert!(paths_are_disjoint(&[BoundColumn::new("Row", vec![])]));
        assert!(!paths_are_disjoint(&[
            BoundColumn::new("Row", vec![]),
            BoundColumn::new("Id", vec![Segment::key("id")]),
        ]));
        let t = run(
            TableBinding {
                schema: Schema::Static(disjoint),
                rows: Selector::root().property("rows").each_index(),
            },
            &events,
        )
        .unwrap();
        assert_eq!(rows(&t), vec![vec!["\"a\"", "1"], vec!["\"b\"", "1"]]);
    }

    #[test]
    fn max_columns_and_max_metadata_bytes_are_enforced_by_name() {
        let limits = Limits {
            max_columns: 2,
            ..Limits::default()
        };
        let err = run_with(from_metadata(), &limits, &worked_example()).unwrap_err();
        assert_eq!(err.code, Code::ResourceLimitExceeded);
        assert_eq!(err.limit.as_ref().unwrap().name, "max_columns");

        let limits = Limits {
            max_metadata_bytes: 64,
            ..Limits::default()
        };
        let err = run_with(from_metadata(), &limits, &worked_example()).unwrap_err();
        assert_eq!(err.limit.as_ref().unwrap().name, "max_metadata_bytes");

        let limits = Limits {
            max_record_bytes: 48,
            ..Limits::default()
        };
        let err = run_with(from_metadata(), &limits, &worked_example()).unwrap_err();
        assert_eq!(err.limit.as_ref().unwrap().name, "max_record_bytes");
        assert_eq!(
            err.path.as_deref(),
            Some(".response.payload.deep.records[0]")
        );

        let limits = Limits {
            max_columns: 1,
            ..Limits::default()
        };
        let err = run_with(
            TableBinding {
                schema: Schema::Infer,
                rows: Selector::root().each_index(),
            },
            &limits,
            &doc(r#"[{"a":1,"b":2}]"#),
        )
        .unwrap_err();
        assert_eq!(err.limit.as_ref().unwrap().name, "max_columns");

        // The inferred names are the table's metadata, held to its bound:
        // two one-byte names take 16 + 2 * (16 + 1) = 50 bytes.
        let infer = || TableBinding {
            schema: Schema::Infer,
            rows: Selector::root().each_index(),
        };
        let at = |max_metadata_bytes| Limits {
            max_metadata_bytes,
            ..Limits::default()
        };
        let err = run_with(infer(), &at(49), &doc(r#"[{"a":1,"b":2}]"#)).unwrap_err();
        assert_eq!(err.code, Code::ResourceLimitExceeded, "{err}");
        assert_eq!(err.limit.as_ref().unwrap().name, "max_metadata_bytes");
        assert_eq!(err.path.as_deref(), Some("[0]"));
        let t = run_with(infer(), &at(50), &doc(r#"[{"a":1,"b":2}]"#)).unwrap();
        assert_eq!(labels(&t), ["a", "b"]);
        // Positional labels and the `value` label are measured the same
        // way: "0" and "1" take 50 bytes too, and "value" 16 + 16 + 5 = 37.
        let err = run_with(infer(), &at(49), &doc("[[1,2]]")).unwrap_err();
        assert_eq!(err.limit.as_ref().unwrap().name, "max_metadata_bytes");
        assert_eq!(err.path.as_deref(), Some("[0]"));
        let t = run_with(infer(), &at(50), &doc("[[1,2]]")).unwrap();
        assert_eq!(labels(&t), ["0", "1"]);
        let err = run_with(infer(), &at(36), &doc("[1]")).unwrap_err();
        assert_eq!(err.limit.as_ref().unwrap().name, "max_metadata_bytes");
        let t = run_with(infer(), &at(37), &doc("[1]")).unwrap();
        assert_eq!(labels(&t), ["value"]);
        let err = run_with(
            infer(),
            &Limits {
                max_columns: 1,
                ..Limits::default()
            },
            &doc("[[1,2]]"),
        )
        .unwrap_err();
        assert_eq!(err.limit.as_ref().unwrap().name, "max_columns");
    }

    #[test]
    fn a_bad_descriptor_names_its_position() {
        let events = doc(
            r#"{"response":{"metadata":{"fields":[{"title":"Id","path":["id"]},{"path":["x"]}]},"payload":{"deep":{"records":[]}}}}"#,
        );
        let err = run(from_metadata(), &events).unwrap_err();
        assert_eq!(err.code, Code::InputInvalid);
        assert_eq!(err.path.as_deref(), Some(".response.metadata.fields[1]"));
        let events = doc(
            r#"{"response":{"metadata":{"fields":{"title":"Id"}},"payload":{"deep":{"records":[]}}}}"#,
        );
        let err = run(from_metadata(), &events).unwrap_err();
        assert_eq!(err.code, Code::InputInvalid);
    }

    #[test]
    fn end_arrives_only_with_the_documents_end_and_rows_count() {
        let metrics = Metrics::new();
        let mut t = TableFromJson::new(
            from_metadata(),
            &Limits::default(),
            Duplicates::Reject,
            metrics.clone(),
            Table::default(),
        )
        .unwrap();
        let events = worked_example();
        replay(&events[..events.len() - 1], &mut t).unwrap();
        assert!(!t.ended());
        assert!(!t.sink().ended);
        assert_eq!(t.sink().rows.len(), 2);
        t.event(JsonEvent::End).unwrap();
        assert!(t.ended() && t.sink().ended);
        assert_eq!(Metrics::get(&metrics.rows), 2);
    }

    #[test]
    fn a_stop_from_the_table_sink_stops_the_run() {
        struct StopAtSchema;
        impl TableSink for StopAtSchema {
            fn table_event(&mut self, ev: TableEvent<'_>) -> Result<Flow, Fail> {
                match ev {
                    TableEvent::Schema(_) => Ok(Flow::Stop),
                    _ => panic!("nothing follows a stop"),
                }
            }
        }
        let mut t = TableFromJson::new(
            from_metadata(),
            &Limits::default(),
            Duplicates::Reject,
            Metrics::new(),
            StopAtSchema,
        )
        .unwrap();
        assert_eq!(replay(&worked_example(), &mut t).unwrap(), Flow::Stop);
    }

    #[test]
    fn overlapping_metadata_and_rows_are_refused() {
        let err = TableFromJson::new(
            TableBinding {
                schema: Schema::FromMetadata {
                    columns: Selector::root().property("a"),
                    column: Box::new(column_from_meta),
                },
                rows: Selector::root().property("a").each_index(),
            },
            &Limits::default(),
            Duplicates::Reject,
            Metrics::new(),
            Table::default(),
        )
        .err()
        .unwrap();
        assert_eq!(err.code, Code::CaptureOverlapUnsupported);
    }
}
