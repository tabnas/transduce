// The harness behind the shared fixtures in ../../test/spec, which every
// runtime of this crate runs. Cargo compiles this module into EVERY
// integration test binary, so an item only some binaries use is dead
// code in the others; the allow keeps that from being a warning rather
// than hiding anything real.
//
// What a row means is documented in docs/reference.md ("Shared
// fixtures"): which source its `grammar` and `mode` name, what each JSON
// column decodes to, and how the result is encoded as the fixture's
// expected JSON. A port's harness reproduces exactly that; nothing here is
// specific to Rust except the calls into this crate's API.
#![allow(dead_code)]

use std::cell::RefCell;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use tabnas::Tabnas;
use tabnas_csv::CsvOptions;
use tabnas_support::{find_spec_dir, Failure, Row, Value};
use tabnas_transduce::source::{LineFormat, LinesSource};
use tabnas_transduce::{
    column_from_meta, BoundColumn, CaptureSpec, Cell, Code, Datum, Duplicates, Fail, Flow, Limits,
    Metrics, MissingPolicy, OwnedJsonEvent, ParserSource, Prune, RouteSink, Router, ScanEmit,
    Schema, Segment, Selected, Selector, Sink, Source, SourceMode, TableBinding, TableEvent,
    TableFromJson, TableSink, Transition, ValueSource,
};

/// The shared `test/spec` directory, found by walking up from the crate.
pub fn spec_dir() -> PathBuf {
    find_spec_dir(Some(Path::new(env!("CARGO_MANIFEST_DIR"))))
        .expect("a test/spec directory above rs/")
}

/// The fixture files and the runner each has: a new file without a runner
/// fails `every_fixture_has_a_runner` rather than passing unread.
pub const FIXTURES: [&str; 6] = [
    "events.tsv",
    "limits.tsv",
    "lines.tsv",
    "route.tsv",
    "scan.tsv",
    "table.tsv",
];

/// A run that failed: the failure, and for an event stream the events
/// that left before it, which an `events.tsv` row may pin in its
/// `prefix` column. The failure is boxed: a `Fail` is large, and this is
/// every stage's error type.
#[derive(Debug)]
pub struct Failed {
    pub fail: Box<Fail>,
    pub prefix: Option<Value>,
}

impl From<Fail> for Failed {
    fn from(fail: Fail) -> Failed {
        Failed {
            fail: Box::new(fail),
            prefix: None,
        }
    }
}

/// A failure as the shared runner sees it: the code, and the position
/// when the failure has one. A row may also pin the failure's `path`, the
/// `limit` it names and the `prefix` of events emitted before it, in
/// columns of those names; when one differs, the code is reported with
/// the difference attached, so the row fails with it in its report.
pub fn to_failure(failed: &Failed, row: &Row) -> Failure {
    let fail = &failed.fail;
    let mut code = fail.code.as_str().to_string();
    let mut differences = Vec::new();
    let want_path = row.named("path");
    if !want_path.is_empty() {
        let got = fail.path.as_deref().unwrap_or("<none>");
        if got != want_path {
            differences.push(format!("path {got}, the fixture pins {want_path}"));
        }
    }
    let want_limit = row.named("limit");
    if !want_limit.is_empty() {
        let got = fail.limit.as_ref().map_or("<none>", |limit| limit.name);
        if got != want_limit {
            differences.push(format!("limit {got}, the fixture pins {want_limit}"));
        }
    }
    let want_prefix = row.named("prefix");
    if !want_prefix.is_empty() {
        let want = tabnas_support::parse_expect(want_prefix)
            .unwrap_or_else(|e| panic!("{}: prefix is not JSON: {e}", row.location()));
        match &failed.prefix {
            Some(got) if tabnas_support::equal_value(got, &want) => {}
            got => differences.push(format!(
                "prefix {}, the fixture pins {want}",
                got.as_ref().map_or("<none>".to_string(), Value::to_string)
            )),
        }
    }
    if !differences.is_empty() {
        code = format!("{code} ({})", differences.join("; "));
    }
    let mut failure = Failure::new(code).with_message(fail.to_string());
    if let (Some(line), Some(col)) = (fail.row, fail.column) {
        failure = failure.at(line as usize, col as usize);
    }
    failure
}

// ---------------------------------------------------------------------
// Reading the columns.

fn json_cell(row: &Row, name: &str) -> Option<serde_json::Value> {
    let cell = row.named(name);
    if cell.is_empty() {
        return None;
    }
    Some(
        serde_json::from_str(cell).unwrap_or_else(|e| {
            panic!("{}: column {name} is not JSON: {e}: {cell}", row.location())
        }),
    )
}

/// The grammar a row names, as its `make()`.
pub fn grammar(name: &str) -> fn() -> Tabnas {
    match name {
        "json" => tabnas_json::make,
        "jsonl" => tabnas_jsonl::make,
        "json5" => tabnas_json5::make,
        "jsonc" => tabnas_jsonc::make,
        "jsonic" => tabnas_jsonic::make,
        "yaml" => tabnas_yaml::make,
        "zon" => tabnas_zon::make,
        "csv" => tabnas_csv::make,
        "toml" => tabnas_toml::make,
        "ini" => tabnas_ini::make,
        other => panic!("no grammar {other:?} in the fixture harness"),
    }
}

/// A selector from its JSON steps: a string is a property, a non-negative
/// integer an index, `{"each":"index"}` every element, `{"each":"member"}`
/// every member value.
pub fn selector(steps: &serde_json::Value) -> Selector {
    let steps = steps
        .as_array()
        .unwrap_or_else(|| panic!("a selector is an array of steps: {steps}"));
    steps.iter().fold(Selector::root(), |sel, step| match step {
        serde_json::Value::String(name) => sel.property(name.as_str()),
        serde_json::Value::Number(n) => sel.index(
            n.as_u64()
                .unwrap_or_else(|| panic!("an index step is a non-negative integer: {n}"))
                as usize,
        ),
        serde_json::Value::Object(each) => match each.get("each").and_then(|e| e.as_str()) {
            Some("index") => sel.each_index(),
            Some("member") => sel.each_member(),
            _ => {
                panic!("an each step is {{\"each\":\"index\"}} or {{\"each\":\"member\"}}: {step}")
            }
        },
        other => panic!("not a selector step: {other}"),
    })
}

/// A concrete path from its JSON segments: a string is a key, a
/// non-negative integer an index.
pub fn segments(json: &serde_json::Value) -> Vec<Segment> {
    json.as_array()
        .unwrap_or_else(|| panic!("a path is an array of segments: {json}"))
        .iter()
        .map(|seg| match seg {
            serde_json::Value::String(key) => Segment::key(key.as_str()),
            serde_json::Value::Number(n) => Segment::Index(
                n.as_u64()
                    .unwrap_or_else(|| panic!("an index segment is a non-negative integer: {n}"))
                    as usize,
            ),
            other => panic!("not a path segment: {other}"),
        })
        .collect()
}

/// `Limits::default()` with the row's `limits` column (a JSON object of
/// field names to values) applied over it.
pub fn limits(row: &Row) -> Limits {
    let mut limits = Limits::default();
    let Some(json) = json_cell(row, "limits") else {
        return limits;
    };
    let fields = json
        .as_object()
        .unwrap_or_else(|| panic!("{}: limits is a JSON object", row.location()));
    for (name, value) in fields {
        let n = value
            .as_u64()
            .unwrap_or_else(|| panic!("{}: limit {name} is not a count", row.location()));
        let n = n as usize;
        match name.as_str() {
            "max_depth" => limits.max_depth = n,
            "max_key_bytes" => limits.max_key_bytes = n,
            "max_scalar_bytes" => limits.max_scalar_bytes = n,
            "max_metadata_bytes" => limits.max_metadata_bytes = n,
            "max_columns" => limits.max_columns = n,
            "max_record_bytes" => limits.max_record_bytes = n,
            "max_capture_bytes" => limits.max_capture_bytes = n,
            "max_output_bytes" => limits.max_output_bytes = Some(n as u64),
            other => panic!("{}: no limit {other:?}", row.location()),
        }
    }
    limits
}

/// The row's `duplicates` policy: `reject` (the default), `last_wins` or
/// `first_wins`.
pub fn duplicates(row: &Row) -> Duplicates {
    match row.named("duplicates") {
        "" | "reject" => Duplicates::Reject,
        "last_wins" => Duplicates::LastWins,
        "first_wins" => Duplicates::FirstWins,
        other => panic!("{}: no duplicates policy {other:?}", row.location()),
    }
}

fn prune(row: &Row) -> Prune {
    match json_cell(row, "prune") {
        None => Prune::Never,
        Some(serde_json::Value::String(all)) if all == "all" => Prune::AllArrays,
        Some(steps) => Prune::Under(selector(&steps)),
    }
}

/// The CSV line format and chunk size from the row's `options` column:
/// `header` (default true), `object`, `number`, `value`, `trim` and
/// `strict` for the grammar, and `chunk_bytes` for the source.
fn line_options(row: &Row) -> (LineFormat, Option<usize>) {
    let json = json_cell(row, "options").unwrap_or(serde_json::json!({}));
    let flag = |name: &str| json.get(name).and_then(serde_json::Value::as_bool);
    let chunk = json
        .get("chunk_bytes")
        .and_then(serde_json::Value::as_u64)
        .map(|n| n as usize);
    let format = match row.named("grammar") {
        "jsonl" => LineFormat::Jsonl,
        "csv" => {
            let mut options = CsvOptions::default();
            if let Some(object) = flag("object") {
                options.object = object;
            }
            if let Some(strict) = flag("strict") {
                options.strict = strict;
            }
            options.number = flag("number");
            options.value = flag("value");
            options.trim = flag("trim");
            LineFormat::csv_with(flag("header").unwrap_or(true), options)
        }
        other => panic!("{}: no line format for grammar {other:?}", row.location()),
    };
    (format, chunk)
}

// ---------------------------------------------------------------------
// Driving a source.

/// Run the row's source into `sink`: `grammar` and `mode` name it.
///
/// - `materialize` and `incremental`: `ParserSource` over the grammar,
///   named with `.grammar`, in that mode (`prune` column for incremental).
/// - `value`: the grammar's parse (an engine error is `Fail::from_tabnas`),
///   then `ValueSource` over the value. No limits apply on this path.
/// - `lines` and `lines-incremental`: `LinesSource` over the text, the
///   walking path (`Source::run`) and the owned path (`run_owned`).
pub fn drive<S: Sink + Send + 'static>(row: &Row, input: &str, sink: S) -> (Result<Flow, Fail>, S) {
    let name = row.named("grammar");
    let limits = limits(row);
    match row.named("mode") {
        mode @ ("materialize" | "incremental") => {
            let mode = if mode == "materialize" {
                SourceMode::Materialize
            } else {
                SourceMode::Incremental { prune: prune(row) }
            };
            ParserSource::new(grammar(name)(), input)
                .grammar(name)
                .mode(mode)
                .limits(limits)
                .run_owned(sink)
        }
        "value" => {
            let mut sink = sink;
            let value = match grammar(name)().parse(input) {
                Ok(value) => value,
                Err(e) => return (Err(Fail::from_tabnas(&e)), sink),
            };
            let outcome = ValueSource(&value).run(&mut sink);
            (outcome, sink)
        }
        mode @ ("lines" | "lines-incremental") => {
            let (format, chunk) = line_options(row);
            let mut source =
                LinesSource::new(Cursor::new(input.as_bytes().to_vec()), format).limits(limits);
            if let Some(chunk) = chunk {
                source = source.chunk_bytes(chunk);
            }
            if mode == "lines" {
                let mut sink = sink;
                let outcome = source.run(&mut sink);
                (outcome, sink)
            } else {
                source.run_owned(sink)
            }
        }
        other => panic!("{}: no mode {other:?}", row.location()),
    }
}

// ---------------------------------------------------------------------
// The encodings.

fn s(text: &str) -> Value {
    Value::String(text.to_string())
}

fn number(value: f64, lexeme: Option<&str>) -> Value {
    Value::Array(vec![
        s("number"),
        Value::Number(value),
        lexeme.map_or(Value::Null, s),
    ])
}

/// One `JsonEvents/1` event: `["object_start"]`, `["key", name]`,
/// `["number", value, lexeme or null]`, `["end"]` and so on.
pub fn event(ev: &OwnedJsonEvent) -> Value {
    let tag = |t: &str| Value::Array(vec![s(t)]);
    match ev {
        OwnedJsonEvent::ObjectStart => tag("object_start"),
        OwnedJsonEvent::ObjectEnd => tag("object_end"),
        OwnedJsonEvent::ArrayStart => tag("array_start"),
        OwnedJsonEvent::ArrayEnd => tag("array_end"),
        OwnedJsonEvent::Key(k) => Value::Array(vec![s("key"), s(k)]),
        OwnedJsonEvent::Null => tag("null"),
        OwnedJsonEvent::Bool(b) => Value::Array(vec![s("bool"), Value::Bool(*b)]),
        OwnedJsonEvent::Number { value, lexeme } => number(*value, lexeme.as_deref()),
        OwnedJsonEvent::String(text) => Value::Array(vec![s("string"), s(text)]),
        OwnedJsonEvent::End => tag("end"),
    }
}

/// A retained value: `null`, booleans and strings as themselves,
/// `["number", value, lexeme or null]`, `["array", item...]` and
/// `["object", [key, value]...]`, members in their order.
pub fn datum(d: &Datum) -> Value {
    match d {
        Datum::Null => Value::Null,
        Datum::Bool(b) => Value::Bool(*b),
        Datum::Number { value, lexeme } => number(*value, lexeme.as_deref()),
        Datum::String(text) => s(text),
        Datum::Array(items) => {
            let mut out = vec![s("array")];
            out.extend(items.iter().map(datum));
            Value::Array(out)
        }
        Datum::Object(members) => {
            let mut out = vec![s("object")];
            out.extend(
                members
                    .iter()
                    .map(|(k, v)| Value::Array(vec![s(k), datum(v)])),
            );
            Value::Array(out)
        }
    }
}

/// A table cell: as a datum's scalars, and `["missing"]`.
pub fn cell(c: &Cell) -> Value {
    match c {
        Cell::Null => Value::Null,
        Cell::Bool(b) => Value::Bool(*b),
        Cell::Number { value, lexeme } => number(*value, lexeme.as_deref()),
        Cell::String(text) => s(text),
        Cell::Missing => Value::Array(vec![s("missing")]),
    }
}

// ---------------------------------------------------------------------
// The stages.

/// The row's events, recorded.
pub fn events(row: &Row, input: &str) -> Result<Value, Failed> {
    let (outcome, recorded) = drive(row, input, Vec::<OwnedJsonEvent>::new());
    let recorded = Value::Array(recorded.iter().map(event).collect());
    match outcome {
        Ok(_) => Ok(recorded),
        Err(fail) => Err(Failed {
            fail: Box::new(fail),
            prefix: Some(recorded),
        }),
    }
}

/// What a route delivered: `[tag, path]` for an observed capture, `[tag,
/// path, value]` for a materialized one, and `"end"` when the router
/// called `end`.
#[derive(Default)]
struct Deliveries(Vec<Value>);

impl RouteSink for Deliveries {
    fn selected(&mut self, selected: Selected) -> Result<Flow, Fail> {
        let mut entry = vec![s(&selected.tag), s(&selected.path.to_string())];
        if let Some(value) = &selected.value {
            entry.push(datum(value));
        }
        self.0.push(Value::Array(entry));
        Ok(Flow::Continue)
    }

    fn end(&mut self) -> Result<Flow, Fail> {
        self.0.push(s("end"));
        Ok(Flow::Continue)
    }
}

/// The row's `captures` column: an array of `{"tag", "select", "mode"}`,
/// `mode` being `materialize` (the default) or `observe`.
fn captures(row: &Row) -> Vec<CaptureSpec> {
    let json = json_cell(row, "captures").unwrap_or(serde_json::json!([]));
    json.as_array()
        .unwrap_or_else(|| panic!("{}: captures is an array", row.location()))
        .iter()
        .map(|spec| {
            let tag = spec["tag"].as_str().expect("a capture has a tag");
            let select = selector(&spec["select"]);
            match spec.get("mode").and_then(serde_json::Value::as_str) {
                None | Some("materialize") => CaptureSpec::materialize(tag, select),
                Some("observe") => CaptureSpec::observe(tag, select),
                Some(other) => panic!("{}: no capture mode {other:?}", row.location()),
            }
        })
        .collect()
}

/// The row's source through a `Router` over its captures.
pub fn route(row: &Row, input: &str) -> Result<Value, Failed> {
    let router = Router::new(
        captures(row),
        &limits(row),
        duplicates(row),
        Metrics::new(),
        Deliveries::default(),
    )?;
    let (outcome, router) = drive(row, input, router);
    outcome?;
    Ok(Value::Array(router.into_inner().0))
}

/// `TableRows/1`, recorded in order: `["schema", [label...]]`, `["row",
/// [cell...]]`, `["end"]`.
#[derive(Default)]
struct TableLog(Vec<Value>);

impl TableSink for TableLog {
    fn table_event(&mut self, ev: TableEvent<'_>) -> Result<Flow, Fail> {
        self.0.push(match ev {
            TableEvent::Schema(columns) => Value::Array(vec![
                s("schema"),
                Value::Array(columns.iter().map(|c| s(&c.label)).collect()),
            ]),
            TableEvent::Row(cells) => Value::Array(vec![
                s("row"),
                Value::Array(cells.iter().map(cell).collect()),
            ]),
            TableEvent::End => Value::Array(vec![s("end")]),
        });
        Ok(Flow::Continue)
    }
}

/// The row's `binding` column: `{"rows": steps, "schema": ...}`, the
/// schema `"infer"`, `{"metadata": steps}` (mapped by `column_from_meta`)
/// or `{"static": [{"label", "source": segments, "missing"}]}`, `missing`
/// being `missing` (the default), `null` or `error`.
fn binding(row: &Row) -> TableBinding {
    let json = json_cell(row, "binding")
        .unwrap_or_else(|| panic!("{}: a table row has a binding", row.location()));
    let rows = selector(&json["rows"]);
    let schema = match &json["schema"] {
        serde_json::Value::String(infer) if infer == "infer" => Schema::Infer,
        serde_json::Value::Object(o) if o.contains_key("metadata") => Schema::FromMetadata {
            columns: selector(&o["metadata"]),
            column: Box::new(column_from_meta),
        },
        serde_json::Value::Object(o) if o.contains_key("static") => Schema::Static(
            o["static"]
                .as_array()
                .expect("static columns are an array")
                .iter()
                .map(|c| BoundColumn {
                    label: c["label"].as_str().expect("a column has a label").into(),
                    source: segments(&c["source"]),
                    missing: match c.get("missing").and_then(serde_json::Value::as_str) {
                        None | Some("missing") => MissingPolicy::Missing,
                        Some("null") => MissingPolicy::Null,
                        Some("error") => MissingPolicy::Error,
                        Some(other) => panic!("no missing policy {other:?}"),
                    },
                })
                .collect(),
        ),
        other => panic!("{}: not a schema: {other}", row.location()),
    };
    TableBinding { schema, rows }
}

/// The row's source through `TableFromJson` over its binding.
pub fn table(row: &Row, input: &str) -> Result<Value, Failed> {
    let transducer = TableFromJson::new(
        binding(row),
        &limits(row),
        duplicates(row),
        Metrics::new(),
        TableLog::default(),
    )?;
    let (outcome, transducer) = drive(row, input, transducer);
    outcome?;
    Ok(Value::Array(transducer.into_inner().0))
}

/// The stage a `limits.tsv` row names in its `stage` column.
pub fn staged(row: &Row, input: &str) -> Result<Value, Failed> {
    match row.named("stage") {
        "events" => events(row, input),
        "route" => route(row, input),
        "table" => table(row, input),
        other => panic!("{}: no stage {other:?}", row.location()),
    }
}

// ---------------------------------------------------------------------
// scan-emit.

enum Item {
    Add(i64),
    Emit(Vec<String>),
    Fail(Code),
}

/// A `scan.tsv` script through `ScanEmit`: a running sum whose step emits
/// `+n` for an integer item, the given strings for `{"emit": [...]}` (the
/// state unchanged), and fails with the code `{"fail": CODE}` names; the
/// finish emits `=<sum>`. `"finish"` calls `finish`. The output sink
/// answers `Stop` for the output the `stop_on` column names. The result
/// is `{"out": [output...], "flows": ["continue" | "stop", one per op]}`.
pub fn scan(row: &Row, script: &str) -> Result<Value, Failed> {
    let ops: serde_json::Value = serde_json::from_str(script)
        .unwrap_or_else(|e| panic!("{}: the script is not JSON: {e}", row.location()));
    let stop_on = row.named("stop_on").to_string();
    let out = Rc::new(RefCell::new(Vec::<Value>::new()));
    let seen = Rc::clone(&out);
    let mut scan = ScanEmit::new(
        0i64,
        |sum: i64, item: Item| match item {
            Item::Add(n) => Ok(Transition::emit(sum + n, format!("+{n}"))),
            Item::Emit(outputs) => Ok(Transition::new(sum, outputs)),
            Item::Fail(code) => Err(Fail::new(code, "the step failed")),
        },
        |sum: i64| Ok(vec![format!("={sum}")]),
        move |output: String| {
            let flow = if output == stop_on {
                Flow::Stop
            } else {
                Flow::Continue
            };
            seen.borrow_mut().push(Value::String(output));
            Ok(flow)
        },
    );
    let mut flows = Vec::new();
    for op in ops.as_array().expect("a script is an array of ops") {
        let flow = match op {
            serde_json::Value::String(finish) if finish == "finish" => scan.finish()?,
            serde_json::Value::Number(n) => {
                scan.item(Item::Add(n.as_i64().expect("an integer item")))?
            }
            serde_json::Value::Object(o) if o.contains_key("emit") => scan.item(Item::Emit(
                o["emit"]
                    .as_array()
                    .expect("emit names an array")
                    .iter()
                    .map(|v| v.as_str().expect("emit names strings").to_string())
                    .collect(),
            ))?,
            serde_json::Value::Object(o) if o.contains_key("fail") => {
                let code = o["fail"].as_str().and_then(Code::parse).expect("a code");
                scan.item(Item::Fail(code))?
            }
            other => panic!("{}: not a scan op: {other}", row.location()),
        };
        flows.push(Value::String(
            match flow {
                Flow::Continue => "continue",
                Flow::Stop => "stop",
            }
            .to_string(),
        ));
    }
    drop(scan);
    let out = Rc::try_unwrap(out).expect("the scan is gone").into_inner();
    Ok(Value::Object(vec![
        ("out".to_string(), Value::Array(out)),
        ("flows".to_string(), Value::Array(flows)),
    ]))
}
