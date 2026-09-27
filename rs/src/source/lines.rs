//! [`LinesSource`]: JSON Lines and CSV a record (or a chunk of records) at
//! a time, from any [`BufRead`].
//!
//! The engine parses a whole `&str`, so a document is retained at least
//! once however it is consumed. Line-delimited formats do not need to be
//! one document: JSON Lines is one value per line, and CSV is a header
//! plus independent records, so each can be parsed a piece at a time with
//! one reused parser and the memory a run needs stops depending on the
//! file's size. That is what this source does, and the only reason it
//! exists: its events are exactly the whole-file parse's (the array of
//! the per-line values, or of the records), which the chunk-boundary
//! tests hold it to at every byte.
//!
//! JSON Lines: each non-blank line is parsed with one `tabnas-json`
//! parser. With an owned sink ([`LinesSource::run_owned`]) the line goes
//! through the rule-event adapter, reset per line, so numbers keep their
//! lexemes; with a borrowed sink ([`Source::run`]) the value is walked and
//! numbers carry none. A line that does not parse is `INPUT_INVALID` with
//! the line's number as the row.
//!
//! CSV: the input is cut into chunks of whole records at newlines outside
//! quotes (a `"` toggles quoting, so `""` inside a field is two toggles
//! and a quoted field may span lines). The header record is kept and
//! prepended to every chunk after the first when `header` is on, so the
//! reused `tabnas-csv` parser names each record's fields as the whole
//! file would. A chunk closes at the first record boundary past
//! [`DEFAULT_CHUNK_BYTES`] (or the configured size), so one chunk, and
//! never a fraction of a record, is what a parse holds.
//!
//! Memory is bounded by one chunk, and a record is never split, so a
//! single record larger than `max_record_bytes` (a line, for JSON Lines)
//! fails with that limit's name rather than growing a chunk without bound;
//! `max_depth`, `max_key_bytes` and `max_scalar_bytes` apply to the events
//! as everywhere. `max_record_bytes` counts the record's source bytes here,
//! where a table transducer downstream would count its retained bytes; it
//! is the same idea of "one row" measured before it is parsed.

use std::io::BufRead;
use std::sync::{Arc, Mutex};

use tabnas::Tabnas;
use tabnas_csv::CsvOptions;

use crate::error::Fail;
use crate::event::JsonEvent;
use crate::limits::{AbortFlag, Limits, Metrics};
use crate::sink::{Flow, Sink};
use crate::source::guard::Guarded;
use crate::source::rule_events::{self, Adapter, Status, GUARD};
use crate::source::{walk_value, Prune, Source};

/// How much of the input one CSV parse holds, at most one record over.
pub const DEFAULT_CHUNK_BYTES: usize = 256 * 1024;

/// The line-delimited format to read.
#[derive(Clone, Debug)]
pub enum LineFormat {
    /// One JSON value per line; blank lines are skipped.
    Jsonl,
    /// CSV records. `header` says whether the first record names the
    /// fields (it overrides `options.header`); the other options are the
    /// grammar's (`object: false` for arrays instead of objects, and so
    /// on), boxed because they are large and the format is passed around.
    Csv {
        header: bool,
        options: Box<CsvOptions>,
    },
}

impl LineFormat {
    /// CSV with a header line and the grammar's default options.
    pub fn csv() -> LineFormat {
        LineFormat::Csv {
            header: true,
            options: Box::new(CsvOptions::default()),
        }
    }

    /// CSV with the given options; `header` decides whether the first
    /// record names the fields.
    pub fn csv_with(header: bool, options: CsvOptions) -> LineFormat {
        LineFormat::Csv {
            header,
            options: Box::new(options),
        }
    }
}

/// A line-delimited reader as a source.
pub struct LinesSource<R: BufRead> {
    reader: R,
    format: LineFormat,
    limits: Limits,
    abort: AbortFlag,
    metrics: Arc<Metrics>,
    chunk_bytes: usize,
}

impl<R: BufRead> LinesSource<R> {
    pub fn new(reader: R, format: LineFormat) -> LinesSource<R> {
        LinesSource {
            reader,
            format,
            limits: Limits::default(),
            abort: AbortFlag::new(),
            metrics: Metrics::new(),
            chunk_bytes: DEFAULT_CHUNK_BYTES,
        }
    }

    pub fn limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }

    pub fn abort(mut self, abort: AbortFlag) -> Self {
        self.abort = abort;
        self
    }

    pub fn metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = metrics;
        self
    }

    /// The CSV chunk size; a chunk closes at the first record boundary at
    /// or past it. Zero closes a chunk at every record.
    pub fn chunk_bytes(mut self, bytes: usize) -> Self {
        self.chunk_bytes = bytes;
        self
    }

    /// Run with an owned sink: JSON Lines through the rule-event adapter
    /// (lexemes kept), CSV through the walk. The sink comes back.
    pub fn run_owned<S: Sink + Send + 'static>(self, sink: S) -> (Result<Flow, Fail>, S) {
        match &self.format {
            LineFormat::Jsonl => self.jsonl_incremental(sink),
            LineFormat::Csv { .. } => {
                let mut guarded =
                    Guarded::new(sink, &self.limits, self.abort.clone(), self.metrics.clone());
                let outcome = self.drive(&mut guarded);
                (outcome, guarded.into_inner())
            }
        }
    }

    /// [`LinesSource::run_owned`] for a boxed sink.
    pub fn run_boxed(
        self,
        sink: Box<dyn Sink + Send>,
    ) -> (Result<Flow, Fail>, Box<dyn Sink + Send>) {
        self.run_owned(sink)
    }

    /// The borrowed-sink drive: both formats through the walk.
    fn drive<S: Sink>(self, guarded: &mut Guarded<S>) -> Result<Flow, Fail> {
        let LinesSource {
            reader,
            format,
            limits,
            abort,
            chunk_bytes,
            ..
        } = self;
        match format {
            LineFormat::Jsonl => {
                let mut parser = tabnas_json::make();
                install_guard(&mut parser, &abort);
                let mut lines = Lines::new(reader, limits.max_record_bytes);
                if guarded.event(JsonEvent::ArrayStart)? == Flow::Stop {
                    return Ok(Flow::Stop);
                }
                while let Some((number, line)) = lines.next_line()? {
                    if line.trim().is_empty() {
                        continue;
                    }
                    let value = parser
                        .parse(line)
                        .map_err(|e| line_failure(&e, number, &abort))?;
                    if walk_value(&value, guarded)? == Flow::Stop {
                        return Ok(Flow::Stop);
                    }
                }
                if guarded.event(JsonEvent::ArrayEnd)? == Flow::Stop {
                    return Ok(Flow::Stop);
                }
                guarded.event(JsonEvent::End)
            }
            LineFormat::Csv { header, options } => {
                let mut options = *options;
                options.header = header;
                let mut parser = tabnas_csv::make_with(options);
                install_guard(&mut parser, &abort);
                let mut chunks = Chunks::new(
                    Lines::new(reader, limits.max_record_bytes),
                    header,
                    chunk_bytes,
                );
                if guarded.event(JsonEvent::ArrayStart)? == Flow::Stop {
                    return Ok(Flow::Stop);
                }
                while let Some(chunk) = chunks.next_chunk()? {
                    let value = parser
                        .parse(&chunk.text)
                        .map_err(|e| chunk.failure(&e, &abort))?;
                    for record in records_of(&value) {
                        if walk_value(record, guarded)? == Flow::Stop {
                            return Ok(Flow::Stop);
                        }
                    }
                }
                if guarded.event(JsonEvent::ArrayEnd)? == Flow::Stop {
                    return Ok(Flow::Stop);
                }
                guarded.event(JsonEvent::End)
            }
        }
    }

    /// JSON Lines with the adapter: one parser, one subscriber, the
    /// adapter reset before each line.
    fn jsonl_incremental<S: Sink + Send + 'static>(self, sink: S) -> (Result<Flow, Fail>, S) {
        let LinesSource {
            reader,
            limits,
            abort,
            metrics,
            ..
        } = self;
        let stop = AbortFlag::new();
        let adapter = Adapter::new(
            sink,
            &limits,
            abort.clone(),
            metrics,
            &Prune::Never,
            stop.clone(),
        );
        let shared = Arc::new(Mutex::new(adapter));
        let mut parser = tabnas_json::make();
        Adapter::install(
            &mut parser,
            Arc::downgrade(&shared),
            abort.clone(),
            stop.clone(),
        );
        let mut lines = Lines::new(reader, limits.max_record_bytes);
        let mut outcome = rule_events::lock(&shared).send(JsonEvent::ArrayStart);
        if outcome.as_ref().is_ok_and(|flow| *flow == Flow::Continue) {
            outcome = loop {
                let (number, line) = match lines.next_line() {
                    Ok(Some(next)) => next,
                    Ok(None) => break Ok(Flow::Continue),
                    Err(fail) => break Err(fail),
                };
                if line.trim().is_empty() {
                    continue;
                }
                let parsed = parser.parse(line);
                let mut adapter = rule_events::lock(&shared);
                match adapter.status() {
                    Status::Running => {}
                    Status::Stopped => break Ok(Flow::Stop),
                    // Reported from the adapter's own status below.
                    Status::Failed(_) => break Ok(Flow::Continue),
                }
                match parsed {
                    Ok(_) if adapter.complete() => adapter.reset(),
                    Ok(_) => break Err(rule_events::not_streamable()),
                    Err(e) => break Err(line_failure(&e, number, &abort)),
                }
            };
        }
        drop(parser);
        let mut adapter = rule_events::take(shared);
        if let (Ok(Flow::Continue), Status::Running) = (&outcome, adapter.status()) {
            outcome = adapter
                .send(JsonEvent::ArrayEnd)
                .and_then(|flow| match flow {
                    Flow::Continue => adapter.send(JsonEvent::End),
                    Flow::Stop => Ok(Flow::Stop),
                });
        }
        let (status, sink) = adapter.finish();
        let outcome = match status {
            Status::Failed(fail) => Err(fail),
            Status::Stopped => Ok(Flow::Stop),
            Status::Running => outcome,
        };
        (outcome, sink)
    }
}

impl<R: BufRead> Source for LinesSource<R> {
    /// Both formats through the walk; JSON Lines numbers carry no lexeme
    /// on this path (see [`LinesSource::run_owned`]).
    fn run(self, sink: &mut dyn Sink) -> Result<Flow, Fail> {
        let mut guarded =
            Guarded::new(sink, &self.limits, self.abort.clone(), self.metrics.clone());
        let outcome = self.drive(&mut guarded);
        guarded.flush();
        outcome
    }
}

fn install_guard(parser: &mut Tabnas, abort: &AbortFlag) {
    let flag = abort.clone();
    parser.parse_guard(GUARD, move |_ctx| !flag.is_aborted());
}

/// An engine error on one line: the line's number is the row.
fn line_failure(error: &tabnas::TabnasError, line: u64, abort: &AbortFlag) -> Fail {
    if error.code == "cancel" && abort.is_aborted() {
        return Fail::aborted();
    }
    let mut fail = Fail::from_tabnas(error);
    fail.row = Some(line);
    fail.column = Some(error.col as u64);
    fail
}

/// The records of a parsed CSV chunk: the elements of its array.
fn records_of(value: &tabnas::Value) -> &[tabnas::Value] {
    match value {
        tabnas::Value::Array(items) => items,
        tabnas::Value::ListRef(list) => &list.value,
        // The grammar returns an array; anything else has no records.
        _ => &[],
    }
}

/// Lines from a reader, numbered from 1, each including its newline, with
/// one buffer reused throughout.
struct Lines<R: BufRead> {
    reader: R,
    buf: Vec<u8>,
    number: u64,
    max_bytes: usize,
}

impl<R: BufRead> Lines<R> {
    fn new(reader: R, max_bytes: usize) -> Lines<R> {
        Lines {
            reader,
            buf: Vec::new(),
            number: 0,
            max_bytes,
        }
    }

    /// The next line without its line ending, or `None` at the end.
    fn next_line(&mut self) -> Result<Option<(u64, &str)>, Fail> {
        match self.next_raw()? {
            None => Ok(None),
            Some((number, raw)) => {
                let end = raw.len()
                    - usize::from(raw.ends_with(b"\n"))
                    - usize::from(raw.ends_with(b"\r\n"));
                let text = std::str::from_utf8(&raw[..end]).map_err(|e| {
                    Fail::input(format!("line {number} is not UTF-8: {e}"))
                        .at(number, e.valid_up_to() as u64 + 1)
                })?;
                Ok(Some((number, text)))
            }
        }
    }

    /// The next line with its line ending, as bytes.
    fn next_raw(&mut self) -> Result<Option<(u64, &[u8])>, Fail> {
        self.buf.clear();
        let read = self
            .reader
            .read_until(b'\n', &mut self.buf)
            .map_err(|e| Fail::input(format!("reading line {}: {e}", self.number + 1)))?;
        if read == 0 {
            return Ok(None);
        }
        self.number += 1;
        if self.buf.len() > self.max_bytes {
            return Err(Fail::limit(
                "max_record_bytes",
                self.max_bytes as u64,
                format!(
                    "line {} is {} bytes, longer than {}",
                    self.number,
                    self.buf.len(),
                    self.max_bytes
                ),
            )
            .at(self.number, 1));
        }
        Ok(Some((self.number, &self.buf)))
    }
}

/// One CSV chunk ready to parse: whole records, the header prepended when
/// it is not the chunk that carried it.
struct Chunk {
    text: String,
    /// The file line the chunk's first record came from.
    first_line: u64,
    /// Lines in `text` before the first record: 1 when the header is in
    /// the text (the first chunk read it, a later one had it prepended),
    /// else 0.
    prefix_lines: u64,
}

impl Chunk {
    /// An engine error inside the chunk, at the file's line.
    fn failure(&self, error: &tabnas::TabnasError, abort: &AbortFlag) -> Fail {
        let row_in_chunk = (error.row as u64).max(1);
        let line = if row_in_chunk > self.prefix_lines {
            self.first_line + row_in_chunk - self.prefix_lines - 1
        } else {
            // The error is in the prepended header itself.
            1
        };
        line_failure(error, line, abort)
    }
}

/// Cuts a CSV reader into chunks of whole records.
struct Chunks<R: BufRead> {
    lines: Lines<R>,
    header: Option<String>,
    want_header: bool,
    chunk_bytes: usize,
    /// Bytes of the current record so far; a record is never cut.
    max_record_bytes: usize,
    done: bool,
}

impl<R: BufRead> Chunks<R> {
    fn new(lines: Lines<R>, header: bool, chunk_bytes: usize) -> Chunks<R> {
        let max_record_bytes = lines.max_bytes;
        Chunks {
            lines,
            header: None,
            want_header: header,
            chunk_bytes,
            max_record_bytes,
            done: false,
        }
    }

    /// Read one whole record (lines until the quotes balance) into `text`.
    /// Returns the record's first line number, or `None` at the end.
    fn read_record(&mut self, text: &mut String) -> Result<Option<u64>, Fail> {
        let start = text.len();
        let mut first_line = None;
        let mut in_quotes = false;
        loop {
            let Some((number, raw)) = self.lines.next_raw()? else {
                // An unterminated quote at the end of the input stays in
                // the text for the parser to report as the grammar does.
                return Ok(first_line);
            };
            first_line.get_or_insert(number);
            let line = std::str::from_utf8(raw).map_err(|e| {
                Fail::input(format!("line {number} is not UTF-8: {e}"))
                    .at(number, e.valid_up_to() as u64 + 1)
            })?;
            for c in line.bytes() {
                if c == b'"' {
                    in_quotes = !in_quotes;
                }
            }
            text.push_str(line);
            if text.len() - start > self.max_record_bytes {
                return Err(Fail::limit(
                    "max_record_bytes",
                    self.max_record_bytes as u64,
                    format!(
                        "the record starting at line {} is longer than {} bytes",
                        first_line.unwrap_or(number),
                        self.max_record_bytes
                    ),
                )
                .at(first_line.unwrap_or(number), 1));
            }
            if !in_quotes {
                return Ok(first_line);
            }
        }
    }

    fn next_chunk(&mut self) -> Result<Option<Chunk>, Fail> {
        if self.done {
            return Ok(None);
        }
        let mut text = String::new();
        let mut prefix_lines = 0;
        if self.want_header && self.header.is_none() {
            // The first chunk carries the header as its own first record.
            match self.read_record(&mut text)? {
                None => {
                    self.done = true;
                    return Ok(None);
                }
                Some(_) => {
                    self.header = Some(text.clone());
                    prefix_lines = 1;
                }
            }
        } else if let Some(header) = &self.header {
            text.push_str(header);
            prefix_lines = 1;
        }
        let mut first_line = None;
        loop {
            match self.read_record(&mut text)? {
                None => {
                    self.done = true;
                    break;
                }
                Some(line) => {
                    first_line.get_or_insert(line);
                    if text.len() >= self.chunk_bytes {
                        break;
                    }
                }
            }
        }
        let Some(first_line) = first_line else {
            // Nothing but the header (or nothing at all) was left: a
            // header-only file is the grammar's empty table, and a chunk
            // of just the prepended header has no records to emit.
            return Ok(None);
        };
        Ok(Some(Chunk {
            text,
            first_line,
            prefix_lines,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Code;
    use crate::event::OwnedJsonEvent;
    use crate::sink::FnSink;
    use crate::source::ValueSource;
    use std::io::{self, Cursor, Read};

    /// A reader that hands out at most `step` bytes per fill, so every
    /// buffer boundary the line reader could meet is met.
    struct Trickle {
        data: Vec<u8>,
        pos: usize,
        step: usize,
    }

    impl Read for Trickle {
        fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
            let n = self.step.min(out.len()).min(self.data.len() - self.pos);
            out[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
            self.pos += n;
            Ok(n)
        }
    }

    fn trickle(data: &str, step: usize) -> io::BufReader<Trickle> {
        io::BufReader::with_capacity(
            step.max(1),
            Trickle {
                data: data.as_bytes().to_vec(),
                pos: 0,
                step: step.max(1),
            },
        )
    }

    fn without_lexemes(events: &[OwnedJsonEvent]) -> Vec<OwnedJsonEvent> {
        events
            .iter()
            .map(|e| match e {
                OwnedJsonEvent::Number { value, .. } => OwnedJsonEvent::Number {
                    value: *value,
                    lexeme: None,
                },
                other => other.clone(),
            })
            .collect()
    }

    fn walked(value: &tabnas::Value) -> Vec<OwnedJsonEvent> {
        let mut rec = Vec::new();
        ValueSource(value).run(&mut rec).unwrap();
        rec
    }

    const JSONL: &str = "{\"a\":1.50,\"b\":[true,null]}\r\n\n  \n{\"a\":2,\"b\":\"x\"}\n[3]\n\"s\"";

    #[test]
    fn jsonl_matches_the_whole_file_parse_at_every_reader_boundary() {
        let want = walked(&tabnas_jsonl::parse(JSONL).unwrap());
        for step in 1..=JSONL.len() {
            let mut rec: Vec<OwnedJsonEvent> = Vec::new();
            LinesSource::new(trickle(JSONL, step), LineFormat::Jsonl)
                .run(&mut rec)
                .unwrap();
            assert_eq!(rec, want, "borrowed, step {step}");

            let (r, rec) = LinesSource::new(trickle(JSONL, step), LineFormat::Jsonl)
                .run_owned(Vec::<OwnedJsonEvent>::new());
            assert_eq!(r.unwrap(), Flow::Continue);
            assert_eq!(without_lexemes(&rec), want, "owned, step {step}");
            assert!(
                rec.contains(&OwnedJsonEvent::Number {
                    value: 1.5,
                    lexeme: Some("1.50".into())
                }),
                "the owned path keeps lexemes"
            );
        }
    }

    #[test]
    fn a_bad_jsonl_line_names_its_line_number() {
        let text = "{\"a\":1}\n\n{\"a\": }\n{\"a\":2}\n";
        let mut rec: Vec<OwnedJsonEvent> = Vec::new();
        let err = LinesSource::new(Cursor::new(text), LineFormat::Jsonl)
            .run(&mut rec)
            .unwrap_err();
        assert_eq!(err.code, Code::InputInvalid);
        assert_eq!(err.row, Some(3));
        assert_eq!(err.column, Some(7));
        let (r, _) = LinesSource::new(Cursor::new(text), LineFormat::Jsonl)
            .run_owned(Vec::<OwnedJsonEvent>::new());
        let err = r.unwrap_err();
        assert_eq!(err.code, Code::InputInvalid);
        assert_eq!(err.row, Some(3));
    }

    #[test]
    fn empty_input_is_an_empty_array_for_both_formats() {
        for format in [LineFormat::Jsonl, LineFormat::csv()] {
            let mut rec: Vec<OwnedJsonEvent> = Vec::new();
            LinesSource::new(Cursor::new(""), format.clone())
                .run(&mut rec)
                .unwrap();
            assert_eq!(
                rec,
                vec![
                    OwnedJsonEvent::ArrayStart,
                    OwnedJsonEvent::ArrayEnd,
                    OwnedJsonEvent::End
                ]
            );
            let (r, rec) = LinesSource::new(Cursor::new("\n\n"), format)
                .run_owned(Vec::<OwnedJsonEvent>::new());
            r.unwrap();
            assert_eq!(rec.len(), 3);
        }
    }

    const CSV: &str =
        "id,note,n\r\n1,\"multi\r\nline, with \"\"quotes\"\"\",2.50\r\n3,plain,4\r\n\r\n5,\"a\",6";

    #[test]
    fn csv_matches_the_whole_file_parse_at_every_chunk_size_and_reader_boundary() {
        let want = walked(&tabnas_csv::parse(CSV).unwrap());
        assert_eq!(
            want.iter()
                .filter(|e| matches!(e, OwnedJsonEvent::ObjectStart))
                .count(),
            3
        );
        for chunk_bytes in 0..=CSV.len() + 1 {
            let mut rec: Vec<OwnedJsonEvent> = Vec::new();
            LinesSource::new(Cursor::new(CSV), LineFormat::csv())
                .chunk_bytes(chunk_bytes)
                .run(&mut rec)
                .unwrap();
            assert_eq!(rec, want, "chunk {chunk_bytes}");
        }
        for step in 1..=CSV.len() {
            let (r, rec) = LinesSource::new(trickle(CSV, step), LineFormat::csv())
                .chunk_bytes(7)
                .run_owned(Vec::<OwnedJsonEvent>::new());
            r.unwrap();
            assert_eq!(rec, want, "step {step}");
        }
    }

    #[test]
    fn csv_without_a_header_yields_the_grammars_records() {
        let text = "1,2\n3,\"4\n5\"\n";
        for object in [true, false] {
            let options = CsvOptions {
                header: false,
                object,
                ..CsvOptions::default()
            };
            let want = walked(&tabnas_csv::make_with(options.clone()).parse(text).unwrap());
            for chunk_bytes in 0..=text.len() {
                let mut rec: Vec<OwnedJsonEvent> = Vec::new();
                LinesSource::new(
                    Cursor::new(text),
                    LineFormat::csv_with(false, options.clone()),
                )
                .chunk_bytes(chunk_bytes)
                .run(&mut rec)
                .unwrap();
                assert_eq!(rec, want, "object {object}, chunk {chunk_bytes}");
            }
        }
    }

    #[test]
    fn a_header_only_file_is_an_empty_table() {
        let mut rec: Vec<OwnedJsonEvent> = Vec::new();
        LinesSource::new(Cursor::new("a,b\n"), LineFormat::csv())
            .run(&mut rec)
            .unwrap();
        assert_eq!(rec.len(), 3);
    }

    #[test]
    fn a_bad_csv_record_names_its_file_line() {
        let text = "a,b\n1,2\n3,\"x\n4,5\n";
        let mut rec: Vec<OwnedJsonEvent> = Vec::new();
        let err = LinesSource::new(Cursor::new(text), LineFormat::csv())
            .chunk_bytes(0)
            .run(&mut rec)
            .unwrap_err();
        assert_eq!(err.code, Code::InputInvalid);
        assert_eq!(err.row, Some(3), "{err}");
        let err = LinesSource::new(Cursor::new(text), LineFormat::csv())
            .run(&mut Vec::<OwnedJsonEvent>::new())
            .unwrap_err();
        assert_eq!(err.row, Some(3), "{err}");
    }

    #[test]
    fn an_oversized_record_names_max_record_bytes() {
        let limits = Limits {
            max_record_bytes: 8,
            ..Limits::default()
        };
        let err = LinesSource::new(
            Cursor::new("{\"a\":1}\n{\"a\":123456}\n"),
            LineFormat::Jsonl,
        )
        .limits(limits.clone())
        .run(&mut Vec::<OwnedJsonEvent>::new())
        .unwrap_err();
        assert_eq!(err.limit.as_ref().unwrap().name, "max_record_bytes");
        assert_eq!(err.row, Some(2));
        let err = LinesSource::new(
            Cursor::new("a\n\"long\nquoted\nfield\"\n"),
            LineFormat::csv(),
        )
        .limits(limits)
        .run(&mut Vec::<OwnedJsonEvent>::new())
        .unwrap_err();
        assert_eq!(err.limit.as_ref().unwrap().name, "max_record_bytes");
        assert_eq!(err.row, Some(2));
    }

    #[test]
    fn a_stop_and_an_abort_end_the_run_on_both_paths() {
        let text = "{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n";
        let stopper = || {
            let mut n = 0;
            FnSink(move |_ev: JsonEvent<'_>| {
                n += 1;
                Ok(if n == 4 { Flow::Stop } else { Flow::Continue })
            })
        };
        let mut sink = stopper();
        assert_eq!(
            LinesSource::new(Cursor::new(text), LineFormat::Jsonl)
                .run(&mut sink)
                .unwrap(),
            Flow::Stop
        );
        let (r, _) = LinesSource::new(Cursor::new(text), LineFormat::Jsonl).run_owned(stopper());
        assert_eq!(r.unwrap(), Flow::Stop);

        let abort = AbortFlag::new();
        abort.abort();
        let err = LinesSource::new(Cursor::new(text), LineFormat::Jsonl)
            .abort(abort.clone())
            .run(&mut Vec::<OwnedJsonEvent>::new())
            .unwrap_err();
        assert_eq!(err.code, Code::Aborted);
        let (r, _) = LinesSource::new(Cursor::new(text), LineFormat::Jsonl)
            .abort(abort)
            .run_owned(Vec::<OwnedJsonEvent>::new());
        assert_eq!(r.unwrap_err().code, Code::Aborted);
    }

    #[test]
    fn invalid_utf8_is_invalid_input_at_its_line() {
        let bytes: &[u8] = b"{\"a\":1}\n{\"a\":\"\xff\"}\n";
        let err = LinesSource::new(Cursor::new(bytes), LineFormat::Jsonl)
            .run(&mut Vec::<OwnedJsonEvent>::new())
            .unwrap_err();
        assert_eq!(err.code, Code::InputInvalid);
        assert_eq!(err.row, Some(2));
    }
}
