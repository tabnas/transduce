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
//! Both formats cut the input where the grammar ends a record, reading
//! the text as the grammar's own lexer does (with the line characters,
//! quotes, separators and comments its parser's resolved options set), and
//! nothing read is left unparsed but a blank JSON Lines record, which the
//! grammar skips too.
//!
//! JSON Lines: each record is parsed with one `tabnas-json` parser. A
//! record is the text between line characters outside a string, so a lone
//! `\r` ends one as `\n` does. A record of nothing but the grammar's
//! spaces (a space or a tab) is blank and skipped; any other is parsed, so
//! a line holding a form feed or a no-break space fails as the grammar
//! fails it. With an owned sink ([`LinesSource::run_owned`]) the record
//! goes through the rule-event adapter, reset per record, so numbers keep
//! their lexemes; with a borrowed sink ([`Source::run`]) the value is
//! walked and numbers carry none. A record that does not parse is
//! `INPUT_INVALID` with its line's number as the row.
//!
//! CSV: the input is cut into chunks of whole records. A quote opens a
//! quoted field only where the lexer starts a token (a record's start,
//! after the field separator or a space), so a quote inside a field is
//! that field's text; the configured quote reads `""` as one quote; a
//! quoted field, a backtick string and a block comment may span lines; and
//! a line character anywhere else, a lone `\r` as much as `\n`, ends a
//! record. The
//! header record, the first record the grammar reads (a blank or comment
//! line before it is none), is kept and prepended to every chunk after the
//! first when `header` is on, so the reused `tabnas-csv` parser names each
//! record's fields as the whole file would. A chunk closes at the first
//! record boundary past [`DEFAULT_CHUNK_BYTES`] (or the configured size),
//! so one chunk, and never a fraction of a record, is what a parse holds.
//! An input that ends inside a quoted field fails with the grammar's
//! `unterminated_string`, in the header as anywhere.
//!
//! The reader is taken a piece at a time, and a piece ends just past each
//! of the grammar's line endings: one of its line characters (a lone `\r`
//! or a configured separator as much as `\n`), a `\r\n`, or under
//! `record.empty` the whole line token the lexer reads. So a record never
//! waits for a `\n` to end, and a chunk can close after any record,
//! however many of them one `\n`-terminated line holds.
//!
//! Memory is bounded by one chunk, and a record is never split, so a
//! single record larger than `max_record_bytes` (a record with its line
//! ending; for the CSV header, everything up to its end) fails with that
//! limit's name rather than growing a chunk without bound.
//! The bound holds while the record is READ, not only once it is whole: a
//! record is taken from the reader a buffer at a time and refused the
//! moment it passes the limit, so an unterminated record of any length
//! costs one buffer beyond the limit and no more. `max_depth`,
//! `max_key_bytes` and `max_scalar_bytes` apply to the events as
//! everywhere. `max_record_bytes` counts the record's source bytes here,
//! where a table transducer downstream would count its retained bytes; it
//! is the same idea of "one row" measured before it is parsed.

use std::io::{self, BufRead};
use std::sync::{Arc, Mutex};

use tabnas::Tabnas;
use tabnas_csv::CsvOptions;

use crate::error::{Code, Fail};
use crate::event::JsonEvent;
use crate::limits::{AbortFlag, Limits, Metrics};
use crate::sink::{Flow, Sink};
use crate::source::guard::Guarded;
use crate::source::rule_events::{self, Adapter, Status, GUARD};
use crate::source::{engine_failure, walk_value, Prune, Source};

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
                let mut records = JsonRecords::new(reader, &parser, limits.max_record_bytes);
                if guarded.event(JsonEvent::ArrayStart)? == Flow::Stop {
                    return Ok(Flow::Stop);
                }
                while let Some((number, record)) = records.next_record()? {
                    let value = parser
                        .parse(record)
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
                let mut parser = tabnas_csv::make_with(options.clone());
                install_guard(&mut parser, &abort);
                let mut chunks = Chunks::new(
                    reader,
                    &parser,
                    &options,
                    chunk_bytes,
                    limits.max_record_bytes,
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
        let mut records = JsonRecords::new(reader, &parser, limits.max_record_bytes);
        let mut outcome = rule_events::lock(&shared).send(JsonEvent::ArrayStart);
        if outcome.as_ref().is_ok_and(|flow| *flow == Flow::Continue) {
            outcome = loop {
                let (number, record) = match records.next_record() {
                    Ok(Some(next)) => next,
                    Ok(None) => break Ok(Flow::Continue),
                    Err(fail) => break Err(fail),
                };
                let parsed = parser.parse(record);
                let mut adapter = rule_events::lock(&shared);
                match adapter.status() {
                    Status::Running => {}
                    Status::Stopped => break Ok(Flow::Stop),
                    // Reported from the adapter's own status below.
                    Status::Failed(_) => break Ok(Flow::Continue),
                }
                match parsed {
                    Ok(_) if adapter.complete() => adapter.reset(),
                    Ok(value) if adapter.idle() => match adapter.walk_whole(&value) {
                        Ok(Flow::Continue) => adapter.reset(),
                        Ok(Flow::Stop) => break Ok(Flow::Stop),
                        Err(fail) => break Err(fail),
                    },
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
    let mut fail = engine_failure(error, abort);
    if fail.code != Code::Aborted {
        fail.row = Some(line);
        fail.column = Some(error.col as u64);
    }
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

/// What the JSON Lines grammar's lexer makes of a line, read from the line
/// parser's resolved options (the grammar is the JSON one with the line
/// token made significant): a line character outside a string ends a
/// record, and a record of nothing but space is blank. Each set is empty
/// while the engine does not lex its kind.
struct JsonLexis {
    line: Vec<char>,
    space: Vec<char>,
    quotes: Vec<char>,
    escape: char,
}

impl JsonLexis {
    fn of(parser: &Tabnas) -> JsonLexis {
        let config = parser.config();
        let mut line = lexed(config.line.lex, &config.line.chars);
        if config.line.lex {
            line.extend(&config.line.fixed);
        }
        JsonLexis {
            line,
            space: lexed(config.space.lex, &config.space.chars),
            quotes: lexed(config.string.lex, &config.string.chars),
            escape: config.string.escape_char,
        }
    }
}

/// JSON Lines records from the input's pieces. A record ends at a line
/// character outside a string, as the grammar's lexer ends one: a line
/// character inside a string is the string's, which the grammar refuses
/// there, so the record goes on into the next piece and fails whole as the
/// grammar fails it. A record of nothing but the grammar's spaces is blank
/// and skipped.
struct JsonRecords<R: BufRead> {
    pieces: Pieces<R>,
    lexis: JsonLexis,
    record: String,
    max_bytes: usize,
}

impl<R: BufRead> JsonRecords<R> {
    fn new(reader: R, parser: &Tabnas, max_bytes: usize) -> JsonRecords<R> {
        let lexis = JsonLexis::of(parser);
        let config = parser.config();
        let pieces = Pieces::new(
            reader,
            lexis.line.clone(),
            config.line.row_chars.chars().collect(),
            config.line.single,
        );
        JsonRecords {
            pieces,
            lexis,
            record: String::new(),
            max_bytes,
        }
    }

    /// The next record that is not blank, without its line ending, and
    /// the row it starts on; `None` at the end of the input. A record
    /// with its line ending longer than `max_bytes` fails with that limit.
    fn next_record(&mut self) -> Result<Option<(u64, &str)>, Fail> {
        let (start, end) = loop {
            self.record.clear();
            let mut start = None;
            let mut quote = None;
            let mut escaped = false;
            let mut end = None;
            while end.is_none() {
                let first = start.unwrap_or(self.pieces.row);
                let max = self.max_bytes;
                let over = || {
                    Fail::limit(
                        "max_record_bytes",
                        max as u64,
                        format!("line {first} is longer than {max} bytes"),
                    )
                    .at(first, 1)
                };
                let budget = max.saturating_sub(self.record.len());
                let Some((row, piece)) = self.pieces.next_piece(budget, over)? else {
                    break;
                };
                start.get_or_insert(row);
                let held = self.record.len();
                for (at, c) in piece.char_indices() {
                    if let Some(open) = quote {
                        if escaped {
                            escaped = false;
                        } else if c == self.lexis.escape {
                            escaped = true;
                        } else if c == open {
                            quote = None;
                        }
                    } else if self.lexis.quotes.contains(&c) {
                        quote = Some(c);
                    } else if self.lexis.line.contains(&c) {
                        end = Some(held + at);
                        break;
                    }
                }
                self.record.push_str(piece);
            }
            let Some(start) = start else {
                return Ok(None);
            };
            let end = end.unwrap_or(self.record.len());
            if !self.record[..end]
                .chars()
                .all(|c| self.lexis.space.contains(&c))
            {
                break (start, end);
            }
        };
        Ok(Some((start, &self.record[..end])))
    }
}

/// The characters of `chars` while the engine lexes their kind, else none.
fn lexed(on: bool, chars: &str) -> Vec<char> {
    if on {
        chars.chars().collect()
    } else {
        Vec::new()
    }
}

/// The input from a reader in pieces, each ending just past a line
/// ending: one of the grammar's line characters, a `\r\n`, or under
/// `line.single` (`record.empty`) the whole line token the lexer reads,
/// every line character up to a repeated one. A record therefore never
/// ends inside a piece, only at its end. Each piece carries the row the
/// engine gives its first character: one more than the row characters
/// before it. One buffer is reused throughout.
struct Pieces<R: BufRead> {
    reader: R,
    buf: Vec<u8>,
    /// Bytes taken from the reader past the last piece, and the start of
    /// the next: part of a character's UTF-8 form, read to see whether it
    /// went on a line token, which it did not.
    carry: Vec<u8>,
    /// The line characters, their UTF-8 forms, and the bytes that end a
    /// form.
    line: Vec<char>,
    forms: Vec<Vec<u8>>,
    ends: [bool; 256],
    single: bool,
    rows: Vec<char>,
    /// Where the next piece starts: its row, and its byte offset in it.
    row: u64,
    offset: u64,
}

impl<R: BufRead> Pieces<R> {
    fn new(reader: R, line: Vec<char>, rows: Vec<char>, single: bool) -> Pieces<R> {
        let forms: Vec<Vec<u8>> = line.iter().map(|c| c.to_string().into_bytes()).collect();
        let mut ends = [false; 256];
        for form in &forms {
            ends[usize::from(form[form.len() - 1])] = true;
        }
        Pieces {
            reader,
            buf: Vec::new(),
            carry: Vec::new(),
            line,
            forms,
            ends,
            single,
            rows,
            row: 1,
            offset: 0,
        }
    }

    /// The next piece and the row it starts on, or `None` at the end of
    /// the input. It is taken from the reader a buffer at a time and
    /// refused with `over`'s failure the moment it passes `budget` bytes,
    /// so a piece without an end costs one buffer past the budget and no
    /// more.
    fn next_piece(
        &mut self,
        budget: usize,
        over: impl Fn() -> Fail,
    ) -> Result<Option<(u64, &str)>, Fail> {
        self.buf.clear();
        self.buf.append(&mut self.carry);
        let row = self.row;
        let mut ended = None;
        loop {
            let available = match self.reader.fill_buf() {
                Ok(available) => available,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(Fail::input(format!("reading line {row}: {e}"))),
            };
            if available.is_empty() {
                break;
            }
            // The first byte that ends a line character's form, checked
            // against the whole form, which may begin in what was taken.
            let mut wanted = available.len();
            let mut from = 0;
            while let Some(at) = available[from..]
                .iter()
                .position(|&b| self.ends[usize::from(b)])
            {
                let at = from + at;
                if let Some(k) = form_ending(&self.forms, &self.buf, &available[..=at]) {
                    ended = Some(k);
                    wanted = at + 1;
                    break;
                }
                from = at + 1;
            }
            let room = budget.saturating_add(1).saturating_sub(self.buf.len());
            let take = wanted.min(room);
            self.buf.extend_from_slice(&available[..take]);
            self.reader.consume(take);
            if self.buf.len() > budget {
                return Err(over());
            }
            // Under the budget, all that was wanted was taken, so a line
            // character seen is a line character kept.
            if ended.is_some() {
                break;
            }
        }
        if let Some(k) = ended {
            self.follow_token(k, budget, &over)?;
        }
        if self.buf.is_empty() {
            return Ok(None);
        }
        let piece = match std::str::from_utf8(&self.buf) {
            Ok(piece) => piece,
            Err(e) => {
                let valid = std::str::from_utf8(&self.buf[..e.valid_up_to()]).unwrap_or_default();
                let (row, offset) = advance(&self.rows, self.row, self.offset, valid);
                return Err(Fail::input(format!(
                    "line {row} is not UTF-8 from its byte {}",
                    offset + 1
                ))
                .at(row, offset + 1));
            }
        };
        (self.row, self.offset) = advance(&self.rows, self.row, self.offset, piece);
        Ok(Some((row, piece)))
    }

    /// Takes the rest of the line token that the piece's last character,
    /// the `k`th line character, began: under `line.single`, every line
    /// character not yet in it, as the lexer reads one; otherwise a `\n`
    /// after a `\r`, so that `\r\n` is one line ending.
    fn follow_token(
        &mut self,
        k: usize,
        budget: usize,
        over: &impl Fn() -> Fail,
    ) -> Result<(), Fail> {
        let mut token = vec![self.line[k]];
        loop {
            let goes_on = |c: char| {
                if self.single {
                    !token.contains(&c)
                } else {
                    token == ['\r'] && c == '\n'
                }
            };
            let wanted: Vec<usize> = (0..self.line.len())
                .filter(|&i| goes_on(self.line[i]))
                .collect();
            if wanted.is_empty() {
                return Ok(());
            }
            let available = loop {
                match self.reader.fill_buf() {
                    Ok(available) => break available,
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                    Err(e) => return Err(Fail::input(format!("reading line {}: {e}", self.row))),
                }
            };
            // The next character, with any part of it already carried.
            let mut next = None;
            let mut partial = false;
            for &i in &wanted {
                let form = &self.forms[i];
                let (head, tail) = form.split_at(self.carry.len().min(form.len()));
                if self.carry.len() + available.len() >= form.len() {
                    if self.carry == head && available.starts_with(tail) {
                        next = Some(i);
                        break;
                    }
                } else if self.carry == head && tail.starts_with(available) {
                    partial = true;
                }
            }
            match next {
                Some(i) => {
                    let rest = self.forms[i].len() - self.carry.len();
                    if self.buf.len() + self.forms[i].len() > budget {
                        return Err(over());
                    }
                    self.buf.append(&mut self.carry);
                    self.buf.extend_from_slice(&available[..rest]);
                    self.reader.consume(rest);
                    token.push(self.line[i]);
                }
                // The buffer ends inside what may be a line character:
                // carry it, and read on.
                None if partial && !available.is_empty() => {
                    let n = available.len();
                    self.carry.extend_from_slice(available);
                    self.reader.consume(n);
                }
                None => return Ok(()),
            }
        }
    }
}

/// The line character whose UTF-8 form `head` followed by `tail` ends with,
/// by its index.
fn form_ending(forms: &[Vec<u8>], head: &[u8], tail: &[u8]) -> Option<usize> {
    forms.iter().position(|form| {
        if tail.len() >= form.len() {
            tail.ends_with(form)
        } else {
            let need = form.len() - tail.len();
            head.len() >= need && head.ends_with(&form[..need]) && form[need..] == *tail
        }
    })
}

/// The row, and the byte offset in it, just after `text`, which starts at
/// `row` and `offset`: a row character starts the next row.
fn advance(rows: &[char], row: u64, offset: u64, text: &str) -> (u64, u64) {
    let mut count = 0;
    let mut last = None;
    for (at, c) in text.char_indices() {
        if rows.contains(&c) {
            count += 1;
            last = Some(at + c.len_utf8());
        }
    }
    match last {
        Some(end) => (row + count, (text.len() - end) as u64),
        None => (row, offset + text.len() as u64),
    }
}

/// One CSV chunk ready to parse: whole records, the header prepended when
/// it is not the chunk that carried it.
struct Chunk {
    text: String,
    /// The file line the chunk's own text, after `prefix_lines`, starts on.
    first_line: u64,
    /// Lines in `text` before the chunk's own: those of the prepended
    /// header in a chunk after the first, and none in the first, which is
    /// the file's text from its start.
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

/// Cuts a CSV reader into chunks of whole records, where the grammar ends
/// them.
struct Chunks<R: BufRead> {
    pieces: Pieces<R>,
    scanner: Scanner,
    /// Whether the first record names the fields, and whether a blank line
    /// is a record (`record.empty`): together, which record the header is.
    want_header: bool,
    record_empty: bool,
    /// The header record as the grammar reads it, once read: every chunk
    /// after the first starts with it.
    header: Option<String>,
    chunk_bytes: usize,
    max_record_bytes: usize,
    started: bool,
    done: bool,
}

impl<R: BufRead> Chunks<R> {
    fn new(
        reader: R,
        parser: &Tabnas,
        options: &CsvOptions,
        chunk_bytes: usize,
        max_record_bytes: usize,
    ) -> Chunks<R> {
        let lexis = Lexis::csv(parser, options);
        let config = parser.config();
        let pieces = Pieces::new(
            reader,
            lexis.line.clone(),
            config.line.row_chars.chars().collect(),
            config.line.single,
        );
        Chunks {
            pieces,
            scanner: Scanner::new(lexis),
            want_header: options.header,
            record_empty: options.record.empty,
            header: None,
            chunk_bytes,
            max_record_bytes,
            started: false,
            done: false,
        }
    }

    /// The next chunk, or `None` once nothing is left to read. Every piece
    /// read goes into a chunk, the first one's header included, so every
    /// byte of the input is parsed.
    fn next_chunk(&mut self) -> Result<Option<Chunk>, Fail> {
        if self.done {
            return Ok(None);
        }
        let mut text = String::new();
        if self.started {
            text.push_str(self.header.as_deref().unwrap_or_default());
        }
        self.started = true;
        let prefix_lines = advance(&self.pieces.rows, 0, 0, &text).0;
        // The chunk closes only past `body`: past the header in front of
        // it, or in the first chunk past the header record itself.
        let mut body = text.len();
        let mut first_line = None;
        // The text a chunk cannot close inside, from its offset and row:
        // one record, or before the header everything up to its end.
        let mut open = (text.len(), None);
        // Where the last record ended, which is where the next one starts.
        let mut last_end = 0;
        loop {
            let first = open.1.unwrap_or(self.pieces.row);
            let max = self.max_record_bytes;
            let over = || {
                Fail::limit(
                    "max_record_bytes",
                    max as u64,
                    format!("the record starting at line {first} is longer than {max} bytes"),
                )
                .at(first, 1)
            };
            let budget = max.saturating_sub(text.len() - open.0);
            let Some((number, piece)) = self.pieces.next_piece(budget, over)? else {
                self.done = true;
                break;
            };
            first_line.get_or_insert(number);
            open.1.get_or_insert(number);
            let at = text.len();
            text.push_str(piece);
            let seeking = self.want_header && self.header.is_none();
            let record_empty = self.record_empty;
            let mut found = None;
            let ended = self.scanner.piece(piece, |end, content| {
                if seeking && found.is_none() && (content || record_empty) {
                    found = Some((last_end, at + end));
                }
                last_end = at + end;
            });
            if let Some((from, to)) = found {
                self.header = Some(text[from..to].to_string());
                body = to;
            }
            // A chunk may close where a piece ends a record, once the
            // header is behind it.
            if ended && !(self.want_header && self.header.is_none()) {
                open = (text.len(), None);
                if text.len() > body && text.len() >= self.chunk_bytes {
                    break;
                }
            }
        }
        let Some(first_line) = first_line else {
            // Nothing was left to read.
            return Ok(None);
        };
        Ok(Some(Chunk {
            text,
            first_line,
            prefix_lines,
        }))
    }
}

/// What decides where the CSV grammar ends a record, read from its
/// parser's resolved options and from the options the plugin builds its
/// quote matcher from, so a separator, quote, record separator or comment
/// setting moves the chunker's cut as it moves the grammar. Each set is
/// empty while the engine does not lex its kind.
struct Lexis {
    /// Line characters: outside a token, each ends a record.
    line: Vec<char>,
    /// Whether a run of line characters stops at a repeated one
    /// (`record.empty`), so that `\n\n` is two line tokens.
    single: bool,
    space: Vec<char>,
    /// The fixed tokens: the field separator, and outside strict mode the
    /// JSON structure characters.
    fixed: Vec<String>,
    /// The RFC 4180 quote, while the grammar's own matcher reads it: a
    /// doubled one inside is one quote, and a line character is text.
    quote: Option<char>,
    /// The engine's own string quotes, after that one: an `escape` takes
    /// the next character whatever it is, and a line character is text
    /// only inside the `multi` ones (a backtick).
    strings: Vec<char>,
    multi: Vec<char>,
    escape: char,
    /// The comment markers, longest first, each with its end: `None` for a
    /// line comment, which a line character ends without being part of.
    comments: Vec<(String, Option<String>)>,
    /// What ends a run of text, as characters and as token starts.
    stops: Vec<char>,
    stop_prefixes: Vec<String>,
    /// Whether the grammar ignores a space (outside strict mode) and a
    /// comment, so that a record of nothing else is blank.
    space_ignored: bool,
    comment_ignored: bool,
}

impl Lexis {
    fn csv(parser: &Tabnas, options: &CsvOptions) -> Lexis {
        let config = parser.config();
        let mut line = lexed(config.line.lex, &config.line.chars);
        if config.line.lex {
            line.extend(&config.line.fixed);
        }
        let space = lexed(config.space.lex, &config.space.chars);
        let fixed: Vec<String> = config
            .fixed
            .tokens
            .values()
            .filter(|token| config.fixed.lex && !token.source.is_empty())
            .map(|token| token.source.clone())
            .collect();
        // The longest marker first, and a tie by name: the engine's order.
        let mut comments: Vec<_> = config
            .comment
            .definitions
            .iter()
            .filter(|(_, comment)| config.comment.lex && comment.lex && !comment.start.is_empty())
            .collect();
        comments.sort_by(|(name, comment), (other_name, other)| {
            other
                .start
                .len()
                .cmp(&comment.start.len())
                .then_with(|| name.cmp(other_name))
        });
        let comments: Vec<(String, Option<String>)> = comments
            .into_iter()
            .map(|(_, comment)| {
                (
                    comment.start.clone(),
                    (!comment.line).then(|| comment.end.clone()),
                )
            })
            .collect();
        // The RFC 4180 matcher as the plugin installs it: on in strict mode
        // unless `string.csv` is false, off otherwise unless it is true, and
        // inert for a quote that is not one UTF-16 code unit.
        let matcher = if options.strict {
            options.string.csv != Some(false)
        } else {
            options.string.csv == Some(true)
        };
        let mut quote = options.string.quote.chars();
        let quote = match (quote.next(), quote.next()) {
            (Some(quote), None) if matcher && quote.len_utf16() == 1 => Some(quote),
            _ => None,
        };
        let ignored = parser.token_set("IGNORE").unwrap_or_default();
        // What the engine's text matcher stops at; it refuses the two
        // Unicode line separators while it lexes lines.
        let mut stops = space.clone();
        stops.extend(&line);
        if config.line.lex {
            stops.extend(['\u{2028}', '\u{2029}']);
        }
        let mut stop_prefixes = fixed.clone();
        stop_prefixes.extend(comments.iter().map(|(start, _)| start.clone()));
        stop_prefixes.extend(config.ender.iter().filter(|e| !e.is_empty()).cloned());
        Lexis {
            line,
            single: config.line.single,
            space,
            fixed,
            quote,
            strings: lexed(config.string.lex, &config.string.chars),
            multi: config.string.multi_chars.chars().collect(),
            escape: config.string.escape_char,
            comments,
            stops,
            stop_prefixes,
            space_ignored: ignored.contains(&tabnas::TIN_SP),
            comment_ignored: ignored.contains(&tabnas::TIN_CM),
        }
    }

    /// The length of the longest fixed token at the head of `rest`.
    fn fixed_at(&self, rest: &str) -> Option<usize> {
        self.fixed
            .iter()
            .filter(|source| rest.starts_with(source.as_str()))
            .map(String::len)
            .max()
    }

    /// The comment that starts at the head of `rest`, by its index.
    fn comment_at(&self, rest: &str) -> Option<usize> {
        self.comments
            .iter()
            .position(|(start, _)| rest.starts_with(start.as_str()))
    }

    /// The length of the line token at the head of `rest`.
    fn line_run(&self, rest: &str) -> usize {
        let mut len = 0;
        for (at, c) in rest.char_indices() {
            if !self.line.contains(&c) || (self.single && rest[..at].contains(c)) {
                break;
            }
            len = at + c.len_utf8();
        }
        len
    }

    /// Whether a run of text stops at the head of `rest`, whose first
    /// character is `c`.
    fn ends_text(&self, rest: &str, c: char) -> bool {
        self.stops.contains(&c)
            || self
                .stop_prefixes
                .iter()
                .any(|prefix| rest.starts_with(prefix.as_str()))
    }
}

/// Where the record scanner is, between two characters.
#[derive(Clone, Copy)]
enum At {
    /// Where the lexer starts a token.
    Start,
    /// Inside text, a number or a keyword: a run that only what `ends_text`
    /// names ends, so a quote inside one is text.
    Text,
    /// Inside an RFC 4180 quoted field.
    Quoted,
    /// Inside one of the engine's own strings.
    Str { quote: char, multi: bool },
    /// Inside a comment, by its index in `Lexis::comments`.
    Comment(usize),
}

/// Follows CSV text through the grammar's tokens, a piece at a time, to
/// find where its records end.
struct Scanner {
    lexis: Lexis,
    at: At,
    /// Whether the record so far holds anything the grammar does not
    /// ignore, so that it is not blank.
    content: bool,
}

impl Scanner {
    fn new(lexis: Lexis) -> Scanner {
        Scanner {
            lexis,
            at: At::Start,
            content: false,
        }
    }

    /// Scan one piece of the input, which ends just past a line ending
    /// unless it is the last, calling `end(offset, content)` where each
    /// record in it ends: `offset` is just past the line token that ends
    /// it, and `content` says whether it held anything the grammar does not
    /// ignore. Returns whether the piece ends a record, so that a chunk may
    /// close after it.
    fn piece(&mut self, line: &str, mut end: impl FnMut(usize, bool)) -> bool {
        let lexis = &self.lexis;
        let mut ended = false;
        let mut i = 0;
        while let Some(c) = line[i..].chars().next() {
            let rest = &line[i..];
            ended = false;
            match self.at {
                // In the lexer's order: the RFC 4180 matcher, fixed tokens,
                // space, lines, strings, comments, and then a run of text.
                At::Start => {
                    if lexis.quote == Some(c) {
                        self.at = At::Quoted;
                        self.content = true;
                        i += c.len_utf8();
                    } else if let Some(len) = lexis.fixed_at(rest) {
                        self.content = true;
                        i += len;
                    } else if lexis.space.contains(&c) {
                        self.content |= !lexis.space_ignored;
                        i += c.len_utf8();
                    } else if lexis.line.contains(&c) {
                        i += lexis.line_run(rest);
                        end(i, self.content);
                        self.content = false;
                        ended = true;
                    } else if lexis.strings.contains(&c) {
                        self.at = At::Str {
                            quote: c,
                            multi: lexis.multi.contains(&c),
                        };
                        self.content = true;
                        i += c.len_utf8();
                    } else if let Some(comment) = lexis.comment_at(rest) {
                        self.at = At::Comment(comment);
                        self.content |= !lexis.comment_ignored;
                        i += lexis.comments[comment].0.len();
                    } else {
                        // A character no token starts with (an ender) is
                        // one the grammar refuses, and lexing resumes after it.
                        if !lexis.ends_text(rest, c) {
                            self.at = At::Text;
                        }
                        self.content = true;
                        i += c.len_utf8();
                    }
                }
                At::Text => {
                    if lexis.ends_text(rest, c) {
                        self.at = At::Start;
                    } else {
                        i += c.len_utf8();
                    }
                }
                At::Quoted => {
                    i += c.len_utf8();
                    if lexis.quote == Some(c) {
                        if line[i..].starts_with(c) {
                            i += c.len_utf8();
                        } else {
                            self.at = At::Start;
                        }
                    }
                }
                At::Str { quote, multi } => {
                    if c == quote {
                        self.at = At::Start;
                        i += c.len_utf8();
                    } else if c == lexis.escape {
                        i += c.len_utf8();
                        i += line[i..].chars().next().map_or(0, char::len_utf8);
                    } else if !multi && lexis.line.contains(&c) {
                        // The grammar refuses the string here, so the line
                        // character is read as a line.
                        self.at = At::Start;
                    } else {
                        i += c.len_utf8();
                    }
                }
                At::Comment(comment) => match &lexis.comments[comment].1 {
                    Some(close) if !close.is_empty() && rest.starts_with(close.as_str()) => {
                        self.at = At::Start;
                        i += close.len();
                    }
                    None if lexis.line.contains(&c) => self.at = At::Start,
                    _ => i += c.len_utf8(),
                },
            }
        }
        ended
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

    /// The JSON Lines source installs the adapter on `tabnas-json` itself,
    /// without the `ParserSource` gate, which is sound only while json is
    /// verified.
    #[test]
    fn the_line_sources_grammar_is_verified() {
        assert!(crate::source::capability::incremental("json"));
    }

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

    /// Counts the bytes handed out, so a test can see how far past the
    /// limit a reader was pulled.
    struct Counting<R> {
        inner: R,
        read: usize,
    }

    impl<R: Read> Read for Counting<R> {
        fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
            let n = self.inner.read(out)?;
            self.read += n;
            Ok(n)
        }
    }

    /// A reader that repeats one byte without end and never a newline.
    struct Endless(u8);

    impl Read for Endless {
        fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
            out.fill(self.0);
            Ok(out.len())
        }
    }

    #[test]
    fn an_unterminated_line_is_refused_at_the_limit_not_after_being_read_whole() {
        const BUFFER: usize = 4096;
        let limits = Limits {
            max_record_bytes: 8,
            ..Limits::default()
        };
        for format in [LineFormat::Jsonl, LineFormat::csv()] {
            let mut reader = io::BufReader::with_capacity(
                BUFFER,
                Counting {
                    inner: Endless(b'a'),
                    read: 0,
                },
            );
            let err = LinesSource::new(&mut reader, format.clone())
                .limits(limits.clone())
                .run(&mut Vec::<OwnedJsonEvent>::new())
                .unwrap_err();
            assert_eq!(err.limit.as_ref().unwrap().name, "max_record_bytes");
            assert_eq!(err.row, Some(1));
            let read = reader.into_inner().read;
            assert!(
                read <= limits.max_record_bytes + BUFFER,
                "{format:?}: {read} bytes were pulled from an endless line"
            );
        }
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

    /// A reading of one text: its events, or its failure as the engine's
    /// code, row and column.
    type Reading = Result<Vec<OwnedJsonEvent>, (String, Option<u64>, Option<u64>)>;

    /// The whole parse's reading, which the line source must reproduce.
    fn whole(parsed: Result<tabnas::Value, tabnas::TabnasError>) -> Reading {
        match parsed {
            Ok(value) => Ok(walked(&value)),
            Err(e) => Err((e.code.to_string(), Some(e.row as u64), Some(e.col as u64))),
        }
    }

    /// The line source's reading. Its failure carries the engine's code at
    /// the head of its message.
    fn reading(outcome: Result<Flow, Fail>, events: Vec<OwnedJsonEvent>) -> Reading {
        match outcome {
            Ok(flow) => {
                assert_eq!(flow, Flow::Continue);
                Ok(events)
            }
            Err(fail) => {
                assert_eq!(fail.code, Code::InputInvalid, "{fail}");
                let code = fail.message.split(':').next().unwrap_or_default();
                Err((code.to_string(), fail.row, fail.column))
            }
        }
    }

    /// Holds the CSV line source to the whole parse of `text` through the
    /// same grammar options, at chunk sizes from a chunk per record (0) to
    /// the whole text and, on the owned path, at reader boundaries too, and
    /// returns that reading.
    fn csv_streams_as_whole(text: &str, options: &CsvOptions) -> Reading {
        let want = whole(tabnas_csv::make_with(options.clone()).parse(text));
        let format = LineFormat::csv_with(options.header, options.clone());
        let sizes =
            (0..=text.len() + 1).filter(|&n| n < 4 || n.is_power_of_two() || n >= text.len());
        for chunk_bytes in sizes {
            let mut rec: Vec<OwnedJsonEvent> = Vec::new();
            let outcome = LinesSource::new(Cursor::new(text), format.clone())
                .chunk_bytes(chunk_bytes)
                .run(&mut rec);
            assert_eq!(reading(outcome, rec), want, "{text:?}, chunk {chunk_bytes}");
        }
        for step in [1, 3] {
            let (outcome, rec) = LinesSource::new(trickle(text, step), format.clone())
                .chunk_bytes(0)
                .run_owned(Vec::<OwnedJsonEvent>::new());
            assert_eq!(reading(outcome, rec), want, "{text:?}, step {step}");
        }
        want
    }

    /// Holds the JSON Lines source to the whole parse of `text` by the
    /// JSON Lines grammar, on both paths, and returns that reading.
    fn jsonl_streams_as_whole(text: &str) -> Reading {
        let want = whole(tabnas_jsonl::parse(text));
        let mut rec: Vec<OwnedJsonEvent> = Vec::new();
        let outcome = LinesSource::new(Cursor::new(text), LineFormat::Jsonl).run(&mut rec);
        assert_eq!(reading(outcome, rec), want, "borrowed, {text:?}");
        let (outcome, rec) = LinesSource::new(Cursor::new(text), LineFormat::Jsonl)
            .run_owned(Vec::<OwnedJsonEvent>::new());
        let rec = without_lexemes(&rec);
        assert_eq!(reading(outcome, rec), want, "owned, {text:?}");
        want
    }

    fn objects(events: &[OwnedJsonEvent]) -> usize {
        events
            .iter()
            .filter(|e| matches!(e, OwnedJsonEvent::ObjectStart))
            .count()
    }

    /// tabnas-csv's vendored corpus file
    /// `papa-misplaced-quotes-in-data-twice-not-as-opening-quotes.csv`, byte
    /// for byte. A quote inside a field is that field's text, not the start
    /// of a quoted field, so the grammar reads two lines, the header and a
    /// record. The chunker took each quote for an opening or a closing one,
    /// read both lines as the header, and never parsed them: an empty table
    /// and success (tabnas/transduce#13).
    const MISPLACED_QUOTES: &str = "A,B\",C\nD,E\",F";

    #[test]
    fn quotes_inside_a_field_are_read_as_the_grammar_reads_them() {
        let header = CsvOptions::default();
        let want = csv_streams_as_whole(MISPLACED_QUOTES, &header).unwrap();
        assert_eq!(objects(&want), 1, "the header and one record: {want:?}");
        // The corpus's own options: no header, each record an array.
        let corpus = CsvOptions {
            header: false,
            object: false,
            ..CsvOptions::default()
        };
        csv_streams_as_whole(MISPLACED_QUOTES, &corpus).unwrap();
        // Records after it, one with a quote inside a field and then a
        // quoted field over two lines: the header the chunker found held a
        // record, which every later chunk repeated, and its cut fell inside
        // the quoted field.
        let more = format!("{MISPLACED_QUOTES}\nG,H,I\nJ\"K,L,\"M\nN\"\n");
        let want = csv_streams_as_whole(&more, &header).unwrap();
        assert_eq!(objects(&want), 3, "{want:?}");
        csv_streams_as_whole(&more, &corpus).unwrap();
    }

    #[test]
    fn an_input_ending_inside_a_quoted_field_fails_as_the_grammar_does() {
        // In the header, which a header-only chunk never parsed, and in a
        // record, with and without a newline after it.
        for text in [
            "a,\"b\n",
            "a,\"b",
            "a,\"b\nc,d\n",
            "\"\n",
            "a\n\"x\n",
            "a\n1\n\"x",
        ] {
            let want = csv_streams_as_whole(text, &CsvOptions::default());
            assert!(
                matches!(&want, Err((code, ..)) if code == "unterminated_string"),
                "{text:?}: {want:?}"
            );
        }
    }

    #[test]
    fn a_last_record_without_a_newline_is_read() {
        for text in ["a,b\n1,2\n3,4", "a,b\n1,2\n3,\"x\ny\"", "a,b\r\n1,2\r\n3,4"] {
            let want = csv_streams_as_whole(text, &CsvOptions::default()).unwrap();
            assert_eq!(objects(&want), 2, "{text:?}");
        }
    }

    fn csv_options(edit: impl FnOnce(&mut CsvOptions)) -> CsvOptions {
        let mut options = CsvOptions::default();
        edit(&mut options);
        options
    }

    #[test]
    fn the_header_is_the_first_record_the_grammar_reads() {
        let empty = csv_options(|o| o.record.empty = true);
        let relaxed = csv_options(|o| o.strict = false);
        let comment = csv_options(|o| o.comment = Some(true));
        let cases: &[(&str, &CsvOptions, usize)] = &[
            // A blank line before it is none of it...
            ("\n\na,b\n1,2\n3,4\n", &CsvOptions::default(), 2),
            ("\r\na,b\r\n1,2\r\n3,4\r\n", &CsvOptions::default(), 2),
            // ...unless a blank line is a record, when it is the header.
            ("\na,b\n\n1,2\n", &empty, 3),
            // A line of spaces is the header in strict mode, blank otherwise.
            ("  \na,b\n1,2\n3,4\n", &CsvOptions::default(), 3),
            ("  \na,b\n1,2\n3,4\n", &relaxed, 2),
            // A comment is no record, and a quote inside one no quote.
            ("# x \"\na,b\n1,2\n3,4\n", &comment, 2),
            ("// x \"\na,b\n1,2\n3,4\n", &relaxed, 2),
            ("a,b # c \"\n1,2\n3,4\n", &comment, 2),
        ];
        for (text, options, records) in cases {
            let want = csv_streams_as_whole(text, options).unwrap();
            assert_eq!(objects(&want), *records, "{text:?}");
        }
    }

    #[test]
    fn a_lone_carriage_return_ends_a_csv_record() {
        for text in ["a,b\r1,2\r3,4\r", "a,b\r1,2\n3,4\n5,6\n"] {
            let want = csv_streams_as_whole(text, &CsvOptions::default()).unwrap();
            assert!(objects(&want) >= 2, "{text:?}");
        }
    }

    #[test]
    fn a_field_spans_lines_only_where_the_grammar_reads_one_that_does() {
        let quote = csv_options(|o| o.string.quote = "'".into());
        let tildes = csv_options(|o| o.field.separation = Some("~~".into()));
        let comment = csv_options(|o| o.comment = Some(true));
        let cases: &[(&str, &CsvOptions)] = &[
            // The engine's own strings: a backtick spans lines, and an
            // escaped newline continues a single-quoted one, which a `"`
            // inside does not end.
            ("a,b\n`x\ny`,z\n1,2\n", &CsvOptions::default()),
            ("a,b\n'x\\\ny',z\n1,2\n", &CsvOptions::default()),
            ("a,b\n'x,\"y',z\n\"p\nq\",r\n", &CsvOptions::default()),
            // A quote after a space, or after a separator of the grammar's.
            ("a,b\nx, \"p\nq\"\n1,2\n", &CsvOptions::default()),
            ("a~~b\nx~~\"p\nq\"\n1~~2\n", &tildes),
            // The configured quote, and a `"` that is then the engine's,
            // which a line refuses.
            ("a,b\n'x\ny',z\n1,2\n", &quote),
            ("a,b\n\"x,y\n1,2\n", &quote),
            // A block comment over lines.
            ("a,b\n/* x\ny */1,2\n3,4\n", &comment),
        ];
        for (text, options) in cases {
            let _ = csv_streams_as_whole(text, options);
        }
    }

    #[test]
    fn configured_record_separators_end_records() {
        // A newline is then field text, and never a place to cut.
        let semicolons = csv_options(|o| o.record.separators = Some(";".into()));
        let want = csv_streams_as_whole("a,b;1,x\ny;3,4\nz;5,6", &semicolons).unwrap();
        assert_eq!(objects(&want), 3);
    }

    #[test]
    fn a_jsonl_line_is_blank_only_when_the_grammar_reads_it_so() {
        // Space and tab are the grammar's blanks; a form feed, a vertical
        // tab, a no-break space or a line separator is not, and the
        // grammar refuses the line rather than skipping it.
        for blank in ["\u{c}", "\u{b}", "\u{a0}", "\u{85}", "\u{2028}", "\u{3000}"] {
            let text = format!("{{\"a\":1}}\n{blank}\n{{\"b\":2}}\n");
            let want = jsonl_streams_as_whole(&text);
            assert_eq!(
                want.unwrap_err().1,
                Some(2),
                "{blank:?} is refused on its line"
            );
        }
        let want = jsonl_streams_as_whole("{\"a\":1}\n \t\n\r\n{\"b\":2}\n   ").unwrap();
        assert_eq!(objects(&want), 2);
    }

    #[test]
    fn a_lone_carriage_return_ends_a_jsonl_record() {
        let want = jsonl_streams_as_whole("{\"a\":1}\r{\"b\":2}\r\r \r{\"c\":3}\n").unwrap();
        assert_eq!(objects(&want), 3);
        // A value cut by one is incomplete, as the grammar reads it, and
        // the position of a failure after one is the grammar's.
        for text in ["{\"a\":\r1}\n", "{\"a\":1}\r{\"b\": }\n"] {
            jsonl_streams_as_whole(text).unwrap_err();
        }
        // Inside a string it is the string's (refused there, as a control
        // character), not a record's end.
        jsonl_streams_as_whole("{\"a\":\"x\ry\"}\n").unwrap_err();
    }

    #[test]
    fn records_one_line_holds_are_bounded_and_cut_one_by_one() {
        // Many records and no `\n`: records ended by a lone `\r`, by a
        // configured separator, and JSON Lines records ended by `\r`. Each
        // record is under the limit and the line far over it, so the limit
        // is the record's, and a chunk closes after a record rather than
        // after the line.
        let limits = Limits {
            max_record_bytes: 8,
            ..Limits::default()
        };
        let crs = format!("a,b\r{}", "1,2\r".repeat(200));
        let semicolons = csv_options(|o| o.record.separators = Some(";".into()));
        let separated = format!("a,b;{}", "1,x;".repeat(200));
        for (text, options) in [(&crs, CsvOptions::default()), (&separated, semicolons)] {
            let want = whole(tabnas_csv::make_with(options.clone()).parse(text));
            assert_eq!(objects(want.as_ref().unwrap()), 200, "{text:?}");
            let format = LineFormat::csv_with(options.header, options.clone());
            for chunk_bytes in [0, 64, DEFAULT_CHUNK_BYTES] {
                let mut rec: Vec<OwnedJsonEvent> = Vec::new();
                let outcome = LinesSource::new(Cursor::new(text.as_str()), format.clone())
                    .limits(limits.clone())
                    .chunk_bytes(chunk_bytes)
                    .run(&mut rec);
                assert_eq!(reading(outcome, rec), want, "{text:?}, chunk {chunk_bytes}");
            }
            // A chunk holds at most its size and one record more.
            let parser = tabnas_csv::make_with(options.clone());
            let mut chunks = Chunks::new(
                Cursor::new(text.as_bytes()),
                &parser,
                &options,
                64,
                limits.max_record_bytes,
            );
            let mut count = 0;
            while let Some(chunk) = chunks.next_chunk().unwrap() {
                assert!(chunk.text.len() <= 64 + 8, "{:?}", chunk.text);
                count += 1;
            }
            assert!(count > 10, "{text:?}: {count} chunks");
        }
        let records = "{\"a\":1}\r".repeat(200);
        let want = whole(tabnas_jsonl::parse(&records));
        assert_eq!(objects(want.as_ref().unwrap()), 200);
        let mut rec: Vec<OwnedJsonEvent> = Vec::new();
        let outcome = LinesSource::new(Cursor::new(records.as_str()), LineFormat::Jsonl)
            .limits(limits.clone())
            .run(&mut rec);
        assert_eq!(reading(outcome, rec), want);
        let (outcome, rec) = LinesSource::new(Cursor::new(records.as_str()), LineFormat::Jsonl)
            .limits(limits.clone())
            .run_owned(Vec::<OwnedJsonEvent>::new());
        assert_eq!(reading(outcome, without_lexemes(&rec)), want);
        // A record over the limit still fails, at the row it starts on,
        // which is counted as the engine counts rows: by `\n`, or by the
        // configured separator.
        let semicolons = csv_options(|o| o.record.separators = Some(";".into()));
        for (text, options, row) in [
            ("a,b\r1,2\r123456789,x\r", CsvOptions::default(), 1),
            ("a,b;1,x;123456789,x;", semicolons, 3),
        ] {
            let err = LinesSource::new(
                Cursor::new(text),
                LineFormat::csv_with(options.header, options),
            )
            .limits(limits.clone())
            .run(&mut Vec::<OwnedJsonEvent>::new())
            .unwrap_err();
            assert_eq!(err.limit.as_ref().unwrap().name, "max_record_bytes");
            assert_eq!(err.row, Some(row), "{text:?}: {err}");
        }
    }

    #[test]
    fn a_line_token_of_two_characters_is_never_cut() {
        // Under `record.empty` a run of line characters ends at a repeated
        // one, so `\r\n`, and `\n\r` as much, is one line token. A chunk
        // cut between its two characters would start with a line token,
        // a record of its own to a chunk without a header.
        for header in [false, true] {
            let options = csv_options(|o| {
                o.record.empty = true;
                o.header = header;
            });
            for text in [
                "a,b\r\n1,2\r\n\r\n3,4\r\n",
                "a,b\n\r1,2\n\r\n\r3,4",
                "a\r\n\r\r\nb\r\n",
            ] {
                csv_streams_as_whole(text, &options).unwrap();
            }
        }
    }

    #[test]
    fn a_line_character_inside_a_jsonl_string_is_the_strings() {
        // The grammar refuses a string at a raw line character, and the
        // record is read whole up to the line character after it, so the
        // failure is the grammar's: a `\n` in a string ends a record no
        // more than a `\r` does.
        for text in [
            "{\"a\":\"x\ny\"}\n{\"b\":1}\n",
            "[\"\\\n\"]\n",
            "{\"a\":\"x\r\ny\"}\r\n",
        ] {
            jsonl_streams_as_whole(text).unwrap_err();
        }
    }

    #[test]
    fn a_separator_of_several_bytes_is_found_across_reads() {
        // A configured separator outside ASCII is matched on its whole
        // UTF-8 form, read a byte at a time too; under `record.empty` a
        // token of two of them is followed across reads, and a character
        // that only starts like one is left to the next record.
        let one = csv_options(|o| o.record.separators = Some("␞".into()));
        csv_streams_as_whole("a,b␞1,é␞3,4␞", &one).unwrap();
        let two = csv_options(|o| {
            o.record.separators = Some("␞¶".into());
            o.record.empty = true;
            o.header = false;
        });
        csv_streams_as_whole("a,b␞¶£,é␞£,2¶␞3,4", &two).unwrap();
    }
}
