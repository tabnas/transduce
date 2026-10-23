//! `TransduceRecords` over the csv grammar's own rule passes makes the
//! events of the grammar's value. tabnas-csv's transducer part is the spec
//! of alchemy's `transduce-records` below, which a host's routers run with
//! this native, and the part promises the walk of the value the parse
//! returns. So every document here streams as that walk: every document
//! of tabnas-csv's own `test/spec` corpus under the grammar's default
//! options, every committed CSV fixture, and the generated records, each
//! read pruned, from its text and from a reader that hands over a few
//! bytes at a time. The one difference is the part's, documented: a
//! header that names a column twice streams both cells, where the walk
//! keeps the last, and a host's tree contract after the part refuses it.

mod support;

use std::io::Read;
use std::path::PathBuf;

use tabnas_transduce::{
    Datum, Fail, Flow, OwnedJsonEvent, ParserEventSource, ParserSource, RecordsSpec,
    TransduceRecords,
};

/// tabnas-csv's part: `alchemy/transduce.alc` in tabnas/csv.
fn csv_part() -> RecordsSpec {
    RecordsSpec::new("csv", "record", "val")
        .with_empty("empty")
        .with_header("field~")
        .with_fill(Datum::String("".into()))
}

type Run = (Result<Flow, Fail>, Vec<OwnedJsonEvent>);

fn native(text: &str) -> Run {
    let (r, t) = ParserEventSource::new(tabnas_csv::make(), text)
        .grammar("csv")
        .prune()
        .run_owned(TransduceRecords::new(csv_part(), Vec::new()));
    (r, t.into_inner())
}

/// A reader that hands over `step` bytes at a time.
struct Trickle {
    bytes: Vec<u8>,
    at: usize,
    step: usize,
}

impl Read for Trickle {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.step.min(buf.len()).min(self.bytes.len() - self.at);
        buf[..n].copy_from_slice(&self.bytes[self.at..self.at + n]);
        self.at += n;
        Ok(n)
    }
}

fn native_from_a_reader(text: &str, step: usize) -> Run {
    let reader = Trickle {
        bytes: text.as_bytes().to_vec(),
        at: 0,
        step,
    };
    let (r, t) = ParserEventSource::from_reader(tabnas_csv::make(), reader)
        .grammar("csv")
        .prune()
        .run_owned(TransduceRecords::new(csv_part(), Vec::new()));
    (r, t.into_inner())
}

fn walk(text: &str) -> Run {
    ParserSource::new(tabnas_csv::make(), text).run_owned(Vec::new())
}

/// The outcome as a comparison reads it: the code alone of a failure,
/// since the walk fails before it writes anything and a stream after what
/// it has written.
fn ending(r: &Result<Flow, Fail>) -> String {
    match r {
        Ok(flow) => format!("{flow:?}"),
        Err(f) => format!("{:?}", f.code),
    }
}

/// Whether an object in the stream names a member twice: the header
/// repeats a name.
fn repeats_a_name(events: &[OwnedJsonEvent]) -> bool {
    let mut open: Vec<Vec<String>> = Vec::new();
    for ev in events {
        match ev {
            OwnedJsonEvent::ObjectStart => open.push(Vec::new()),
            OwnedJsonEvent::ObjectEnd => {
                open.pop();
            }
            OwnedJsonEvent::Key(k) => {
                if let Some(keys) = open.last_mut() {
                    if keys.iter().any(|seen| seen.as_str() == &**k) {
                        return true;
                    }
                    keys.push(k.to_string());
                }
            }
            _ => {}
        }
    }
    false
}

/// Every document of tabnas-csv's own corpus, the committed CSV fixtures
/// and the generated records, by name.
fn documents() -> Vec<(String, String)> {
    let mut docs = Vec::new();
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../csv/test/spec");
    let specs = tabnas_support::load_spec_dir(&dir, &tabnas_support::SpecOptions::default())
        .unwrap_or_else(|e| {
            panic!("{e}: clone https://github.com/tabnas/csv beside this repository")
        });
    for spec in &specs {
        for row in &spec.rows {
            docs.push((
                format!("{}:{}", row.file, row.line),
                row.unesc_named("input"),
            ));
        }
    }
    let fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    for entry in std::fs::read_dir(fixtures).expect("tests/fixtures exists") {
        let path = entry.expect("a directory entry").path();
        if path.extension().and_then(|e| e.to_str()) == Some("csv") {
            let text = std::fs::read_to_string(&path).expect("a UTF-8 fixture");
            docs.push((path.display().to_string(), text));
        }
    }
    docs.push(("records_csv(2000)".into(), support::records_csv(2_000)));
    docs.push(("records_csv(0)".into(), support::records_csv(0)));
    docs
}

#[test]
fn every_document_streams_as_the_walk_of_its_value() {
    let docs = documents();
    let mut agreed = 0;
    let mut repeated = 0;
    let mut differ = Vec::new();
    for (name, text) in &docs {
        let (wr, walked) = walk(text);
        let (nr, streamed) = native(text);
        if wr.is_err() {
            // A document the grammar refuses: both refuse, with its code.
            if ending(&wr) != ending(&nr) {
                differ.push(format!("{name}: walk {wr:?}, native {nr:?}"));
            } else {
                agreed += 1;
            }
            continue;
        }
        if nr.is_ok() && walked == streamed {
            agreed += 1;
        } else if nr.is_ok() && repeats_a_name(&streamed) {
            repeated += 1;
        } else {
            differ.push(format!(
                "{name} {text:?}:\n  walk   {wr:?} {walked:?}\n  native {nr:?} {streamed:?}"
            ));
        }
    }
    println!(
        "records: {agreed} of {} documents stream as the walk, {repeated} repeat a header name",
        docs.len()
    );
    assert!(differ.is_empty(), "{}", differ.join("\n"));
    assert!(agreed > 50, "the corpus was read: {agreed} documents");
}

#[test]
fn a_reader_a_few_bytes_at_a_time_streams_as_the_text() {
    for (name, text) in documents() {
        let whole = native(&text);
        for step in [1, 2, 3] {
            let read = native_from_a_reader(&text, step);
            assert_eq!(
                (ending(&read.0), &read.1),
                (ending(&whole.0), &whole.1),
                "{name}, {step} bytes at a time"
            );
        }
    }
}

#[test]
fn a_header_that_repeats_a_name_streams_both_cells() {
    let (r, streamed) = native("a,a\n1,2\n");
    assert_eq!(r.unwrap(), Flow::Continue);
    use OwnedJsonEvent::*;
    assert_eq!(
        streamed,
        vec![
            ArrayStart,
            ObjectStart,
            Key("a".into()),
            String("1".into()),
            Key("a".into()),
            String("2".into()),
            ObjectEnd,
            ArrayEnd,
            End,
        ]
    );
    let (_, walked) = walk("a,a\n1,2\n");
    assert_eq!(
        walked,
        vec![
            ArrayStart,
            ObjectStart,
            Key("a".into()),
            String("2".into()),
            ObjectEnd,
            ArrayEnd,
            End,
        ]
    );
}
