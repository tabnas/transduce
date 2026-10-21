// `ParserEventSource::from_reader` (admin ADR-30): the rule passes of a
// source the parser reads from a reader, against the same text read whole.
// Every committed fixture of a grammar the harness builds is handed over a
// few bytes at a time. JSON's, JSONC's and JSON Lines' grammars, whose
// matchers are the engine's own, declare `lex.window` and are read a window
// at a time; every other grammar reads the reader whole first. The passes,
// the value and the outcome must be the same, and a reader that fails is
// `INPUT_INVALID` with the engine's `unreadable`.

mod common;

use std::fs;
use std::io::Read;
use std::path::PathBuf;

use tabnas::Tabnas;
use tabnas_transduce::{Code, Fail, Flow, ParserEvent, ParserEventSource, ParserSink};

/// A reader that hands its bytes over at most `size` at a time.
struct Pieces {
    bytes: Vec<u8>,
    at: usize,
    size: usize,
}

impl Read for Pieces {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.size.min(buf.len()).min(self.bytes.len() - self.at);
        buf[..n].copy_from_slice(&self.bytes[self.at..self.at + n]);
        self.at += n;
        Ok(n)
    }
}

/// A reader that hands over `good` bytes, a few at a time, and then fails.
struct Failing {
    bytes: Vec<u8>,
    at: usize,
    good: usize,
}

impl Read for Failing {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.at >= self.good {
            return Err(std::io::Error::other("the disk went away"));
        }
        let n = 3.min(buf.len()).min(self.good - self.at);
        buf[..n].copy_from_slice(&self.bytes[self.at..self.at + n]);
        self.at += n;
        Ok(n)
    }
}

/// The passes a run delivered, in the fixture encoding.
#[derive(Default)]
struct Passes(Vec<serde_json::Value>);

impl ParserSink for Passes {
    fn event(&mut self, ev: &ParserEvent) -> Result<Flow, Fail> {
        self.0.push(ev.to_json());
        Ok(Flow::Continue)
    }
}

/// The grammar a fixture's extension names, and whether its matchers are
/// the engine's own, so that it may declare `lex.window`.
fn grammar_for(file: &str) -> Option<(&'static str, bool)> {
    match file.rsplit('.').next()? {
        "json" => Some(("json", true)),
        "jsonc" => Some(("jsonc", true)),
        "jsonl" => Some(("jsonl", true)),
        "json5" => Some(("json5", false)),
        "jsonic" => Some(("jsonic", false)),
        "yaml" => Some(("yaml", false)),
        "zon" => Some(("zon", false)),
        "csv" => Some(("csv", false)),
        "toml" => Some(("toml", false)),
        "ini" => Some(("ini", false)),
        _ => None,
    }
}

fn parser(name: &str, window: bool) -> Tabnas {
    let mut parser = common::grammar(name)();
    if window {
        parser
            .set_options(|options| options.lex.window = true)
            .expect("lex.window sets");
    }
    parser
}

/// What a run showed: its outcome, its passes and the value it returned.
fn run(source: ParserEventSource<'_>) -> (String, Vec<serde_json::Value>, Option<String>) {
    let (outcome, passes, value) = source.run_owned_with_value(Passes::default());
    let outcome = match outcome {
        Ok(flow) => format!("{flow:?}"),
        Err(fail) => format!(
            "{:?} {} {:?} {:?}",
            fail.code, fail.message, fail.row, fail.column
        ),
    };
    (outcome, passes.0, value.map(|value| value.to_string()))
}

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

#[test]
fn every_fixture_streams_the_same_from_a_reader() {
    let mut files: Vec<String> = fs::read_dir(fixtures_dir())
        .expect("tests/fixtures exists")
        .map(|entry| {
            entry
                .expect("a directory entry")
                .file_name()
                .into_string()
                .expect("UTF-8")
        })
        .collect();
    files.sort();
    let mut compared = 0;
    for file in &files {
        let Some((name, window)) = grammar_for(file) else {
            continue;
        };
        let text = fs::read_to_string(fixtures_dir().join(file)).expect("a UTF-8 fixture");
        let whole = run(ParserEventSource::new(parser(name, window), &text));
        for size in [1, 3, 64] {
            let reader = Pieces {
                bytes: text.clone().into_bytes(),
                at: 0,
                size,
            };
            let read = run(ParserEventSource::from_reader(parser(name, window), reader));
            assert_eq!(read, whole, "{file} in pieces of {size}");
            compared += 1;
        }
    }
    assert!(compared >= 60, "{compared}");
}

#[test]
fn an_unreadable_source_is_invalid_input() {
    let text = b"[1,\n2,\n3333,\n4]".to_vec();
    for window in [true, false] {
        let reader = Failing {
            bytes: text.clone(),
            at: 0,
            good: 9,
        };
        let (outcome, passes, _) = ParserEventSource::from_reader(parser("json", window), reader)
            .run_owned_with_value(Passes::default());
        let fail = outcome.expect_err("the read fails");
        assert_eq!(fail.code, Code::InputInvalid, "{fail:?}");
        assert!(fail.message.starts_with("unreadable: "), "{}", fail.message);
        assert!(
            fail.message.contains("the disk went away"),
            "{}",
            fail.message
        );
        assert_eq!((fail.row, fail.column), (Some(3), Some(3)));
        if !window {
            // Read whole first, the source fails before any pass.
            assert!(passes.0.is_empty(), "{:?}", passes.0);
        }
    }
}
