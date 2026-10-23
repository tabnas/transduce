//! Sources: where `JsonEvents/1` come from.
//!
//! - [`ValueSource`] walks a parsed engine value. Always correct, retains
//!   the whole value: the fallback for every grammar.
//! - [`ParserSource`] parses one text with a tabnas parser and walks its
//!   value: the whole value, for every grammar. A format whose package
//!   ships a transducer part streams through it instead, over
//!   [`ParserEventSource`]; the rule-event inference that streamed the
//!   verified grammars before the parts was retired with alchemy's
//!   step 13.
//! - [`LinesSource`] reads CSV from any `BufRead` a chunk of records at a
//!   time, bounding memory whatever the file's size. JSON Lines is read
//!   through its part, from a reader ([`ParserEventSource::from_reader`]).
//!
//! - [`ParserEventSource`] drives a tabnas parse of one text and relays
//!   its rule passes as `ParserEvents/1`, the input a transducer part
//!   reads, dropping the passes a probe rewinds.
//!
//! Every JSON-events source emits through [`Guarded`], which enforces the
//! source limits (`max_depth`, `max_key_bytes`, `max_scalar_bytes`), polls
//! the abort flag and counts the source metrics; [`ParserEventSource`]
//! holds `max_scalar_bytes` on each pass as it builds it, and polls the
//! flag and counts its events the same way.

pub mod capability;
pub mod guard;
pub mod lines;
pub mod parser;
pub mod parser_events;

pub use guard::Guarded;
pub use lines::{LineFormat, LinesSource, DEFAULT_CHUNK_BYTES};
pub use parser::ParserSource;
pub use parser_events::ParserEventSource;

use crate::error::Fail;
use crate::event::{JsonEvent, Number};
use crate::limits::AbortFlag;
use crate::sink::{Flow, Sink};

/// The name this crate's parse guard is installed under.
pub(crate) const GUARD: &str = "tabnas-transduce.abort";

/// RFC 8259's number grammar: `-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?`.
/// Only such a lexeme is kept, so a renderer can write it as it stands.
pub(crate) fn is_json_number(s: &str) -> bool {
    let b = s.as_bytes();
    let mut i = 0;
    if b.first() == Some(&b'-') {
        i += 1;
    }
    match b.get(i) {
        Some(b'0') => i += 1,
        Some(c) if c.is_ascii_digit() => {
            while b.get(i).is_some_and(u8::is_ascii_digit) {
                i += 1;
            }
        }
        _ => return false,
    }
    if b.get(i) == Some(&b'.') {
        i += 1;
        let start = i;
        while b.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
        }
        if i == start {
            return false;
        }
    }
    if matches!(b.get(i), Some(b'e') | Some(b'E')) {
        i += 1;
        if matches!(b.get(i), Some(b'+') | Some(b'-')) {
            i += 1;
        }
        let start = i;
        while b.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
        }
        if i == start {
            return false;
        }
    }
    i == b.len()
}

/// The engine's cancel code: what a parse guard that answered `false`
/// reports, whether the guard is this crate's abort or the grammar's own.
const CANCEL: &str = "cancel";

/// Map an engine error to a failure. A cancel while the caller's flag is
/// set is `ABORTED`. A cancel otherwise is a guard the GRAMMAR installed
/// (tabnas-json refuses nesting deeper than 128, well below the default
/// `Limits::max_depth`), so the message says so instead of "parse
/// cancelled", which would read as the caller's doing; it stays
/// `INPUT_INVALID`, because the document is what the grammar refused. Any
/// other error is the input's, with the engine's code and position.
pub(crate) fn engine_failure(error: &tabnas::TabnasError, abort: &AbortFlag) -> Fail {
    if error.code != CANCEL {
        return Fail::from_tabnas(error);
    }
    if abort.is_aborted() {
        return Fail::aborted();
    }
    let mut fail = Fail::from_tabnas(error);
    fail.message = format!(
        "the grammar stopped the parse with a guard of its own ({}: {}); a grammar may refuse \
         nesting or size below this crate's Limits",
        error.code,
        error.detail.trim_end()
    );
    fail
}

/// Something that can drive a sink with one document's events.
pub trait Source {
    /// Run to completion, or until the sink stops, or until a failure.
    /// `Ok(Flow::Stop)` means the sink stopped it; the document was not
    /// validated past that point.
    fn run(self, sink: &mut dyn Sink) -> Result<Flow, Fail>;
}

/// Emit a parsed engine value as events, ending with [`JsonEvent::End`].
pub struct ValueSource<'v>(pub &'v tabnas::Value);

impl Source for ValueSource<'_> {
    fn run(self, sink: &mut dyn Sink) -> Result<Flow, Fail> {
        if walk_value(self.0, sink)? == Flow::Stop {
            return Ok(Flow::Stop);
        }
        sink.event(JsonEvent::End)
    }
}

/// Emit one engine value's events (without `End`).
///
/// `Undefined` is `null`, as the engine serializes it; the metadata
/// wrappers (`Text`, `MapRef`, `ListRef`) unwrap to their plain forms.
pub fn walk_value(value: &tabnas::Value, sink: &mut dyn Sink) -> Result<Flow, Fail> {
    macro_rules! send {
        ($ev:expr) => {
            if sink.event($ev)? == Flow::Stop {
                return Ok(Flow::Stop);
            }
        };
    }
    match value {
        tabnas::Value::Undefined | tabnas::Value::Null => send!(JsonEvent::Null),
        tabnas::Value::Bool(b) => send!(JsonEvent::Bool(*b)),
        tabnas::Value::Number(n) => send!(JsonEvent::Number(Number::new(*n))),
        tabnas::Value::String(s) => send!(JsonEvent::String(s)),
        tabnas::Value::Text(t) => send!(JsonEvent::String(&t.string)),
        tabnas::Value::Array(items) => {
            send!(JsonEvent::ArrayStart);
            for item in items.iter() {
                if walk_value(item, sink)? == Flow::Stop {
                    return Ok(Flow::Stop);
                }
            }
            send!(JsonEvent::ArrayEnd);
        }
        tabnas::Value::ListRef(list) => {
            send!(JsonEvent::ArrayStart);
            for item in list.value.iter() {
                if walk_value(item, sink)? == Flow::Stop {
                    return Ok(Flow::Stop);
                }
            }
            send!(JsonEvent::ArrayEnd);
        }
        tabnas::Value::Object(members) => {
            send!(JsonEvent::ObjectStart);
            for (k, v) in members.iter() {
                send!(JsonEvent::Key(k));
                if walk_value(v, sink)? == Flow::Stop {
                    return Ok(Flow::Stop);
                }
            }
            send!(JsonEvent::ObjectEnd);
        }
        tabnas::Value::MapRef(map) => {
            send!(JsonEvent::ObjectStart);
            for (k, v) in map.value.iter() {
                send!(JsonEvent::Key(k));
                if walk_value(v, sink)? == Flow::Stop {
                    return Ok(Flow::Stop);
                }
            }
            send!(JsonEvent::ObjectEnd);
        }
    }
    Ok(Flow::Continue)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::OwnedJsonEvent;

    #[test]
    fn a_value_walks_in_document_order() {
        let v = tabnas_json::parse(r#"{"a":[1,"x"],"b":null}"#).unwrap();
        let mut rec: Vec<OwnedJsonEvent> = Vec::new();
        ValueSource(&v).run(&mut rec).unwrap();
        assert_eq!(
            rec,
            vec![
                OwnedJsonEvent::ObjectStart,
                OwnedJsonEvent::Key("a".into()),
                OwnedJsonEvent::ArrayStart,
                OwnedJsonEvent::Number {
                    value: 1.0,
                    lexeme: None
                },
                OwnedJsonEvent::String("x".into()),
                OwnedJsonEvent::ArrayEnd,
                OwnedJsonEvent::Key("b".into()),
                OwnedJsonEvent::Null,
                OwnedJsonEvent::ObjectEnd,
                OwnedJsonEvent::End,
            ]
        );
    }
}
