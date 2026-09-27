//! Sources: where `JsonEvents/1` come from.
//!
//! - [`ValueSource`] walks a parsed engine value. Always correct, retains
//!   the whole value: the fallback for every grammar.
//! - `ParserSource` (this module's `parser` submodule, in progress) drives a
//!   tabnas parse and turns its rule events into source events as they
//!   happen, for the grammars the differential suite has verified.
//! - `LinesSource` (in progress) reads JSON Lines or CSV a record at a time,
//!   bounding memory whatever the file size.

use crate::error::Fail;
use crate::event::{JsonEvent, Number};
use crate::sink::{Flow, Sink};

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
