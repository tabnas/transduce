//! The push boundary between stages.
//!
//! A pipeline is a chain of sinks. The source calls the first sink once per
//! event, synchronously, on the thread that parses; each stage does its work
//! and calls the next. Nothing is queued between stages, so a slow writer at
//! the end slows the parser at the start: that is the backpressure, and it
//! costs no buffer. A stage that must stop early (a `take`) answers
//! [`Flow::Stop`], which the source turns into a cancelled parse.

use crate::error::Fail;
use crate::event::{JsonEvent, OwnedJsonEvent};

/// What a stage wants next.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Flow {
    /// Keep sending.
    Continue,
    /// The stage has all it needs; the source should stop. Not an error:
    /// the source stops the parse, releases what it holds, and reports
    /// nothing further. Whether the rest of the input is validated first
    /// is the source's documented policy.
    Stop,
}

/// A consumer of `JsonEvents/1`.
pub trait Sink {
    /// One event. An `Err` aborts the run; the source stops the parse and
    /// the error reaches the caller unchanged.
    fn event(&mut self, ev: JsonEvent<'_>) -> Result<Flow, Fail>;
}

impl Sink for Vec<OwnedJsonEvent> {
    fn event(&mut self, ev: JsonEvent<'_>) -> Result<Flow, Fail> {
        self.push(ev.to_owned());
        Ok(Flow::Continue)
    }
}

impl<S: Sink + ?Sized> Sink for &mut S {
    fn event(&mut self, ev: JsonEvent<'_>) -> Result<Flow, Fail> {
        (**self).event(ev)
    }
}

impl<S: Sink + ?Sized> Sink for Box<S> {
    fn event(&mut self, ev: JsonEvent<'_>) -> Result<Flow, Fail> {
        (**self).event(ev)
    }
}

/// A sink made of a closure.
pub struct FnSink<F>(pub F);

impl<F> Sink for FnSink<F>
where
    F: FnMut(JsonEvent<'_>) -> Result<Flow, Fail>,
{
    fn event(&mut self, ev: JsonEvent<'_>) -> Result<Flow, Fail> {
        (self.0)(ev)
    }
}

/// A sink that counts events and drops them: the cheapest consumer, for
/// measuring a source on its own.
#[derive(Debug, Default)]
pub struct CountSink {
    pub events: u64,
}

impl Sink for CountSink {
    fn event(&mut self, _ev: JsonEvent<'_>) -> Result<Flow, Fail> {
        self.events += 1;
        Ok(Flow::Continue)
    }
}

/// Replay a recording into a sink, stopping where the sink stops.
pub fn replay(events: &[OwnedJsonEvent], sink: &mut dyn Sink) -> Result<Flow, Fail> {
    for ev in events {
        if sink.event(ev.as_event())? == Flow::Stop {
            return Ok(Flow::Stop);
        }
    }
    Ok(Flow::Continue)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_vector_records_and_replays() {
        let mut rec: Vec<OwnedJsonEvent> = Vec::new();
        rec.event(JsonEvent::ArrayStart).unwrap();
        rec.event(JsonEvent::Bool(true)).unwrap();
        rec.event(JsonEvent::ArrayEnd).unwrap();
        rec.event(JsonEvent::End).unwrap();
        let mut count = CountSink::default();
        assert_eq!(replay(&rec, &mut count).unwrap(), Flow::Continue);
        assert_eq!(count.events, 4);
    }

    #[test]
    fn stop_ends_a_replay() {
        let rec = vec![
            OwnedJsonEvent::ArrayStart,
            OwnedJsonEvent::Null,
            OwnedJsonEvent::ArrayEnd,
            OwnedJsonEvent::End,
        ];
        let mut seen = 0;
        let mut stopper = FnSink(|_ev: JsonEvent<'_>| {
            seen += 1;
            Ok(if seen == 2 {
                Flow::Stop
            } else {
                Flow::Continue
            })
        });
        assert_eq!(replay(&rec, &mut stopper).unwrap(), Flow::Stop);
        assert_eq!(seen, 2);
    }
}
