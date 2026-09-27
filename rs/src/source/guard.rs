//! The source-side limits, applied to events as they are produced.
//!
//! Every source emits through a [`Guarded`] sink, so the three limits a
//! source owns (`max_depth`, `max_key_bytes`, `max_scalar_bytes`) are
//! checked once, in one place, whether the events come from a live parse
//! or from a walk over a parsed value; the abort flag is polled per event
//! so a walk stops as promptly as a parse does; and the source metrics
//! (events, keys, scalars) are counted locally and flushed in one step,
//! so the hot path carries no atomic per event.

use std::sync::Arc;

use crate::error::Fail;
use crate::event::JsonEvent;
use crate::limits::{AbortFlag, Limits, Metrics};
use crate::sink::{Flow, Sink};

/// A sink wrapper that enforces the source limits and counts.
pub struct Guarded<S> {
    inner: S,
    max_depth: usize,
    max_key_bytes: usize,
    max_scalar_bytes: usize,
    abort: AbortFlag,
    metrics: Arc<Metrics>,
    depth: usize,
    events: u64,
    keys: u64,
    scalars: u64,
}

impl<S: Sink> Guarded<S> {
    pub fn new(inner: S, limits: &Limits, abort: AbortFlag, metrics: Arc<Metrics>) -> Guarded<S> {
        Guarded {
            inner,
            max_depth: limits.max_depth,
            max_key_bytes: limits.max_key_bytes,
            max_scalar_bytes: limits.max_scalar_bytes,
            abort,
            metrics,
            depth: 0,
            events: 0,
            keys: 0,
            scalars: 0,
        }
    }

    /// Add the counts so far to the shared metrics and start again.
    pub fn flush(&mut self) {
        Metrics::add(&self.metrics.events, self.events);
        Metrics::add(&self.metrics.keys, self.keys);
        Metrics::add(&self.metrics.scalars, self.scalars);
        self.events = 0;
        self.keys = 0;
        self.scalars = 0;
    }

    /// Open containers right now.
    pub fn depth(&self) -> usize {
        self.depth
    }

    pub fn inner(&self) -> &S {
        &self.inner
    }

    pub fn inner_mut(&mut self) -> &mut S {
        &mut self.inner
    }

    /// Flush the counts and give the sink back.
    pub fn into_inner(mut self) -> S {
        self.flush();
        self.inner
    }

    fn scalar(&mut self, bytes: usize) -> Result<(), Fail> {
        self.scalars += 1;
        if bytes > self.max_scalar_bytes {
            return Err(Fail::limit(
                "max_scalar_bytes",
                self.max_scalar_bytes as u64,
                format!(
                    "a scalar of {bytes} bytes is larger than {}",
                    self.max_scalar_bytes
                ),
            ));
        }
        Ok(())
    }
}

impl<S: Sink> Sink for Guarded<S> {
    fn event(&mut self, ev: JsonEvent<'_>) -> Result<Flow, Fail> {
        if self.abort.is_aborted() {
            return Err(Fail::aborted());
        }
        self.events += 1;
        match ev {
            JsonEvent::ObjectStart | JsonEvent::ArrayStart => {
                self.depth += 1;
                if self.depth > self.max_depth {
                    return Err(Fail::limit(
                        "max_depth",
                        self.max_depth as u64,
                        format!("a container is nested deeper than {}", self.max_depth),
                    ));
                }
            }
            JsonEvent::ObjectEnd | JsonEvent::ArrayEnd => {
                self.depth = self.depth.saturating_sub(1);
            }
            JsonEvent::Key(k) => {
                self.keys += 1;
                if k.len() > self.max_key_bytes {
                    return Err(Fail::limit(
                        "max_key_bytes",
                        self.max_key_bytes as u64,
                        format!(
                            "a key of {} bytes is longer than {}",
                            k.len(),
                            self.max_key_bytes
                        ),
                    ));
                }
            }
            JsonEvent::String(s) => self.scalar(s.len())?,
            JsonEvent::Number(n) => self.scalar(n.lexeme.map_or(0, str::len))?,
            JsonEvent::Null | JsonEvent::Bool(_) => self.scalar(0)?,
            JsonEvent::End => self.flush(),
        }
        self.inner.event(ev)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Code;
    use crate::event::OwnedJsonEvent;
    use crate::sink::replay;

    fn guarded(limits: Limits) -> (Guarded<Vec<OwnedJsonEvent>>, Arc<Metrics>, AbortFlag) {
        let metrics = Metrics::new();
        let abort = AbortFlag::new();
        (
            Guarded::new(Vec::new(), &limits, abort.clone(), metrics.clone()),
            metrics,
            abort,
        )
    }

    #[test]
    fn counts_are_flushed_at_end_and_on_into_inner() {
        let (mut g, metrics, _) = guarded(Limits::default());
        let events = [
            OwnedJsonEvent::ObjectStart,
            OwnedJsonEvent::Key("a".into()),
            OwnedJsonEvent::Null,
            OwnedJsonEvent::Key("b".into()),
            OwnedJsonEvent::String("x".into()),
            OwnedJsonEvent::ObjectEnd,
        ];
        replay(&events, &mut g).unwrap();
        assert_eq!(Metrics::get(&metrics.events), 0);
        g.event(JsonEvent::End).unwrap();
        assert_eq!(Metrics::get(&metrics.events), 7);
        assert_eq!(Metrics::get(&metrics.keys), 2);
        assert_eq!(Metrics::get(&metrics.scalars), 2);
        g.event(JsonEvent::Null).unwrap();
        let rec = g.into_inner();
        assert_eq!(Metrics::get(&metrics.events), 8);
        assert_eq!(rec.len(), 8);
    }

    #[test]
    fn each_source_limit_fails_by_name() {
        let (mut g, _, _) = guarded(Limits {
            max_depth: 1,
            ..Limits::default()
        });
        g.event(JsonEvent::ArrayStart).unwrap();
        let err = g.event(JsonEvent::ArrayStart).unwrap_err();
        assert_eq!(err.code, Code::ResourceLimitExceeded);
        assert_eq!(err.limit.as_ref().unwrap().name, "max_depth");

        let (mut g, _, _) = guarded(Limits {
            max_key_bytes: 2,
            ..Limits::default()
        });
        g.event(JsonEvent::ObjectStart).unwrap();
        let err = g.event(JsonEvent::Key("abc")).unwrap_err();
        assert_eq!(err.limit.as_ref().unwrap().name, "max_key_bytes");

        let (mut g, _, _) = guarded(Limits {
            max_scalar_bytes: 2,
            ..Limits::default()
        });
        assert_eq!(
            g.event(JsonEvent::String("abc"))
                .unwrap_err()
                .limit
                .unwrap()
                .name,
            "max_scalar_bytes"
        );
        let (mut g, _, _) = guarded(Limits {
            max_scalar_bytes: 2,
            ..Limits::default()
        });
        assert!(g
            .event(JsonEvent::Number(crate::event::Number::with_lexeme(
                1.5, "1.500"
            )))
            .is_err());
    }

    #[test]
    fn an_aborted_flag_stops_the_next_event() {
        let (mut g, _, abort) = guarded(Limits::default());
        g.event(JsonEvent::ArrayStart).unwrap();
        abort.abort();
        assert_eq!(g.event(JsonEvent::Null).unwrap_err().code, Code::Aborted);
    }
}
