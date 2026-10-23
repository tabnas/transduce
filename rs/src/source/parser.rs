//! [`ParserSource`]: `JsonEvents/1` from a tabnas parse of one text.
//!
//! It parses, then walks the value: always sound, for every grammar, and
//! it retains the whole value. A format whose package ships a transducer
//! part streams through the part instead, over
//! [`super::ParserEventSource`]; the rule-event inference that streamed
//! the grammars the differential suite had verified, before the parts, was
//! retired with alchemy's streaming plan's step 13.
//!
//! Failure mapping, in this order: a sink failure is returned as it was;
//! a sink that stopped is `Ok(Flow::Stop)`; a parse cancelled through the
//! caller's [`AbortFlag`] is `ABORTED`; any other engine error is
//! `INPUT_INVALID` with the engine's code and position
//! ([`Fail::from_tabnas`]); a grammar's own guard cancelling the parse is
//! `INPUT_INVALID` too, with a message that names the grammar's guard
//! rather than a cancellation nobody asked for (`source::engine_failure`).

use std::sync::Arc;

use tabnas::Tabnas;

use crate::error::Fail;
use crate::event::JsonEvent;
use crate::limits::{AbortFlag, Limits, Metrics};
use crate::sink::{Flow, Sink};
use crate::source::guard::Guarded;
use crate::source::{engine_failure, walk_value, Source, GUARD};

/// A tabnas parser applied to one text, as a source.
pub struct ParserSource<'s> {
    parser: Tabnas,
    text: &'s str,
    limits: Limits,
    abort: AbortFlag,
    metrics: Arc<Metrics>,
}

impl<'s> ParserSource<'s> {
    /// A source with default limits, its own abort flag and fresh metrics;
    /// the builder methods change each.
    pub fn new(parser: Tabnas, text: &'s str) -> ParserSource<'s> {
        ParserSource {
            parser,
            text,
            limits: Limits::default(),
            abort: AbortFlag::new(),
            metrics: Metrics::new(),
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

    /// Run with an owned sink, and hand the sink back with the outcome.
    pub fn run_owned<S: Sink + Send + 'static>(self, sink: S) -> (Result<Flow, Fail>, S) {
        let (outcome, sink, _) = self.run_owned_with_value(sink);
        (outcome, sink)
    }

    /// [`ParserSource::run_owned`], also handing back the grammar's value,
    /// when the parse returned one.
    pub fn run_owned_with_value<S: Sink + Send + 'static>(
        self,
        sink: S,
    ) -> (Result<Flow, Fail>, S, Option<tabnas::Value>) {
        let mut guarded =
            Guarded::new(sink, &self.limits, self.abort.clone(), self.metrics.clone());
        let (outcome, value) = materialize(self.parser, self.text, &self.abort, &mut guarded);
        (outcome, guarded.into_inner(), value)
    }

    /// [`ParserSource::run_owned`] for a boxed sink.
    pub fn run_boxed(
        self,
        sink: Box<dyn Sink + Send>,
    ) -> (Result<Flow, Fail>, Box<dyn Sink + Send>) {
        self.run_owned(sink)
    }
}

impl Source for ParserSource<'_> {
    /// Parse, then walk, into a borrowed sink.
    fn run(self, sink: &mut dyn Sink) -> Result<Flow, Fail> {
        let mut guarded = Guarded::new(sink, &self.limits, self.abort.clone(), self.metrics);
        let (outcome, _) = materialize(self.parser, self.text, &self.abort, &mut guarded);
        guarded.flush();
        outcome
    }
}

/// Parse, then walk; the grammar's value comes back beside the outcome.
fn materialize<S: Sink>(
    mut parser: Tabnas,
    text: &str,
    abort: &AbortFlag,
    guarded: &mut Guarded<S>,
) -> (Result<Flow, Fail>, Option<tabnas::Value>) {
    let flag = abort.clone();
    parser.parse_guard(GUARD, move |_ctx| !flag.is_aborted());
    let value = match parser.parse(text) {
        Ok(value) => value,
        Err(e) => return (Err(engine_failure(&e, abort)), None),
    };
    drop(parser);
    let outcome = match walk_value(&value, guarded) {
        Ok(Flow::Continue) => guarded.event(JsonEvent::End),
        other => other,
    };
    (outcome, Some(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Code;
    use crate::event::OwnedJsonEvent;
    use crate::sink::FnSink;

    fn record(src: &str) -> (Result<Flow, Fail>, Vec<OwnedJsonEvent>) {
        ParserSource::new(tabnas_json::make(), src).run_owned(Vec::new())
    }

    const DOC: &str = r#"{"a":[1,2.50,"x",{"b":null}],"c":{},"d":[],"e":1e21,"f":true}"#;

    #[test]
    fn a_root_scalar_is_one_event_then_end() {
        let (r, events) = record(" 42 ");
        assert_eq!(r.unwrap(), Flow::Continue);
        assert_eq!(
            events,
            vec![
                OwnedJsonEvent::Number {
                    value: 42.0,
                    lexeme: None
                },
                OwnedJsonEvent::End
            ]
        );
        let (r, events) = record(r#""s""#);
        assert_eq!(r.unwrap(), Flow::Continue);
        assert_eq!(
            events,
            vec![OwnedJsonEvent::String("s".into()), OwnedJsonEvent::End]
        );
    }

    #[test]
    fn the_borrowed_run_walks_as_the_owned_run_does() {
        let mut rec: Vec<OwnedJsonEvent> = Vec::new();
        let r = ParserSource::new(tabnas_json::make(), DOC).run(&mut rec);
        assert_eq!(r.unwrap(), Flow::Continue);
        assert_eq!(rec, record(DOC).1);
    }

    #[test]
    fn a_stop_from_the_sink_stops_the_walk_and_returns_the_sink() {
        let seen = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = seen.clone();
        let sink = FnSink(move |_ev: JsonEvent<'_>| {
            let n = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
            Ok(if n == 3 { Flow::Stop } else { Flow::Continue })
        });
        let (r, _sink) = ParserSource::new(tabnas_json::make(), DOC).run_owned(sink);
        assert_eq!(r.unwrap(), Flow::Stop);
        assert_eq!(seen.load(std::sync::atomic::Ordering::Relaxed), 3);
    }

    #[test]
    fn a_sink_failure_comes_back_unchanged() {
        let sink = FnSink(|ev: JsonEvent<'_>| {
            if ev == JsonEvent::Key("c") {
                Err(Fail::output("disk full").at_path(".c"))
            } else {
                Ok(Flow::Continue)
            }
        });
        let (r, _) = ParserSource::new(tabnas_json::make(), DOC).run_owned(sink);
        let err = r.unwrap_err();
        assert_eq!(err.code, Code::OutputFailed);
        assert_eq!(err.path.as_deref(), Some(".c"));
    }

    #[test]
    fn an_aborted_flag_cancels_the_parse_as_aborted() {
        let abort = AbortFlag::new();
        abort.abort();
        let (r, _) = ParserSource::new(tabnas_json::make(), DOC)
            .abort(abort)
            .run_owned(Vec::<OwnedJsonEvent>::new());
        assert_eq!(r.unwrap_err().code, Code::Aborted);
    }

    #[test]
    fn a_parse_error_is_invalid_input_with_its_position() {
        let (r, _) = record("{\"a\": 1,\n \"b\": }");
        let err = r.unwrap_err();
        assert_eq!(err.code, Code::InputInvalid);
        assert!(err.message.starts_with("unexpected"), "{}", err.message);
        assert_eq!(err.row, Some(2));
        assert_eq!(err.column, Some(7));
    }

    /// tabnas-json refuses nesting deeper than 128 through a guard of its
    /// own, below the default `max_depth`; the failure must not read as a
    /// cancellation the caller asked for.
    #[test]
    fn a_grammars_own_guard_is_invalid_input_that_names_the_grammar() {
        let src = format!("{}1{}", "[".repeat(200), "]".repeat(200));
        let (r, _) = record(&src);
        let err = r.unwrap_err();
        assert_eq!(err.code, Code::InputInvalid);
        assert!(
            err.message.starts_with("the grammar stopped the parse"),
            "{err}"
        );
        assert!(err.message.contains("cancel"), "{err}");
        assert_eq!(err.column, Some(128));
    }

    #[test]
    fn source_limits_apply_by_name() {
        let limits = Limits {
            max_key_bytes: 1,
            ..Limits::default()
        };
        let (r, _) = ParserSource::new(tabnas_json::make(), r#"{"ab":1}"#)
            .limits(limits)
            .run_owned(Vec::<OwnedJsonEvent>::new());
        let err = r.unwrap_err();
        assert_eq!(err.code, Code::ResourceLimitExceeded);
        assert_eq!(err.limit.as_ref().unwrap().name, "max_key_bytes");

        let limits = Limits {
            max_depth: 2,
            ..Limits::default()
        };
        let (r, _) = ParserSource::new(tabnas_json::make(), "[[[1]]]")
            .limits(limits)
            .run_owned(Vec::<OwnedJsonEvent>::new());
        assert_eq!(r.unwrap_err().limit.unwrap().name, "max_depth");

        let limits = Limits {
            max_scalar_bytes: 2,
            ..Limits::default()
        };
        let (r, _) = ParserSource::new(tabnas_json::make(), r#"["abc"]"#)
            .limits(limits)
            .run_owned(Vec::<OwnedJsonEvent>::new());
        assert_eq!(r.unwrap_err().limit.unwrap().name, "max_scalar_bytes");
    }

    #[test]
    fn metrics_count_the_source_events() {
        let metrics = Metrics::new();
        let (r, events) = ParserSource::new(tabnas_json::make(), DOC)
            .metrics(metrics.clone())
            .run_owned(Vec::<OwnedJsonEvent>::new());
        r.unwrap();
        assert_eq!(Metrics::get(&metrics.events), events.len() as u64);
        assert_eq!(Metrics::get(&metrics.keys), 6);
        assert_eq!(Metrics::get(&metrics.scalars), 6);
    }
}
