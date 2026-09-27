//! [`ParserSource`]: `JsonEvents/1` from a tabnas parse of one text.
//!
//! Two modes. `Materialize` parses, then walks the value: always sound,
//! retains the whole value, and is the only mode a borrowed sink can be
//! driven in. `Incremental` installs the rule-event adapter
//! ([`super::rule_events`]) so events leave the parse as containers open
//! and entries land, and optionally prunes streamed array elements from
//! the engine's tree. It is sound for the grammars
//! [`super::capability::incremental`] lists, which the differential suite
//! verifies, and produces the identical stream there. The engine's
//! subscriber must own its state (`Fn + Send + Sync + 'static`), so the
//! incremental path takes the sink by value ([`ParserSource::run_owned`],
//! [`ParserSource::run_boxed`]) and hands it back afterwards.
//!
//! Failure mapping, in this order: a sink failure is returned as it was;
//! a sink that stopped is `Ok(Flow::Stop)`; a parse cancelled through the
//! caller's [`AbortFlag`] is `ABORTED`; any other engine error is
//! `INPUT_INVALID` with the engine's code and position
//! ([`Fail::from_tabnas`]), a grammar's own depth guard included.

use std::sync::{Arc, Mutex};

use tabnas::Tabnas;

use crate::error::Fail;
use crate::event::JsonEvent;
use crate::limits::{AbortFlag, Limits, Metrics};
use crate::sink::{Flow, Sink};
use crate::source::guard::Guarded;
use crate::source::rule_events::{self, Adapter, Status, GUARD};
use crate::source::{walk_value, Prune, Source, SourceMode};

/// A tabnas parser applied to one text, as a source.
pub struct ParserSource<'s> {
    parser: Tabnas,
    text: &'s str,
    mode: SourceMode,
    limits: Limits,
    abort: AbortFlag,
    metrics: Arc<Metrics>,
}

impl<'s> ParserSource<'s> {
    /// A source in `Materialize` mode with default limits, its own abort
    /// flag and fresh metrics; the builder methods change each.
    pub fn new(parser: Tabnas, text: &'s str) -> ParserSource<'s> {
        ParserSource {
            parser,
            text,
            mode: SourceMode::Materialize,
            limits: Limits::default(),
            abort: AbortFlag::new(),
            metrics: Metrics::new(),
        }
    }

    pub fn mode(mut self, mode: SourceMode) -> Self {
        self.mode = mode;
        self
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

    /// Run with an owned sink, in the configured mode, and hand the sink
    /// back with the outcome.
    pub fn run_owned<S: Sink + Send + 'static>(self, sink: S) -> (Result<Flow, Fail>, S) {
        match &self.mode {
            SourceMode::Materialize => {
                let mut guarded =
                    Guarded::new(sink, &self.limits, self.abort.clone(), self.metrics.clone());
                let outcome = materialize(self.parser, self.text, &self.abort, &mut guarded);
                (outcome, guarded.into_inner())
            }
            SourceMode::Incremental { prune } => incremental(
                self.parser,
                self.text,
                &self.limits,
                self.abort,
                self.metrics,
                prune,
                sink,
            ),
        }
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
    /// Parse, then walk. A borrowed sink cannot be handed to the engine's
    /// subscriber, so this is the `Materialize` path whatever the mode;
    /// the incremental path is [`ParserSource::run_owned`]. The events
    /// are the same for a verified grammar; only the retention differs.
    fn run(self, sink: &mut dyn Sink) -> Result<Flow, Fail> {
        let mut guarded = Guarded::new(sink, &self.limits, self.abort.clone(), self.metrics);
        let outcome = materialize(self.parser, self.text, &self.abort, &mut guarded);
        guarded.flush();
        outcome
    }
}

/// The engine's cancel code: what a parse guard that answered `false`
/// reports.
const CANCEL: &str = "cancel";

/// Map an engine error: the caller's abort is `ABORTED`; everything else,
/// a grammar's own guard included, is the input's fault.
fn engine_failure(error: &tabnas::TabnasError, abort: &AbortFlag) -> Fail {
    if error.code == CANCEL && abort.is_aborted() {
        Fail::aborted()
    } else {
        Fail::from_tabnas(error)
    }
}

fn materialize<S: Sink>(
    mut parser: Tabnas,
    text: &str,
    abort: &AbortFlag,
    guarded: &mut Guarded<S>,
) -> Result<Flow, Fail> {
    let flag = abort.clone();
    parser.parse_guard(GUARD, move |_ctx| !flag.is_aborted());
    let value = parser.parse(text).map_err(|e| engine_failure(&e, abort))?;
    drop(parser);
    if walk_value(&value, guarded)? == Flow::Stop {
        return Ok(Flow::Stop);
    }
    guarded.event(JsonEvent::End)
}

fn incremental<S: Sink + Send + 'static>(
    mut parser: Tabnas,
    text: &str,
    limits: &Limits,
    abort: AbortFlag,
    metrics: Arc<Metrics>,
    prune: &Prune,
    sink: S,
) -> (Result<Flow, Fail>, S) {
    let stop = AbortFlag::new();
    let adapter = Adapter::new(sink, limits, abort.clone(), metrics, prune, stop.clone());
    let shared = Arc::new(Mutex::new(adapter));
    Adapter::install(
        &mut parser,
        Arc::downgrade(&shared),
        abort.clone(),
        stop.clone(),
    );
    let parsed = parser.parse(text);
    drop(parser);
    let mut adapter = rule_events::take(shared);
    let outcome = match adapter.status() {
        Status::Failed(_) | Status::Stopped => Ok(Flow::Continue),
        Status::Running => match parsed {
            Ok(_) if adapter.complete() => adapter.send(JsonEvent::End),
            Ok(_) => Err(rule_events::not_streamable()),
            Err(e) => Err(engine_failure(&e, &abort)),
        },
    };
    let (status, sink) = adapter.finish();
    let outcome = match status {
        Status::Failed(fail) => Err(fail),
        Status::Stopped => Ok(Flow::Stop),
        Status::Running => outcome,
    };
    (outcome, sink)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Code;
    use crate::event::OwnedJsonEvent;
    use crate::selector::Selector;
    use crate::sink::FnSink;

    fn incremental_mode() -> SourceMode {
        SourceMode::Incremental {
            prune: Prune::Never,
        }
    }

    fn record(mode: SourceMode, src: &str) -> (Result<Flow, Fail>, Vec<OwnedJsonEvent>) {
        ParserSource::new(tabnas_json::make(), src)
            .mode(mode)
            .run_owned(Vec::new())
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

    const DOC: &str = r#"{"a":[1,2.50,"x",{"b":null}],"c":{},"d":[],"e":1e21,"f":true}"#;

    #[test]
    fn incremental_events_equal_the_walk_and_carry_lexemes() {
        let (r1, inc) = record(incremental_mode(), DOC);
        let (r2, mat) = record(SourceMode::Materialize, DOC);
        assert_eq!(r1.unwrap(), Flow::Continue);
        assert_eq!(r2.unwrap(), Flow::Continue);
        assert_eq!(without_lexemes(&inc), mat);
        assert_eq!(inc.last(), Some(&OwnedJsonEvent::End));
        let lexemes: Vec<Option<&str>> = inc
            .iter()
            .filter_map(|e| match e {
                OwnedJsonEvent::Number { lexeme, .. } => Some(lexeme.as_deref()),
                _ => None,
            })
            .collect();
        assert_eq!(lexemes, [Some("1"), Some("2.50"), Some("1e21")]);
        assert!(mat.iter().all(|e| !matches!(
            e,
            OwnedJsonEvent::Number {
                lexeme: Some(_),
                ..
            }
        )));
    }

    #[test]
    fn a_root_scalar_is_one_event_then_end() {
        let (r, inc) = record(incremental_mode(), " 42 ");
        assert_eq!(r.unwrap(), Flow::Continue);
        assert_eq!(
            inc,
            vec![
                OwnedJsonEvent::Number {
                    value: 42.0,
                    lexeme: Some("42".into())
                },
                OwnedJsonEvent::End
            ]
        );
        let (r, inc) = record(incremental_mode(), r#""s""#);
        assert_eq!(r.unwrap(), Flow::Continue);
        assert_eq!(
            inc,
            vec![OwnedJsonEvent::String("s".into()), OwnedJsonEvent::End]
        );
    }

    #[test]
    fn the_borrowed_run_materializes_in_either_mode() {
        for mode in [SourceMode::Materialize, incremental_mode()] {
            let mut rec: Vec<OwnedJsonEvent> = Vec::new();
            let r = ParserSource::new(tabnas_json::make(), DOC)
                .mode(mode)
                .run(&mut rec);
            assert_eq!(r.unwrap(), Flow::Continue);
            assert_eq!(rec, record(SourceMode::Materialize, DOC).1);
        }
    }

    #[test]
    fn a_stop_from_the_sink_stops_the_parse_and_returns_the_sink() {
        for mode in [SourceMode::Materialize, incremental_mode()] {
            let seen = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let counter = seen.clone();
            let sink = FnSink(move |_ev: JsonEvent<'_>| {
                let n = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                Ok(if n == 3 { Flow::Stop } else { Flow::Continue })
            });
            let (r, _sink) = ParserSource::new(tabnas_json::make(), DOC)
                .mode(mode)
                .run_owned(sink);
            assert_eq!(r.unwrap(), Flow::Stop);
            assert_eq!(seen.load(std::sync::atomic::Ordering::Relaxed), 3);
        }
    }

    #[test]
    fn a_sink_failure_comes_back_unchanged() {
        for mode in [SourceMode::Materialize, incremental_mode()] {
            let sink = FnSink(|ev: JsonEvent<'_>| {
                if ev == JsonEvent::Key("c") {
                    Err(Fail::output("disk full").at_path(".c"))
                } else {
                    Ok(Flow::Continue)
                }
            });
            let (r, _) = ParserSource::new(tabnas_json::make(), DOC)
                .mode(mode)
                .run_owned(sink);
            let err = r.unwrap_err();
            assert_eq!(err.code, Code::OutputFailed);
            assert_eq!(err.path.as_deref(), Some(".c"));
        }
    }

    #[test]
    fn an_aborted_flag_cancels_the_parse_as_aborted() {
        for mode in [SourceMode::Materialize, incremental_mode()] {
            let abort = AbortFlag::new();
            abort.abort();
            let (r, _) = ParserSource::new(tabnas_json::make(), DOC)
                .mode(mode)
                .abort(abort)
                .run_owned(Vec::<OwnedJsonEvent>::new());
            assert_eq!(r.unwrap_err().code, Code::Aborted);
        }
    }

    #[test]
    fn a_parse_error_is_invalid_input_with_its_position() {
        for mode in [SourceMode::Materialize, incremental_mode()] {
            let (r, _) = record(mode, "{\"a\": 1,\n \"b\": }");
            let err = r.unwrap_err();
            assert_eq!(err.code, Code::InputInvalid);
            assert!(err.message.starts_with("unexpected"), "{}", err.message);
            assert_eq!(err.row, Some(2));
            assert_eq!(err.column, Some(7));
        }
    }

    #[test]
    fn source_limits_apply_in_both_modes_by_name() {
        for mode in [SourceMode::Materialize, incremental_mode()] {
            let limits = Limits {
                max_key_bytes: 1,
                ..Limits::default()
            };
            let (r, _) = ParserSource::new(tabnas_json::make(), r#"{"ab":1}"#)
                .mode(mode.clone())
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
                .mode(mode.clone())
                .limits(limits)
                .run_owned(Vec::<OwnedJsonEvent>::new());
            assert_eq!(r.unwrap_err().limit.unwrap().name, "max_depth");

            let limits = Limits {
                max_scalar_bytes: 2,
                ..Limits::default()
            };
            let (r, _) = ParserSource::new(tabnas_json::make(), r#"["abc"]"#)
                .mode(mode)
                .limits(limits)
                .run_owned(Vec::<OwnedJsonEvent>::new());
            assert_eq!(r.unwrap_err().limit.unwrap().name, "max_scalar_bytes");
        }
    }

    #[test]
    fn metrics_count_the_source_events() {
        for mode in [SourceMode::Materialize, incremental_mode()] {
            let metrics = Metrics::new();
            let (r, events) = ParserSource::new(tabnas_json::make(), DOC)
                .mode(mode)
                .metrics(metrics.clone())
                .run_owned(Vec::<OwnedJsonEvent>::new());
            r.unwrap();
            assert_eq!(Metrics::get(&metrics.events), events.len() as u64);
            assert_eq!(Metrics::get(&metrics.keys), 6);
            assert_eq!(Metrics::get(&metrics.scalars), 6);
        }
    }

    #[test]
    fn pruning_leaves_the_events_untouched() {
        let (_, plain) = record(incremental_mode(), DOC);
        for prune in [
            Prune::AllArrays,
            Prune::Under(Selector::root().property("a").each_index()),
            Prune::Under(Selector::root().property("a")),
        ] {
            let (r, pruned) = record(SourceMode::Incremental { prune }, DOC);
            r.unwrap();
            assert_eq!(pruned, plain);
        }
    }
}
