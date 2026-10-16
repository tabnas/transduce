//! [`ParserEventSource`]: `ParserEvents/1` from a tabnas parse of one
//! text, the input a transducer part reads.
//!
//! The source installs one `rule_done` subscriber, which relays each rule
//! pass the engine completes as a [`ParserEvent`], on the thread that
//! parses, into a [`ParserSink`]: an `Open` when a rule's open pass has
//! run, a `Close` when its close pass has. It knows no grammar. When the
//! engine has returned the parse's value it sends one `End`, so a document
//! is complete only once it validated, as every source's is.
//!
//! What each pass carries is alchemy's [`RulePass`]:
//!
//! - the rule's `name`, its depth `d` and instance `i`, the alternate that
//!   matched (`alt`: `p`, `r`, `b`, `g`), whether recovery forced the
//!   close, and its named counters `n`, sorted by name;
//! - the `tokens` the pass consumed: those its alternate matched, less the
//!   `b` it gave back from their end, which are the next rule's to
//!   consume, and none when `b` is more than it recorded, since a close
//!   alternate can back up a token it did not record. A token is its
//!   name, its value, its source text and its 1-based row and column; a
//!   token with no value (the end of the source) has `null`, and a number
//!   carries its source text as its lexeme when that text is a JSON number
//!   spelling the value (RFC 8259), the rule the incremental source keeps
//!   lexemes by, so a renderer can write it as it stands;
//! - on a close pass, the rule's node when that is a scalar (`value`), a
//!   number with the lexeme of the rule's first open token as above;
//!   nothing on an open pass, nor for a container.
//!
//! A pass made while a probe is open is speculative: the grammar parses
//! ahead and rewinds past it (tabnas-bnf's probe, the engine's own for a
//! serialized grammar's `pd_` keys), so it is never the document's. The
//! engine flags it (`RuleDone::probe`), and the source drops it before any
//! sink sees it.
//!
//! The source holds its limits as it builds each pass, before the sink
//! sees it: a token's value or source text, or a pass's value, longer than
//! `max_scalar_bytes` is refused with that limit's name, as a JSON-events
//! source refuses a scalar, so one huge token in a document reaches no
//! program's step. `max_depth` and `max_key_bytes` hold where a part's
//! outputs become `JsonEvents`, on the events it writes. The abort flag is
//! polled per pass, and `Metrics::events` counts the events delivered.
//!
//! Failure mapping is [`super::ParserSource`]'s: a sink failure is
//! returned as it was; a sink that stopped is `Ok(Flow::Stop)`; a parse
//! cancelled through the caller's [`AbortFlag`] is `ABORTED`; any other
//! engine error is `INPUT_INVALID` with the engine's code and position.

use std::sync::{Arc, Mutex, MutexGuard, Weak};

use tabnas::{Rule, RuleDone, RuleState, Tabnas, Value};

use crate::datum::Datum;
use crate::error::Fail;
use crate::limits::{AbortFlag, Limits, Metrics};
use crate::sink::Flow;
use crate::source::engine_failure;
use crate::source::rule_events::{is_json_number, GUARD};
use crate::{ParserEvent, ParserSink, PassAlt, PassToken, RulePass};

/// A tabnas parser applied to one text, as a source of its rule passes.
pub struct ParserEventSource<'s> {
    parser: Tabnas,
    text: &'s str,
    limits: Limits,
    abort: AbortFlag,
    metrics: Arc<Metrics>,
}

impl<'s> ParserEventSource<'s> {
    /// A source with default limits, its own abort flag and fresh metrics;
    /// the builder methods change each.
    pub fn new(parser: Tabnas, text: &'s str) -> ParserEventSource<'s> {
        ParserEventSource {
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

    /// Run with an owned sink and hand the sink back with the outcome. The
    /// engine's subscriber must own its state, so the sink is taken by
    /// value, as [`super::ParserSource::run_owned`] takes it.
    pub fn run_owned<S: ParserSink + Send + 'static>(self, sink: S) -> (Result<Flow, Fail>, S) {
        let ParserEventSource {
            mut parser,
            text,
            limits,
            abort,
            metrics,
        } = self;
        let stop = AbortFlag::new();
        let relay = Arc::new(Mutex::new(Relay {
            sink,
            status: Status::Running,
            max_scalar_bytes: limits.max_scalar_bytes,
            abort: abort.clone(),
            stop: stop.clone(),
            events: 0,
        }));
        install(
            &mut parser,
            Arc::downgrade(&relay),
            abort.clone(),
            stop.clone(),
        );
        let parsed = parser.parse(text);
        drop(parser);
        let mut relay = match Arc::try_unwrap(relay) {
            Ok(mutex) => mutex
                .into_inner()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            Err(_) => unreachable!("the parse returned, so no subscriber holds the relay"),
        };
        let outcome = match std::mem::replace(&mut relay.status, Status::Running) {
            Status::Failed(fail) => Err(fail),
            Status::Stopped => Ok(Flow::Stop),
            Status::Running => match &parsed {
                Ok(_) => relay.send(&ParserEvent::End),
                Err(e) => Err(engine_failure(e, &abort)),
            },
        };
        Metrics::add(&metrics.events, relay.events);
        (outcome, relay.sink)
    }

    /// [`ParserEventSource::run_owned`] for a boxed sink.
    pub fn run_boxed(
        self,
        sink: Box<dyn ParserSink + Send>,
    ) -> (Result<Flow, Fail>, Box<dyn ParserSink + Send>) {
        self.run_owned(sink)
    }
}

/// How the run stands, as seen from inside the subscriber.
enum Status {
    Running,
    /// The sink answered `Stop`.
    Stopped,
    /// The sink or a limit failed; the parse was told to cancel.
    Failed(Fail),
}

/// The subscriber's state: the sink and how the run stands.
struct Relay<S> {
    sink: S,
    status: Status,
    max_scalar_bytes: usize,
    abort: AbortFlag,
    /// Set to make the parse guard cancel the parse.
    stop: AbortFlag,
    events: u64,
}

impl<S: ParserSink> Relay<S> {
    fn send(&mut self, ev: &ParserEvent) -> Result<Flow, Fail> {
        self.events += 1;
        self.sink.event(ev)
    }

    fn fail(&mut self, fail: Fail) {
        self.status = Status::Failed(fail);
        self.stop.abort();
    }

    /// One pass the engine completed.
    fn on_done(&mut self, rule: &Rule, done: &RuleDone) {
        if done.probe || !matches!(self.status, Status::Running) {
            return;
        }
        if self.abort.is_aborted() {
            self.fail(Fail::aborted());
            return;
        }
        let open = done.state == RuleState::Open;
        let pass = match pass(rule, done, open, self.max_scalar_bytes) {
            Ok(pass) => pass,
            Err(fail) => return self.fail(fail),
        };
        let ev = if open {
            ParserEvent::Open(pass)
        } else {
            ParserEvent::Close(pass)
        };
        match self.send(&ev) {
            Ok(Flow::Continue) => {}
            Ok(Flow::Stop) => {
                self.status = Status::Stopped;
                self.stop.abort();
            }
            Err(fail) => self.fail(fail),
        }
    }
}

/// Install the subscriber and the parse guard. The guard cancels the parse
/// when the caller's flag or the relay's own stop flag is set. The
/// subscriber upgrades its `Weak` per pass, so the driver keeps the only
/// strong reference.
fn install<S: ParserSink + Send + 'static>(
    parser: &mut Tabnas,
    relay: Weak<Mutex<Relay<S>>>,
    abort: AbortFlag,
    stop: AbortFlag,
) {
    parser.subscribe_rule_done(move |rule, _ctx, done| {
        if let Some(relay) = relay.upgrade() {
            lock(&relay).on_done(rule, done);
        }
    });
    parser.parse_guard(GUARD, move |_ctx| !abort.is_aborted() && !stop.is_aborted());
}

/// Lock the relay, recovering from a poisoned lock: the engine catches a
/// panicking subscriber and fails the parse, and the state is still
/// needed to report that.
fn lock<S>(relay: &Mutex<Relay<S>>) -> MutexGuard<'_, Relay<S>> {
    relay
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// The pass as alchemy's [`RulePass`], its sizes held to
/// `max_scalar_bytes`.
fn pass(rule: &Rule, done: &RuleDone, open: bool, max: usize) -> Result<RulePass, Fail> {
    let matched = if open { &rule.o } else { &rule.c };
    let b = done.alt.as_ref().map_or(0, |a| a.b);
    let kept = &matched[..matched.len().saturating_sub(b)];
    let mut tokens = Vec::with_capacity(kept.len());
    for t in kept {
        let src = t.src.as_str();
        held(src.len(), "a token's source text", max)?;
        let val = scalar_of(&t.val, Some(src));
        if let Datum::String(s) = &val {
            held(s.len(), "a token's value", max)?;
        }
        tokens.push(PassToken {
            name: t.name.as_str().into(),
            val,
            src: src.into(),
            row: t.site.ri as u64,
            col: t.site.ci as u64,
        });
    }
    let value = if open {
        None
    } else {
        let node = rule.node.borrow();
        match &*node {
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) | Value::Text(_) => {
                let lexeme = rule.o0().map(|t| t.src.as_str());
                let value = scalar_of(&node, lexeme);
                if let Datum::String(s) = &value {
                    held(s.len(), "a rule's value", max)?;
                }
                Some(value)
            }
            _ => None,
        }
    };
    let mut n: Vec<(Box<str>, i64)> = rule
        .n
        .iter()
        .map(|(k, v)| (k.as_str().into(), *v as i64))
        .collect();
    n.sort();
    Ok(RulePass {
        name: rule.name.as_str().into(),
        d: rule.d as u64,
        i: rule.i as u64,
        alt: done.alt.as_ref().map(|a| PassAlt {
            p: a.p.as_str().into(),
            r: a.r.as_str().into(),
            b: a.b as u64,
            g: a.g.iter().map(|g| g.as_str().into()).collect(),
        }),
        tokens,
        forced: done.forced,
        n,
        value,
    })
}

/// An engine value as a pass carries it: `Undefined` as null, and a number
/// with `lexeme` when that text is a JSON number spelling it.
fn scalar_of(value: &Value, lexeme: Option<&str>) -> Datum {
    match value {
        Value::Number(n) => Datum::Number {
            value: *n,
            lexeme: lexeme
                .filter(|src| is_json_number(src) && src.parse::<f64>().ok() == Some(*n))
                .map(Into::into),
        },
        other => Datum::from_tabnas(other),
    }
}

/// `max_scalar_bytes` held on one text a pass carries.
fn held(bytes: usize, what: &str, max: usize) -> Result<(), Fail> {
    if bytes > max {
        return Err(Fail::limit(
            "max_scalar_bytes",
            max as u64,
            format!("{what} of {bytes} bytes is larger than {max}"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Code;

    /// A recorder of the events.
    #[derive(Default)]
    struct Seen(Vec<ParserEvent>);

    impl ParserSink for Seen {
        fn event(&mut self, ev: &ParserEvent) -> Result<Flow, Fail> {
            self.0.push(ev.clone());
            Ok(Flow::Continue)
        }
    }

    /// The events, each in the fixture encoding.
    fn encoded(events: &[ParserEvent]) -> Vec<String> {
        events.iter().map(|e| e.to_json().to_string()).collect()
    }

    fn passes(src: &str) -> (Result<Flow, Fail>, Vec<ParserEvent>) {
        let (r, seen) = ParserEventSource::new(tabnas_json::make(), src).run_owned(Seen::default());
        (r, seen.0)
    }

    #[test]
    fn a_root_scalar_is_its_val_rules_two_passes_then_end() {
        let (r, seen) = passes(" 2.50 ");
        assert_eq!(r.unwrap(), Flow::Continue);
        let text = encoded(&seen);
        // The open pass consumed the number, whose source text is its
        // lexeme; the close consumed the end of the source, which has no
        // value, and carries the node's value.
        assert_eq!(
            text,
            [
                r##"["open",{"name":"val","d":0,"i":0,"alt":{"p":"","r":"","b":0,"g":["val","json"]},"tokens":[["#NR",2.5,"2.50",1,2]],"forced":false,"n":{}}]"##,
                r##"["close",{"name":"val","d":0,"i":0,"alt":{"p":"","r":"","b":0,"g":["end","json"]},"tokens":[["#ZZ",null,"",1,7]],"forced":false,"n":{},"value":2.5}]"##,
                r#"["end"]"#,
            ]
        );
        // The encoding writes a value as JSON; the lexeme is the event's.
        let ParserEvent::Open(open) = &seen[0] else {
            panic!("an open pass");
        };
        assert_eq!(
            open.tokens[0].val,
            Datum::Number {
                value: 2.5,
                lexeme: Some("2.50".into())
            }
        );
    }

    #[test]
    fn a_number_keeps_its_lexeme_only_when_it_is_a_json_number() {
        assert_eq!(
            scalar_of(&Value::Number(2.5), Some("2.50")),
            Datum::Number {
                value: 2.5,
                lexeme: Some("2.50".into())
            }
        );
        for src in ["0x10", "1_0", "+1", "2.5", ""] {
            let datum = scalar_of(&Value::Number(16.0), Some(src));
            assert_eq!(
                datum,
                Datum::Number {
                    value: 16.0,
                    lexeme: None
                },
                "{src:?}"
            );
        }
        assert_eq!(scalar_of(&Value::Undefined, Some("")), Datum::Null);
    }

    #[test]
    fn a_parse_error_is_invalid_input_after_the_passes_before_it() {
        let (r, seen) = passes("[1,\n ]");
        let err = r.unwrap_err();
        assert_eq!(err.code, Code::InputInvalid);
        assert_eq!(err.row, Some(2));
        assert!(!seen.is_empty());
        assert!(!seen.contains(&ParserEvent::End));
    }

    #[test]
    fn a_huge_token_is_refused_by_name_before_the_sink_sees_it() {
        let limits = Limits {
            max_scalar_bytes: 3,
            ..Limits::default()
        };
        let (r, seen) = ParserEventSource::new(tabnas_json::make(), r#"["abcd"]"#)
            .limits(limits)
            .run_owned(Seen::default());
        let err = r.unwrap_err();
        assert_eq!(err.code, Code::ResourceLimitExceeded);
        assert_eq!(err.limit.unwrap().name, "max_scalar_bytes");
        assert!(!encoded(&seen.0).iter().any(|e| e.contains("abcd")));
    }

    #[test]
    fn a_stop_stops_the_parse_and_a_failure_comes_back() {
        struct StopAt(usize, usize);
        impl ParserSink for StopAt {
            fn event(&mut self, _: &ParserEvent) -> Result<Flow, Fail> {
                self.1 += 1;
                Ok(if self.1 == self.0 {
                    Flow::Stop
                } else {
                    Flow::Continue
                })
            }
        }
        let (r, sink) =
            ParserEventSource::new(tabnas_json::make(), "[1,2,3]").run_owned(StopAt(2, 0));
        assert_eq!(r.unwrap(), Flow::Stop);
        assert_eq!(sink.1, 2);

        struct Refuse;
        impl ParserSink for Refuse {
            fn event(&mut self, _: &ParserEvent) -> Result<Flow, Fail> {
                Err(Fail::output("disk full"))
            }
        }
        let (r, _) = ParserEventSource::new(tabnas_json::make(), "[1]").run_owned(Refuse);
        assert_eq!(r.unwrap_err().code, Code::OutputFailed);
    }

    #[test]
    fn an_aborted_flag_cancels_the_parse_as_aborted() {
        let abort = AbortFlag::new();
        abort.abort();
        let (r, seen) = ParserEventSource::new(tabnas_json::make(), "[1]")
            .abort(abort)
            .run_owned(Seen::default());
        assert_eq!(r.unwrap_err().code, Code::Aborted);
        assert!(seen.0.is_empty());
    }

    #[test]
    fn metrics_count_the_events_delivered() {
        let metrics = Metrics::new();
        let (r, seen) = ParserEventSource::new(tabnas_json::make(), r#"{"a":[1]}"#)
            .metrics(metrics.clone())
            .run_owned(Seen::default());
        r.unwrap();
        assert_eq!(Metrics::get(&metrics.events), seen.0.len() as u64);
    }
}
