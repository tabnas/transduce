//! Captures: recognize selected scopes and deliver them, complete, in
//! source order.
//!
//! A [`Router`] is a [`Sink`] that feeds every event through one
//! [`Matcher`] for all of its [`CaptureSpec`]s and hands each completed
//! match to a [`RouteSink`]. A `Materialize` capture is built into a
//! [`Datum`] under a byte budget and delivered when its value completes;
//! an `Observe` capture delivers only the path, at the value's end, and
//! retains nothing. Materialized captures may not nest or coincide: the
//! router holds at most one value at a time, which is what makes its
//! retention one selected scope rather than a document. That is checked
//! at construction, conservatively, with [`Selector::may_overlap`], and
//! again at run time so a stream the check could not foresee still fails
//! rather than mixing two values.

use std::fmt;
use std::sync::Arc;

use crate::datum::{Datum, DatumBuilder, Duplicates};
use crate::error::{Code, Fail};
use crate::event::JsonEvent;
use crate::limits::{Limits, Metrics};
use crate::matcher::{CaptureId, HitKind, Matcher};
use crate::selector::{Path, Selector};
use crate::sink::{Flow, Sink};

/// What a capture keeps of its match.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CaptureMode {
    /// Build the value and deliver it whole.
    Materialize,
    /// Deliver only that the value occurred, and where, when it ends.
    Observe,
}

/// The byte budget one materialized capture may not exceed, named after
/// the `Limits` field the failure reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Budget {
    pub bytes: usize,
    pub name: &'static str,
}

/// One capture: a tag for the consumer, the selector to match, the mode.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CaptureSpec {
    pub tag: Box<str>,
    pub selector: Selector,
    pub mode: CaptureMode,
    /// The budget for a `Materialize` capture; `None` takes the router's
    /// `max_capture_bytes`. A stage with a more specific limit (a table's
    /// rows under `max_record_bytes`) sets it so the failure names that.
    pub budget: Option<Budget>,
}

impl CaptureSpec {
    pub fn new(tag: impl Into<Box<str>>, selector: Selector, mode: CaptureMode) -> CaptureSpec {
        CaptureSpec {
            tag: tag.into(),
            selector,
            mode,
            budget: None,
        }
    }

    pub fn materialize(tag: impl Into<Box<str>>, selector: Selector) -> CaptureSpec {
        CaptureSpec::new(tag, selector, CaptureMode::Materialize)
    }

    pub fn observe(tag: impl Into<Box<str>>, selector: Selector) -> CaptureSpec {
        CaptureSpec::new(tag, selector, CaptureMode::Observe)
    }

    pub fn budget(mut self, bytes: usize, name: &'static str) -> CaptureSpec {
        self.budget = Some(Budget { bytes, name });
        self
    }
}

/// One completed match.
#[derive(Clone, Debug, PartialEq)]
pub struct Selected {
    /// The spec's position in the router's list, for dispatch without a
    /// string comparison.
    pub id: CaptureId,
    pub tag: Box<str>,
    pub path: Path,
    /// The value for a `Materialize` capture; `None` for `Observe`.
    pub value: Option<Datum>,
}

/// The consumer of a router's matches.
pub trait RouteSink {
    /// A capture's value is beginning at `path`'s position. Nothing has
    /// been retained for it yet, so a consumer that knows the value is
    /// out of order can refuse it here at no cost; the router adds the
    /// path to a failure that has none.
    fn began(&mut self, id: CaptureId, tag: &str) -> Result<(), Fail> {
        let _ = (id, tag);
        Ok(())
    }

    fn selected(&mut self, selected: Selected) -> Result<Flow, Fail>;

    /// The document ended, validated. Exactly once, after the last match.
    fn end(&mut self) -> Result<Flow, Fail>;
}

impl RouteSink for Vec<Selected> {
    fn selected(&mut self, selected: Selected) -> Result<Flow, Fail> {
        self.push(selected);
        Ok(Flow::Continue)
    }

    fn end(&mut self) -> Result<Flow, Fail> {
        Ok(Flow::Continue)
    }
}

/// A route sink made of a closure; `end` is a no-op.
pub struct FnRoute<F>(pub F);

impl<F> RouteSink for FnRoute<F>
where
    F: FnMut(Selected) -> Result<Flow, Fail>,
{
    fn selected(&mut self, selected: Selected) -> Result<Flow, Fail> {
        (self.0)(selected)
    }

    fn end(&mut self) -> Result<Flow, Fail> {
        Ok(Flow::Continue)
    }
}

/// The one materialization in progress.
struct Active {
    id: CaptureId,
    builder: DatumBuilder,
}

/// Recognizes every capture in one pass and delivers completed matches.
pub struct Router<D: RouteSink> {
    specs: Vec<CaptureSpec>,
    matcher: Matcher,
    downstream: D,
    active: Option<Active>,
    /// Observed values in progress: `(capture, depth)`, innermost last.
    observing: Vec<(CaptureId, usize)>,
    max_depth: usize,
    max_capture_bytes: usize,
    duplicates: Duplicates,
    metrics: Arc<Metrics>,
    ended: bool,
}

impl<D: RouteSink> fmt::Debug for Router<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Router")
            .field("specs", &self.specs)
            .field("depth", &self.matcher.depth())
            .field("ended", &self.ended)
            .finish_non_exhaustive()
    }
}

impl<D: RouteSink> Router<D> {
    /// Build a router; two specs that may overlap are refused unless both
    /// observe.
    pub fn new(
        specs: Vec<CaptureSpec>,
        limits: &Limits,
        duplicates: Duplicates,
        metrics: Arc<Metrics>,
        downstream: D,
    ) -> Result<Router<D>, Fail> {
        for (i, a) in specs.iter().enumerate() {
            for b in &specs[i + 1..] {
                let both_observe = a.mode == CaptureMode::Observe && b.mode == CaptureMode::Observe;
                if !both_observe && a.selector.may_overlap(&b.selector) {
                    return Err(Fail::new(
                        Code::CaptureOverlapUnsupported,
                        format!(
                            "captures {:?} ({}) and {:?} ({}) may select overlapping scopes; only observed captures may overlap",
                            a.tag, a.selector, b.tag, b.selector
                        ),
                    ));
                }
            }
        }
        let matcher = Matcher::new(&specs.iter().map(|s| s.selector.clone()).collect::<Vec<_>>());
        Ok(Router {
            specs,
            matcher,
            downstream,
            active: None,
            observing: Vec::new(),
            max_depth: limits.max_depth,
            max_capture_bytes: limits.max_capture_bytes,
            duplicates,
            metrics,
            ended: false,
        })
    }

    pub fn specs(&self) -> &[CaptureSpec] {
        &self.specs
    }

    pub fn downstream(&self) -> &D {
        &self.downstream
    }

    pub fn downstream_mut(&mut self) -> &mut D {
        &mut self.downstream
    }

    /// Whether `End` has been delivered.
    pub fn ended(&self) -> bool {
        self.ended
    }

    pub fn into_inner(self) -> D {
        self.downstream
    }

    /// Start the capture `id` at the value beginning at `depth`.
    fn begin(&mut self, id: CaptureId, depth: usize) -> Result<(), Fail> {
        let spec = &self.specs[id];
        if let Err(mut fail) = self.downstream.began(id, &spec.tag) {
            if fail.path.is_none() {
                fail.path = Some(self.matcher.path(depth).to_string());
            }
            return Err(fail);
        }
        match spec.mode {
            CaptureMode::Observe => {
                self.observing.push((id, depth));
                Ok(())
            }
            CaptureMode::Materialize => {
                if let Some(active) = &self.active {
                    let path = self.matcher.path(depth);
                    return Err(Fail::new(
                        Code::CaptureOverlapUnsupported,
                        format!(
                            "capture {:?} began at {path} while capture {:?} was still being materialized",
                            spec.tag, self.specs[active.id].tag
                        ),
                    )
                    .at_path(path.to_string()));
                }
                let budget = spec.budget.unwrap_or(Budget {
                    bytes: self.max_capture_bytes,
                    name: "max_capture_bytes",
                });
                let builder = DatumBuilder::new(budget.bytes, budget.name, self.duplicates)
                    .at(self.matcher.path(depth));
                self.active = Some(Active { id, builder });
                Ok(())
            }
        }
    }

    fn deliver(&mut self, id: CaptureId, path: Path, value: Option<Datum>) -> Result<Flow, Fail> {
        let selected = Selected {
            id,
            tag: self.specs[id].tag.clone(),
            path,
            value,
        };
        self.downstream.selected(selected)
    }
}

impl<D: RouteSink> Sink for Router<D> {
    fn event(&mut self, ev: JsonEvent<'_>) -> Result<Flow, Fail> {
        let hit = self.matcher.event(ev)?;
        Metrics::add(&self.metrics.events, 1);
        match hit.kind {
            HitKind::Key => Metrics::add(&self.metrics.keys, 1),
            HitKind::Scalar => Metrics::add(&self.metrics.scalars, 1),
            HitKind::Start if hit.depth + 1 > self.max_depth => {
                let path = self.matcher.path(hit.depth);
                return Err(Fail::limit(
                    "max_depth",
                    self.max_depth as u64,
                    format!(
                        "a container at {path} is nested deeper than {}",
                        self.max_depth
                    ),
                )
                .at_path(path.to_string()));
            }
            _ => {}
        }
        for k in 0..hit.begins {
            let id = self.matcher.begins()[k];
            self.begin(id, hit.depth)?;
        }
        if let Some(active) = &mut self.active {
            active.builder.event(ev)?;
            if active.builder.finished() {
                let bytes = active.builder.bytes() as u64;
                let id = active.id;
                // `finished()` held, so the builder has the value.
                let value = active.builder.take();
                self.active = None;
                let path = self.matcher.path(hit.depth);
                self.metrics.capture(bytes);
                let flow = self.deliver(id, path, value);
                self.metrics.release(bytes);
                if flow? == Flow::Stop {
                    return Ok(Flow::Stop);
                }
            }
        }
        if matches!(hit.kind, HitKind::Scalar | HitKind::Close) {
            while self
                .observing
                .last()
                .is_some_and(|&(_, depth)| depth == hit.depth)
            {
                // The loop condition just saw the entry.
                let (id, _) = self.observing.pop().unwrap_or_default();
                let path = self.matcher.path(hit.depth);
                if self.deliver(id, path, None)? == Flow::Stop {
                    return Ok(Flow::Stop);
                }
            }
        }
        if hit.kind == HitKind::End {
            self.ended = true;
            return self.downstream.end();
        }
        Ok(Flow::Continue)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::event::OwnedJsonEvent;
    use crate::sink::replay;

    /// The spec's worked example: metadata first, then records whose
    /// member order differs, one number with a lexeme worth keeping.
    pub(crate) fn worked_example() -> Vec<OwnedJsonEvent> {
        use OwnedJsonEvent::*;
        let key = |k: &str| Key(k.into());
        let s = |v: &str| String(v.into());
        let n = |v: f64, l: &str| Number {
            value: v,
            lexeme: Some(l.into()),
        };
        vec![
            ObjectStart,
            key("response"),
            ObjectStart,
            key("metadata"),
            ObjectStart,
            key("fields"),
            ArrayStart,
            ObjectStart,
            key("title"),
            s("Identifier"),
            key("path"),
            ArrayStart,
            s("id"),
            ArrayEnd,
            ObjectEnd,
            ObjectStart,
            key("title"),
            s("Full name"),
            key("path"),
            ArrayStart,
            s("person"),
            s("name"),
            ArrayEnd,
            ObjectEnd,
            ObjectStart,
            key("title"),
            s("Balance"),
            key("path"),
            ArrayStart,
            s("account"),
            s("balance"),
            ArrayEnd,
            ObjectEnd,
            ArrayEnd,
            ObjectEnd,
            key("payload"),
            ObjectStart,
            key("deep"),
            ObjectStart,
            key("records"),
            ArrayStart,
            ObjectStart,
            key("id"),
            n(123.0, "123"),
            key("person"),
            ObjectStart,
            key("name"),
            s("Alice"),
            ObjectEnd,
            key("account"),
            ObjectStart,
            key("balance"),
            n(50.25, "50.25"),
            ObjectEnd,
            ObjectEnd,
            ObjectStart,
            key("account"),
            ObjectStart,
            key("balance"),
            Number {
                value: 72.0,
                lexeme: None,
            },
            ObjectEnd,
            key("id"),
            n(456.0, "456"),
            key("person"),
            ObjectStart,
            key("name"),
            s("Bob"),
            ObjectEnd,
            ObjectEnd,
            ArrayEnd,
            ObjectEnd,
            ObjectEnd,
            ObjectEnd,
            ObjectEnd,
            End,
        ]
    }

    pub(crate) fn metadata_selector() -> Selector {
        Selector::root()
            .property("response")
            .property("metadata")
            .property("fields")
    }

    pub(crate) fn records_selector() -> Selector {
        Selector::root()
            .property("response")
            .property("payload")
            .property("deep")
            .property("records")
            .each_index()
    }

    fn router(specs: Vec<CaptureSpec>) -> Result<Router<Vec<Selected>>, Fail> {
        Router::new(
            specs,
            &Limits::default(),
            Duplicates::Reject,
            Metrics::new(),
            Vec::new(),
        )
    }

    #[test]
    fn the_worked_example_delivers_metadata_then_each_record_in_order() {
        let mut r = router(vec![
            CaptureSpec::materialize("meta", metadata_selector()),
            CaptureSpec::materialize("row", records_selector()),
        ])
        .unwrap();
        assert_eq!(replay(&worked_example(), &mut r).unwrap(), Flow::Continue);
        assert!(r.ended());
        let out = r.into_inner();
        let got: Vec<(String, String, String)> = out
            .iter()
            .map(|s| {
                (
                    s.tag.to_string(),
                    s.path.to_string(),
                    s.value.as_ref().unwrap().to_string(),
                )
            })
            .collect();
        assert_eq!(
            got,
            vec![
                (
                    "meta".into(),
                    ".response.metadata.fields".into(),
                    r#"[{"title":"Identifier","path":["id"]},{"title":"Full name","path":["person","name"]},{"title":"Balance","path":["account","balance"]}]"#.into()
                ),
                (
                    "row".into(),
                    ".response.payload.deep.records[0]".into(),
                    r#"{"id":123,"person":{"name":"Alice"},"account":{"balance":50.25}}"#.into()
                ),
                (
                    "row".into(),
                    ".response.payload.deep.records[1]".into(),
                    r#"{"account":{"balance":72},"id":456,"person":{"name":"Bob"}}"#.into()
                ),
            ]
        );
        assert_eq!(out[0].id, 0);
        assert_eq!(out[1].id, 1);
    }

    #[test]
    fn overlapping_materializations_are_rejected_at_construction_both_ways() {
        let rows = Selector::root().property("records").each_index();
        let inner = Selector::root().property("records").index(2).property("x");
        for (a, b) in [(rows.clone(), inner.clone()), (inner, rows.clone())] {
            let err = router(vec![
                CaptureSpec::materialize("a", a),
                CaptureSpec::materialize("b", b),
            ])
            .unwrap_err();
            assert_eq!(err.code, Code::CaptureOverlapUnsupported);
        }
        let err = router(vec![
            CaptureSpec::observe("a", rows.clone()),
            CaptureSpec::materialize("b", Selector::root()),
        ])
        .unwrap_err();
        assert_eq!(err.code, Code::CaptureOverlapUnsupported);
        assert!(router(vec![
            CaptureSpec::observe("a", rows.clone()),
            CaptureSpec::observe("b", Selector::root()),
        ])
        .is_ok());
        assert!(router(vec![
            CaptureSpec::materialize("a", rows),
            CaptureSpec::materialize("b", Selector::root().property("meta")),
        ])
        .is_ok());
    }

    #[test]
    fn a_capture_beginning_inside_a_materialization_fails_at_run_time() {
        // Two identical selectors pass no construction check; force the
        // runtime path by building the router around the check.
        let specs = vec![
            CaptureSpec::materialize("outer", Selector::root().property("a")),
            CaptureSpec::materialize("inner", Selector::root().property("a").property("b")),
        ];
        let limits = Limits::default();
        let mut r = Router {
            matcher: Matcher::new(&[specs[0].selector.clone(), specs[1].selector.clone()]),
            specs,
            downstream: Vec::<Selected>::new(),
            active: None,
            observing: Vec::new(),
            max_depth: limits.max_depth,
            max_capture_bytes: limits.max_capture_bytes,
            duplicates: Duplicates::Reject,
            metrics: Metrics::new(),
            ended: false,
        };
        let events = [
            OwnedJsonEvent::ObjectStart,
            OwnedJsonEvent::Key("a".into()),
            OwnedJsonEvent::ObjectStart,
            OwnedJsonEvent::Key("b".into()),
            OwnedJsonEvent::Null,
        ];
        let err = replay(&events, &mut r).unwrap_err();
        assert_eq!(err.code, Code::CaptureOverlapUnsupported);
        assert_eq!(err.path.as_deref(), Some(".a.b"));
    }

    #[test]
    fn a_capture_over_its_budget_names_the_limit() {
        let limits = Limits {
            max_capture_bytes: 40,
            ..Limits::default()
        };
        let mut r = Router::new(
            vec![CaptureSpec::materialize("row", records_selector())],
            &limits,
            Duplicates::Reject,
            Metrics::new(),
            Vec::<Selected>::new(),
        )
        .unwrap();
        let err = replay(&worked_example(), &mut r).unwrap_err();
        assert_eq!(err.code, Code::ResourceLimitExceeded);
        assert_eq!(err.limit.as_ref().unwrap().name, "max_capture_bytes");
        assert_eq!(err.limit.as_ref().unwrap().value, 40);
        assert_eq!(
            err.path.as_deref(),
            Some(".response.payload.deep.records[0]")
        );
    }

    #[test]
    fn a_spec_budget_overrides_the_router_limit_and_its_name() {
        let mut r =
            router(vec![CaptureSpec::materialize("meta", metadata_selector())
                .budget(10, "max_metadata_bytes")])
            .unwrap();
        let err = replay(&worked_example(), &mut r).unwrap_err();
        assert_eq!(err.limit.as_ref().unwrap().name, "max_metadata_bytes");
    }

    #[test]
    fn depth_over_the_limit_names_max_depth() {
        let limits = Limits {
            max_depth: 3,
            ..Limits::default()
        };
        let mut r = Router::new(
            vec![],
            &limits,
            Duplicates::Reject,
            Metrics::new(),
            Vec::<Selected>::new(),
        )
        .unwrap();
        let fine = [
            OwnedJsonEvent::ArrayStart,
            OwnedJsonEvent::ArrayStart,
            OwnedJsonEvent::ArrayStart,
        ];
        replay(&fine, &mut r).unwrap();
        let err = r.event(JsonEvent::ArrayStart).unwrap_err();
        assert_eq!(err.code, Code::ResourceLimitExceeded);
        assert_eq!(err.limit.as_ref().unwrap().name, "max_depth");
        assert_eq!(err.path.as_deref(), Some("[0][0][0]"));
    }

    #[test]
    fn observe_delivers_paths_at_the_value_end_and_nests() {
        let mut r = router(vec![
            CaptureSpec::observe("row", records_selector()),
            CaptureSpec::observe(
                "records",
                Selector::root()
                    .property("response")
                    .property("payload")
                    .property("deep")
                    .property("records"),
            ),
            CaptureSpec::observe("title", metadata_selector().each_index().property("title")),
        ])
        .unwrap();
        replay(&worked_example(), &mut r).unwrap();
        let out = r.into_inner();
        let got: Vec<(String, String)> = out
            .iter()
            .map(|s| {
                assert!(s.value.is_none());
                (s.tag.to_string(), s.path.to_string())
            })
            .collect();
        assert_eq!(
            got,
            vec![
                ("title".into(), ".response.metadata.fields[0].title".into()),
                ("title".into(), ".response.metadata.fields[1].title".into()),
                ("title".into(), ".response.metadata.fields[2].title".into()),
                ("row".into(), ".response.payload.deep.records[0]".into()),
                ("row".into(), ".response.payload.deep.records[1]".into()),
                ("records".into(), ".response.payload.deep.records".into()),
            ]
        );
    }

    #[test]
    fn a_stop_from_downstream_stops_the_router() {
        struct TakeOne(usize);
        impl RouteSink for TakeOne {
            fn selected(&mut self, _s: Selected) -> Result<Flow, Fail> {
                self.0 += 1;
                Ok(Flow::Stop)
            }
            fn end(&mut self) -> Result<Flow, Fail> {
                panic!("end must not follow a stop")
            }
        }
        let mut r = Router::new(
            vec![CaptureSpec::materialize("row", records_selector())],
            &Limits::default(),
            Duplicates::Reject,
            Metrics::new(),
            TakeOne(0),
        )
        .unwrap();
        assert_eq!(replay(&worked_example(), &mut r).unwrap(), Flow::Stop);
        assert_eq!(r.downstream().0, 1);
        assert!(!r.ended());
    }

    #[test]
    fn end_is_delivered_exactly_once_and_a_route_with_no_specs_is_valid() {
        struct Ends(u32);
        impl RouteSink for Ends {
            fn selected(&mut self, _s: Selected) -> Result<Flow, Fail> {
                panic!("nothing is selected")
            }
            fn end(&mut self) -> Result<Flow, Fail> {
                self.0 += 1;
                Ok(Flow::Continue)
            }
        }
        let mut r = Router::new(
            vec![],
            &Limits::default(),
            Duplicates::Reject,
            Metrics::new(),
            Ends(0),
        )
        .unwrap();
        replay(&worked_example(), &mut r).unwrap();
        assert_eq!(r.downstream().0, 1);
        assert!(r.ended());
        assert_eq!(
            r.event(JsonEvent::End).unwrap_err().code,
            Code::ProtocolOrderError
        );
        assert_eq!(r.downstream().0, 1);
    }

    #[test]
    fn malformed_streams_are_protocol_errors_before_anything_is_delivered() {
        let mut r = router(vec![CaptureSpec::materialize("all", Selector::root())]).unwrap();
        let err = replay(
            &[OwnedJsonEvent::ObjectStart, OwnedJsonEvent::ArrayEnd],
            &mut r,
        )
        .unwrap_err();
        assert_eq!(err.code, Code::ProtocolOrderError);
        assert!(r.into_inner().is_empty());
    }

    #[test]
    fn duplicates_follow_the_policy_inside_a_capture() {
        let events = [
            OwnedJsonEvent::ObjectStart,
            OwnedJsonEvent::Key("a".into()),
            OwnedJsonEvent::Number {
                value: 1.0,
                lexeme: None,
            },
            OwnedJsonEvent::Key("a".into()),
            OwnedJsonEvent::Number {
                value: 2.0,
                lexeme: None,
            },
            OwnedJsonEvent::ObjectEnd,
            OwnedJsonEvent::End,
        ];
        let run = |policy| {
            let mut r = Router::new(
                vec![CaptureSpec::materialize("all", Selector::root())],
                &Limits::default(),
                policy,
                Metrics::new(),
                Vec::<Selected>::new(),
            )
            .unwrap();
            replay(&events, &mut r).map(|_| r.into_inner()[0].value.clone().unwrap().to_string())
        };
        assert_eq!(
            run(Duplicates::Reject).unwrap_err().code,
            Code::DuplicateMember
        );
        assert_eq!(run(Duplicates::LastWins).unwrap(), r#"{"a":2}"#);
        assert_eq!(run(Duplicates::FirstWins).unwrap(), r#"{"a":1}"#);
    }

    #[test]
    fn metrics_count_events_keys_scalars_and_capture_bytes() {
        let metrics = Metrics::new();
        let mut r = Router::new(
            vec![CaptureSpec::materialize("row", records_selector())],
            &Limits::default(),
            Duplicates::Reject,
            metrics.clone(),
            Vec::<Selected>::new(),
        )
        .unwrap();
        let events = worked_example();
        replay(&events, &mut r).unwrap();
        assert_eq!(Metrics::get(&metrics.events), events.len() as u64);
        assert_eq!(
            Metrics::get(&metrics.keys),
            events
                .iter()
                .filter(|e| matches!(e, OwnedJsonEvent::Key(_)))
                .count() as u64
        );
        assert_eq!(
            Metrics::get(&metrics.scalars),
            events.iter().filter(|e| e.as_event().is_scalar()).count() as u64
        );
        assert_eq!(Metrics::get(&metrics.captured_bytes), 0);
        let biggest = r
            .into_inner()
            .iter()
            .map(|s| s.value.as_ref().unwrap().byte_size())
            .max()
            .unwrap() as u64;
        assert_eq!(Metrics::get(&metrics.captured_bytes_high), biggest);
    }

    #[test]
    fn a_scalar_capture_is_delivered_whole() {
        let mut r = router(vec![CaptureSpec::materialize(
            "v",
            Selector::root().property("v"),
        )])
        .unwrap();
        let events = [
            OwnedJsonEvent::ObjectStart,
            OwnedJsonEvent::Key("v".into()),
            OwnedJsonEvent::Number {
                value: 1.5,
                lexeme: Some("1.50".into()),
            },
            OwnedJsonEvent::ObjectEnd,
            OwnedJsonEvent::End,
        ];
        replay(&events, &mut r).unwrap();
        let out = r.into_inner();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].value.as_ref().unwrap().to_string(), "1.50");
        assert_eq!(
            out[0].value,
            Some(Datum::Number {
                value: 1.5,
                lexeme: Some("1.50".into())
            })
        );
    }
}
