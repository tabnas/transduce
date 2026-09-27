//! The rule-event adapter: `JsonEvents/1` from a live tabnas parse.
//!
//! The engine tells a `ruleDone` subscriber about every rule pass, with
//! the rule's node: the shared cell (`Rc<RefCell<Value>>`) the rule
//! accumulates its value in. The adapter watches those cells and nothing
//! grammar-specific, which is what lets one adapter serve every grammar.
//! The algorithm is the measured prototype's, kept exactly:
//!
//! - A rule whose node is a container at the end of its OPEN pass, in a
//!   cell not already on the frame stack, STARTS a container. Pushed and
//!   replaced rules share their parent's cell, so a pair or an element
//!   rule starts nothing.
//! - At the end of a CLOSE pass whose node cell is the top frame's, a
//!   longer container means new entries: the tail of an array, the last
//!   members of a map (insertion ordered). A new entry that is the
//!   container which completed last was already streamed; any other is
//!   walked whole, late. Verified grammars never take that path.
//! - A key is announced early when the rule stashed it in `u["key"]` at
//!   its OPEN pass (`@key$` and the grammars' own actions do); a key the
//!   adapter only learns at insertion is announced then.
//! - The top frame ENDS when a rule at the frame's depth whose node is the
//!   frame's cell closes, or the rule that started it closes, unless that
//!   close REPLACES the rule (`alt.r`): a replaced rule hands its cell to
//!   its successor and the container goes on. This is a refinement over
//!   the prototype, which ended a frame on the frame rule's close alone
//!   and so closed YAML's `yamlElemMap` when it became `yamlElemPair`.
//! - A root scalar is emitted at the depth-0 rule's close, unless that
//!   close replaces the rule: jsonic's `1,2,3` closes `val` with `1` and
//!   replaces it by `list`, which promotes the value into a list in the
//!   same cell, so the scalar is not the root. A frame therefore streams
//!   the entries already in its container when it starts (the promoted
//!   value), and a frame that starts at the root after a root value has
//!   completed is refused with `STREAMABILITY_UNKNOWN`: its first element
//!   was streamed before the array's start and cannot be taken back.
//!   `End` is not the adapter's to emit: the source sends it after the
//!   engine has returned `Ok`, so a document is complete only when it
//!   validated.
//!
//! Number lexemes are best effort: at the close of a rule whose node is a
//! `Number`, the first open token's source text is kept when it is a JSON
//! number that parses to the same value, and attached to that number when
//! it is inserted next. A number the adapter learns of any other way has
//! no lexeme. The lexeme is dropped at every container boundary and every
//! late walk, so it never crosses to another value.
//!
//! Pruning drops the elements of an array frame from the shared cell
//! after they were emitted, so a document's rows do not pile up in the
//! engine's tree while the transducer streams them. It changes the value
//! the engine returns, which is why only the incremental source, which
//! discards that value, ever asks for it.
//!
//! The subscriber must be `Fn + Send + Sync + 'static`, so the adapter
//! sits behind an `Arc<Mutex<_>>`; the parse runs on one thread and the
//! lock is never contended. The subscriber holds only a `Weak` reference,
//! so once the parse has returned the driver provably holds the only
//! strong one and takes the adapter, and the sink, back.

use std::rc::Rc;
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use tabnas::{Rule, RuleDone, RuleState, Tabnas, Value};

use crate::error::{Code, Fail};
use crate::event::{JsonEvent, Number};
use crate::limits::{AbortFlag, Limits, Metrics};
use crate::matcher::{HitKind, Matcher};
use crate::selector::{Selector, Step};
use crate::sink::{Flow, Sink};
use crate::source::guard::Guarded;
use crate::source::Prune;

/// The name the adapter's parse guard is installed under.
pub(crate) const GUARD: &str = "tabnas-transduce.abort";

/// One open container.
#[derive(Debug)]
struct Frame {
    cell: usize,
    array: bool,
    len: usize,
    rule_i: usize,
    depth: usize,
    /// The key announced early for the next member, when `has_key`.
    key: String,
    has_key: bool,
    prune: bool,
}

/// How the run stands, as seen from inside the callbacks.
#[derive(Debug)]
pub(crate) enum Status {
    Running,
    /// The sink answered `Stop`.
    Stopped,
    /// The sink or a limit failed; the parse was told to cancel.
    Failed(Fail),
}

enum PruneState {
    Never,
    All,
    /// Arrays at the locations this matcher names.
    Under(Matcher),
}

/// The adapter's state, shared with the engine's subscriber.
pub(crate) struct Adapter<S: Sink> {
    frames: Vec<Frame>,
    open: usize,
    /// The container that completed last, by `Arc` pointer: the next entry
    /// inserted into its parent is this one, already streamed.
    last_completed: Option<usize>,
    lexeme: String,
    lexeme_value: f64,
    lexeme_ready: bool,
    sink: Guarded<S>,
    status: Status,
    /// Set to make the parse guard cancel the parse.
    stop: AbortFlag,
    prune: PruneState,
    /// Whether the last `Start` the prune matcher saw named a pruned array.
    prune_hit: bool,
    /// A whole root value (a scalar, or the outermost frame) has been emitted.
    root_done: bool,
}

impl<S: Sink + Send + 'static> Adapter<S> {
    pub(crate) fn new(
        sink: S,
        limits: &Limits,
        abort: AbortFlag,
        metrics: Arc<Metrics>,
        prune: &Prune,
        stop: AbortFlag,
    ) -> Adapter<S> {
        let prune = match prune {
            Prune::Never => PruneState::Never,
            Prune::AllArrays => PruneState::All,
            Prune::Under(selector) => {
                // A selector that names the elements names the array one
                // step up; one that names the array is taken as is.
                let steps = selector.steps();
                let array = match steps.last() {
                    Some(Step::EachIndex) => Selector(steps[..steps.len() - 1].to_vec()),
                    _ => selector.clone(),
                };
                PruneState::Under(Matcher::new(&[array]))
            }
        };
        Adapter {
            frames: Vec::new(),
            open: 0,
            last_completed: None,
            lexeme: String::new(),
            lexeme_value: 0.0,
            lexeme_ready: false,
            sink: Guarded::new(sink, limits, abort, metrics),
            status: Status::Running,
            stop,
            prune,
            prune_hit: false,
            root_done: false,
        }
    }

    /// Install the subscriber and the parse guard on `parser`. The guard
    /// cancels the parse when the caller's flag or the adapter's own stop
    /// flag is set. The subscriber upgrades its `Weak` per pass: one
    /// refcount step, so the driver keeps the only strong reference.
    pub(crate) fn install(
        parser: &mut Tabnas,
        shared: Weak<Mutex<Adapter<S>>>,
        abort: AbortFlag,
        stop: AbortFlag,
    ) {
        parser.subscribe_rule_done(move |rule, _ctx, done| {
            if let Some(shared) = shared.upgrade() {
                lock(&shared).on_done(rule, done);
            }
        });
        parser.parse_guard(GUARD, move |_ctx| !abort.is_aborted() && !stop.is_aborted());
    }

    /// Ready for another document into the same sink: the line sources
    /// parse one value per line with one parser and one subscriber.
    pub(crate) fn reset(&mut self) {
        self.open = 0;
        self.last_completed = None;
        self.lexeme_ready = false;
        self.prune_hit = false;
        self.root_done = false;
    }

    pub(crate) fn status(&self) -> &Status {
        &self.status
    }

    /// Whether one whole root value was emitted and every frame closed.
    pub(crate) fn complete(&self) -> bool {
        self.root_done && self.open == 0
    }

    /// Send one event straight to the sink, outside the parse (the line
    /// sources' array brackets and `End`).
    pub(crate) fn send(&mut self, ev: JsonEvent<'_>) -> Result<Flow, Fail> {
        self.sink.event(ev)
    }

    pub(crate) fn finish(self) -> (Status, S) {
        (self.status, self.sink.into_inner())
    }

    fn fail(&mut self, fail: Fail) {
        self.status = Status::Failed(fail);
        self.stop.abort();
    }

    /// Emit one event. `false` means stop: the sink stopped or failed, and
    /// the parse has been told to cancel.
    fn emit(&mut self, ev: JsonEvent<'_>) -> bool {
        if let PruneState::Under(matcher) = &mut self.prune {
            match matcher.event(ev) {
                Ok(hit) => self.prune_hit = hit.kind == HitKind::Start && hit.begins > 0,
                Err(fail) => {
                    self.fail(fail);
                    return false;
                }
            }
        }
        match self.sink.event(ev) {
            Ok(Flow::Continue) => true,
            Ok(Flow::Stop) => {
                self.status = Status::Stopped;
                self.stop.abort();
                false
            }
            Err(fail) => {
                self.fail(fail);
                false
            }
        }
    }

    /// Emit a scalar value, with the remembered lexeme when it is this
    /// number's.
    fn scalar(&mut self, value: &Value) -> bool {
        match value {
            Value::Undefined | Value::Null => self.emit(JsonEvent::Null),
            Value::Bool(b) => self.emit(JsonEvent::Bool(*b)),
            Value::Number(n) => {
                if self.lexeme_ready && self.lexeme_value == *n {
                    self.lexeme_ready = false;
                    let lexeme = std::mem::take(&mut self.lexeme);
                    let ok = self.emit(JsonEvent::Number(Number::with_lexeme(*n, &lexeme)));
                    self.lexeme = lexeme;
                    ok
                } else {
                    // A remembered lexeme that is not this number's may be
                    // a later entry's of the same batch (a promoted first
                    // value comes before the number just parsed); the
                    // batch's end drops it.
                    self.emit(JsonEvent::Number(Number::new(*n)))
                }
            }
            Value::String(s) => self.emit(JsonEvent::String(s)),
            Value::Text(t) => self.emit(JsonEvent::String(&t.string)),
            // Not a scalar; callers check first.
            _ => true,
        }
    }

    /// Emit a whole value the adapter did not see being built (late).
    fn walk(&mut self, value: &Value) -> bool {
        self.lexeme_ready = false;
        match value {
            Value::Array(items) => self.walk_array(items),
            Value::ListRef(list) => self.walk_array(&list.value),
            Value::Object(members) => self.walk_object(members.iter()),
            Value::MapRef(map) => self.walk_object(map.value.iter()),
            scalar => self.scalar(scalar),
        }
    }

    fn walk_array(&mut self, items: &[Value]) -> bool {
        if !self.emit(JsonEvent::ArrayStart) {
            return false;
        }
        for item in items {
            if !self.walk(item) {
                return false;
            }
        }
        self.emit(JsonEvent::ArrayEnd)
    }

    fn walk_object<'v>(&mut self, members: impl Iterator<Item = (&'v String, &'v Value)>) -> bool {
        if !self.emit(JsonEvent::ObjectStart) {
            return false;
        }
        for (k, v) in members {
            if !self.emit(JsonEvent::Key(k)) || !self.walk(v) {
                return false;
            }
        }
        self.emit(JsonEvent::ObjectEnd)
    }

    /// Open a frame. Its `len` starts at zero whatever the container holds:
    /// entries present when the adapter first sees a cell were never
    /// streamed (jsonic's promoted first value), and the next close pass
    /// emits them before the new ones. The one already-streamed entry a
    /// container can hold is the last completed frame, which that pass
    /// recognizes by pointer and skips.
    fn push_frame(&mut self, cell: usize, array: bool, rule: &Rule, prune: bool) {
        let len = 0;
        if self.open == self.frames.len() {
            self.frames.push(Frame {
                cell,
                array,
                len,
                rule_i: rule.i,
                depth: rule.d,
                key: String::new(),
                has_key: false,
                prune,
            });
        } else {
            let f = &mut self.frames[self.open];
            f.cell = cell;
            f.array = array;
            f.len = len;
            f.rule_i = rule.i;
            f.depth = rule.d;
            f.key.clear();
            f.has_key = false;
            f.prune = prune;
        }
        self.open += 1;
    }

    /// The subscriber's body.
    pub(crate) fn on_done(&mut self, rule: &Rule, done: &RuleDone) {
        if !matches!(self.status, Status::Running) {
            return;
        }
        if done.alt.as_ref().is_some_and(|a| a.err.is_some()) {
            return;
        }
        let cell = Rc::as_ptr(&rule.node) as usize;
        match done.state {
            RuleState::Open => self.opened(rule, cell),
            RuleState::Close => {
                let replaces = done.alt.as_ref().is_some_and(|a| !a.r.is_empty());
                self.closed(rule, cell, replaces)
            }
        }
    }

    fn opened(&mut self, rule: &Rule, cell: usize) {
        let node = rule.node.borrow();
        let Some((array, len)) = container_len(&node) else {
            return;
        };
        if !self.frames[..self.open].iter().any(|f| f.cell == cell) {
            if self.open == 0 && self.root_done {
                self.fail(not_streamable());
                return;
            }
            self.last_completed = None;
            self.lexeme_ready = false;
            if !self.emit(if array {
                JsonEvent::ArrayStart
            } else {
                JsonEvent::ObjectStart
            }) {
                return;
            }
            let prune = match &self.prune {
                PruneState::Never => false,
                PruneState::All => array,
                PruneState::Under(_) => array && self.prune_hit,
            };
            self.push_frame(cell, array, rule, prune);
        } else if !array {
            let top = &self.frames[self.open - 1];
            if top.cell == cell && !top.has_key && top.len == len {
                if let Some(Value::String(k)) = rule.u.get("key") {
                    if !self.emit(JsonEvent::Key(k)) {
                        return;
                    }
                    let top = &mut self.frames[self.open - 1];
                    top.key.clear();
                    top.key.push_str(k);
                    top.has_key = true;
                }
            }
        }
    }

    fn closed(&mut self, rule: &Rule, cell: usize, replaces: bool) {
        // A root scalar's lexeme has to be known before it is emitted below.
        self.remember_lexeme(rule);

        let mut prune_from = None;
        {
            let node = rule.node.borrow();
            if let Some(top_i) = self.open.checked_sub(1) {
                if self.frames[top_i].cell == cell {
                    if let Some((array, len)) = container_len(&node) {
                        let old = self.frames[top_i].len;
                        if len > old {
                            for i in old..len {
                                let Some((key, value)) = entry_at(&node, i) else {
                                    break;
                                };
                                if !array {
                                    let key = key.unwrap_or("");
                                    let top = &mut self.frames[top_i];
                                    let announced = top.has_key && top.key == key;
                                    top.has_key = false;
                                    if !announced && !self.emit(JsonEvent::Key(key)) {
                                        return;
                                    }
                                }
                                let pointer = container_ptr(value);
                                if pointer.is_some() && pointer == self.last_completed {
                                    self.last_completed = None;
                                } else if pointer.is_some() {
                                    if !self.walk(value) {
                                        return;
                                    }
                                } else if !self.scalar(value) {
                                    return;
                                }
                            }
                            self.frames[top_i].len = len;
                            self.lexeme_ready = false;
                            if array && self.frames[top_i].prune {
                                prune_from = Some(old);
                            }
                        }
                    }
                }
            }
        }
        if let Some(from) = prune_from {
            let mut node = rule.node.borrow_mut();
            match &mut *node {
                Value::Array(items) => Arc::make_mut(items).truncate(from),
                Value::ListRef(list) => Arc::make_mut(list).value.truncate(from),
                _ => {}
            }
            self.frames[self.open - 1].len = from;
        }

        if replaces {
            return;
        }

        if let Some(top_i) = self.open.checked_sub(1) {
            let top = &self.frames[top_i];
            if top.rule_i == rule.i || (top.cell == cell && top.depth == rule.d) {
                let array = top.array;
                self.open -= 1;
                self.lexeme_ready = false;
                if !self.emit(if array {
                    JsonEvent::ArrayEnd
                } else {
                    JsonEvent::ObjectEnd
                }) {
                    return;
                }
                self.last_completed = container_ptr(&rule.node.borrow());
                if self.open == 0 {
                    self.root_done = true;
                }
            }
        }

        if rule.d == 0 && self.open == 0 && !self.root_done {
            let node = rule.node.borrow();
            if container_len(&node).is_none() {
                if !self.scalar(&node) {
                    return;
                }
                self.root_done = true;
            }
        }
    }

    /// Keep the source text of a number rule's first token when it is a
    /// JSON number spelling the node's value.
    fn remember_lexeme(&mut self, rule: &Rule) {
        let node = rule.node.borrow();
        let Value::Number(n) = &*node else {
            return;
        };
        let Some(token) = rule.o0() else {
            return;
        };
        let src = token.src.as_str();
        if is_json_number(src) && src.parse::<f64>().ok() == Some(*n) {
            self.lexeme.clear();
            self.lexeme.push_str(src);
            self.lexeme_value = *n;
            self.lexeme_ready = true;
        } else {
            self.lexeme_ready = false;
        }
    }
}

/// Lock the shared adapter, recovering from a poisoned lock: the engine
/// catches a panicking subscriber and fails the parse, and the state is
/// still needed to report that.
pub(crate) fn lock<S: Sink>(shared: &Mutex<Adapter<S>>) -> MutexGuard<'_, Adapter<S>> {
    shared
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Take the adapter back once the parse has returned.
///
/// This cannot fail: the subscriber holds a `Weak` and upgrades it only
/// while it runs, which is only inside `Tabnas::parse` on this thread, and
/// that has returned; the driver's `Arc` is therefore the one strong
/// reference. The `unreachable!` is the type system's due, not a path.
pub(crate) fn take<S: Sink + Send + 'static>(shared: Arc<Mutex<Adapter<S>>>) -> Adapter<S> {
    match Arc::try_unwrap(shared) {
        Ok(mutex) => mutex
            .into_inner()
            .unwrap_or_else(|poisoned| poisoned.into_inner()),
        Err(_) => unreachable!("the parse returned, so no subscriber holds the adapter"),
    }
}

/// The failure for an incremental run whose events did not amount to one
/// whole document: the grammar builds its result in a way the adapter
/// cannot follow, and only the whole-value path is sound for it.
pub(crate) fn not_streamable() -> Fail {
    Fail::new(
        Code::StreamabilityUnknown,
        "the grammar did not build its value through rule events the incremental source can follow; \
         it is not in capability::incremental, so run it with SourceMode::Materialize",
    )
}

fn container_len(v: &Value) -> Option<(bool, usize)> {
    match v {
        Value::Object(m) => Some((false, m.len())),
        Value::Array(a) => Some((true, a.len())),
        Value::MapRef(m) => Some((false, m.value.len())),
        Value::ListRef(l) => Some((true, l.value.len())),
        _ => None,
    }
}

fn container_ptr(v: &Value) -> Option<usize> {
    match v {
        Value::Object(m) => Some(Arc::as_ptr(m) as *const () as usize),
        Value::Array(a) => Some(Arc::as_ptr(a) as *const () as usize),
        Value::MapRef(m) => Some(Arc::as_ptr(m) as *const () as usize),
        Value::ListRef(l) => Some(Arc::as_ptr(l) as *const () as usize),
        _ => None,
    }
}

fn entry_at(v: &Value, i: usize) -> Option<(Option<&str>, &Value)> {
    match v {
        Value::Object(m) => m.get_index(i).map(|(k, v)| (Some(k.as_str()), v)),
        Value::MapRef(m) => m.value.get_index(i).map(|(k, v)| (Some(k.as_str()), v)),
        Value::Array(a) => a.get(i).map(|v| (None, v)),
        Value::ListRef(l) => l.value.get(i).map(|v| (None, v)),
        _ => None,
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::OwnedJsonEvent;

    /// Drive a parse through the adapter by hand and return the engine's
    /// value, to see what pruning did to it.
    fn parse_pruned(src: &str, prune: Prune) -> (String, Vec<OwnedJsonEvent>) {
        let mut parser = tabnas_json::make();
        let stop = AbortFlag::new();
        let adapter = Adapter::new(
            Vec::<OwnedJsonEvent>::new(),
            &Limits::default(),
            AbortFlag::new(),
            Metrics::new(),
            &prune,
            stop.clone(),
        );
        let shared = Arc::new(Mutex::new(adapter));
        Adapter::install(&mut parser, Arc::downgrade(&shared), AbortFlag::new(), stop);
        let value = parser.parse(src).unwrap();
        drop(parser);
        let (_, events) = take(shared).finish();
        (crate::Datum::from_tabnas(&value).to_string(), events)
    }

    #[test]
    fn pruning_empties_the_streamed_arrays_in_the_engines_value_only() {
        let src = r#"{"rows":[{"a":1},{"a":2}],"keep":[1,2,3],"n":{"rows":[[1],[2]]}}"#;
        let (whole, events) = parse_pruned(src, Prune::Never);
        assert_eq!(whole, src);

        let (pruned, same_events) = parse_pruned(
            src,
            Prune::Under(Selector::root().property("rows").each_index()),
        );
        assert_eq!(same_events, events, "pruning never changes the events");
        assert_eq!(
            pruned,
            r#"{"rows":[],"keep":[1,2,3],"n":{"rows":[[1],[2]]}}"#
        );

        let (pruned, same_events) = parse_pruned(src, Prune::AllArrays);
        assert_eq!(same_events, events);
        assert_eq!(pruned, r#"{"rows":[],"keep":[],"n":{"rows":[]}}"#);

        let (pruned, _) = parse_pruned(
            src,
            Prune::Under(Selector::root().property("n").property("rows")),
        );
        assert_eq!(
            pruned,
            r#"{"rows":[{"a":1},{"a":2}],"keep":[1,2,3],"n":{"rows":[]}}"#
        );
    }

    #[test]
    fn json_number_lexemes_are_recognized() {
        for ok in ["0", "-0", "12", "1.5", "50.25", "1e21", "1E+2", "-3.25e-7"] {
            assert!(is_json_number(ok), "{ok}");
        }
        for bad in [
            "", "-", "01", "1.", ".5", "+1", "0x10", "1_000", "1e", "NaN", "Infinity", "1 ",
        ] {
            assert!(!is_json_number(bad), "{bad}");
        }
    }
}
