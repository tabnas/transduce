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
//! - A repeated member name does not grow the map: the engine's insert
//!   replaces the earlier value in place, or the grammar merges the two
//!   (jsonic's `map.extend`, which yaml, json5, jsonc and zon inherit).
//!   The announcing rule's CLOSE pass is where the member landed (the
//!   engine notifies after the close actions), so when that pass leaves
//!   the map at its old length the adapter looks the member up and HOLDS
//!   what it finds until its next event. The engine reports a close pass
//!   whether or not the pass stood: a lifecycle action may have failed the
//!   parse (zon's own guard refuses a repeated field before jsonic's
//!   assignment runs), and the map then still holds the earlier value,
//!   which is not the member. Nothing in the event tells the two apart,
//!   but a failed pass is the last the engine reports, so a held member is
//!   streamed only once another rule event, or the end of its frame in the
//!   same pass, shows that the pass stood; a parse that fails at that pass
//!   leaves `Key a` as the last event of a protocol-valid prefix, and the
//!   source reports the grammar's error. A held scalar is streamed, so
//!   the stream reads `Key a, 1, Key a, 2` and a router's `Duplicates`
//!   policy decides, as it would for any repeated member; a container the
//!   adapter has just streamed is complete already; any other container is
//!   a merge of the earlier value with the new one, whose first half has
//!   already left, and the run fails with `DUPLICATE_MEMBER`. The
//!   whole-value walk sees only the survivor, so a repeated name is the
//!   one documented place the two streams differ.
//! - A grammar may rewrite a map's members after the adapter streamed
//!   them: YAML resolves a `<<` merge key when the mapping closes, removing
//!   the member and appending the merged ones. The adapter keeps an
//!   order-independent hash of the distinct names it streamed into each
//!   map and compares it with the map's names when the frame ends; a
//!   difference fails the run with `STREAMABILITY_UNKNOWN` rather than
//!   letting a stream the walk contradicts complete.
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
//!   was streamed before the array's start and cannot be taken back. That
//!   is the shape of a YAML stream of several documents and of a jsonic
//!   top-level implicit list whose first element is a container; both
//!   fail after the first value's events, never with a wrong stream. A
//!   grammar may also rewrite the root without opening a frame: YAML's
//!   stream rule wraps every document in a list when the source ends, and
//!   a stream whose later documents are scalars or empty shows no frame
//!   after the first. So when the parse root (a depth-0 rule) closes for
//!   good after a root value was streamed, its cell must still hold that
//!   value: the same container, or a scalar where a scalar was streamed.
//!   Anything else is refused with `STREAMABILITY_UNKNOWN`, again before
//!   `End`. `End` is not the adapter's to emit: the source sends it after the
//!   engine has returned `Ok`, so a document is complete only when it
//!   validated. A parse that returned `Ok` without the adapter emitting
//!   anything (YAML's empty document is `null`) is walked by the source:
//!   with nothing streamed, the walk is the whole stream.
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

use std::hash::{BuildHasher, RandomState};
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
    /// The last member name streamed; the one announced early for the
    /// next member, when `has_key`.
    key: String,
    has_key: bool,
    prune: bool,
    /// The wrapping sum of the hashes of the distinct names streamed into
    /// this map, to compare with the map's names when the frame ends.
    names: u64,
}

/// A member announced early whose rule closed without growing the map,
/// held until the adapter's next event shows that the close stood.
#[derive(Debug)]
struct Held {
    key: String,
    /// What the map held under the name at that close; `None` when the
    /// grammar announced a member it never stored.
    value: Option<Value>,
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
    /// The container that completed last: the next entry inserted into its
    /// parent is this one, already streamed. Held as the value, not as a
    /// pointer, so the allocation stays alive and a merged container built
    /// after it cannot be mistaken for it by landing at the same address.
    last_completed: Option<Value>,
    /// The member settled at the last pass, not yet streamed.
    held: Option<Held>,
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
    /// Anything at all has been emitted for the current document.
    emitted: bool,
    /// Hashes member names for the frames' `names` sums.
    hasher: RandomState,
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
            held: None,
            lexeme: String::new(),
            lexeme_value: 0.0,
            lexeme_ready: false,
            sink: Guarded::new(sink, limits, abort, metrics),
            status: Status::Running,
            stop,
            prune,
            prune_hit: false,
            root_done: false,
            emitted: false,
            hasher: RandomState::new(),
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
        self.held = None;
        self.lexeme_ready = false;
        self.prune_hit = false;
        self.root_done = false;
        self.emitted = false;
    }

    pub(crate) fn status(&self) -> &Status {
        &self.status
    }

    /// Whether one whole root value was emitted and every frame closed.
    pub(crate) fn complete(&self) -> bool {
        self.root_done && self.open == 0
    }

    /// Whether nothing at all was emitted for the current document, so
    /// the value the engine returned can be walked in its place.
    pub(crate) fn idle(&self) -> bool {
        !self.emitted
    }

    /// Emit a whole value through the sink, outside the parse: what the
    /// source does with an engine value no rule event showed.
    pub(crate) fn walk_whole(&mut self, value: &Value) -> Result<Flow, Fail> {
        self.emitted = true;
        crate::source::walk_value(value, &mut self.sink)
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
        self.emitted = true;
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
                names: 0,
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
            f.names = 0;
        }
        self.open += 1;
    }

    /// The subscriber's body.
    pub(crate) fn on_done(&mut self, rule: &Rule, done: &RuleDone) {
        if !matches!(self.status, Status::Running) {
            return;
        }
        // Any further event means the pass that held a member stood.
        if !self.flush_held() {
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
                self.fail(wrapped_root());
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
        // Whether a whole root value had left before this pass: the pass
        // that completes the root is exempt from the check at the end.
        let was_done = self.root_done;

        let mut prune_from = None;
        {
            let node = rule.node.borrow();
            if let Some(top_i) = self.open.checked_sub(1) {
                if self.frames[top_i].cell == cell {
                    if let Some((array, len)) = container_len(&node) {
                        let old = self.frames[top_i].len;
                        if !array && self.frames[top_i].has_key && len == old {
                            // The announced member did not grow the map, and
                            // the rule that announced it is closing: it
                            // replaced or merged an earlier member of the
                            // same name, or the pass failed the parse before
                            // storing it. Hold what the map has under the
                            // name; the next event decides. (A member that
                            // did grow the map is streamed by the loop below,
                            // which settles the announced one first when
                            // another landed ahead of it.)
                            let pending = self.frames[top_i].key.as_str();
                            let announcer = rule.u.get("key").is_some_and(
                                |k| matches!(k, Value::String(s) if s.as_str() == pending),
                            );
                            if announcer {
                                self.hold_pending(&node, top_i);
                            }
                        }
                        if len > old {
                            for i in old..len {
                                let Some((key, value)) = entry_at(&node, i) else {
                                    break;
                                };
                                if !array {
                                    let key = key.unwrap_or("");
                                    let top = &self.frames[top_i];
                                    if top.has_key && top.key != key {
                                        // A member landed ahead of the one
                                        // announced; that one keeps its
                                        // place in the stream.
                                        if !self.settle_pending(&node, top_i) {
                                            return;
                                        }
                                    }
                                    let name = self.hasher.hash_one(key);
                                    let top = &mut self.frames[top_i];
                                    let announced = top.has_key;
                                    top.has_key = false;
                                    top.names = top.names.wrapping_add(name);
                                    if !announced {
                                        top.key.clear();
                                        top.key.push_str(key);
                                        if !self.emit(JsonEvent::Key(key)) {
                                            return;
                                        }
                                    }
                                }
                                if !self.entry(value) {
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
                // A member held by this very pass goes before the frame ends.
                if !self.flush_held() {
                    return;
                }
                let top = &self.frames[top_i];
                if top.has_key {
                    // The announcing rule never closed on this cell: the
                    // member is whatever the map holds under the name now.
                    let node = rule.node.borrow();
                    if !self.settle_pending(&node, top_i) {
                        return;
                    }
                }
                if !array {
                    let node = rule.node.borrow();
                    let names = member_names(&node)
                        .map(|k| self.hasher.hash_one(k))
                        .fold(0u64, u64::wrapping_add);
                    if names != self.frames[top_i].names {
                        self.fail(rewritten_map());
                        return;
                    }
                }
                self.open -= 1;
                self.lexeme_ready = false;
                if !self.emit(if array {
                    JsonEvent::ArrayEnd
                } else {
                    JsonEvent::ObjectEnd
                }) {
                    return;
                }
                let node = rule.node.borrow();
                self.last_completed = is_container(&node).then(|| node.clone());
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

        // The parse root closing for good after the root value left: the
        // cell must still hold what was streamed. A grammar's close actions
        // may replace it (YAML's stream rule wraps its documents in a list
        // when the source ends, and later documents that are scalars or
        // empty opened no frame to be refused at), and the events that
        // left cannot be taken back.
        if was_done && rule.d == 0 && self.open == 0 {
            let node = rule.node.borrow();
            let streamed = match (&self.last_completed, is_container(&node)) {
                (Some(last), true) => same_container(&node, last),
                // A root scalar was streamed, and the cell holds a scalar.
                (None, false) => true,
                _ => false,
            };
            if !streamed {
                self.fail(rewritten_root());
            }
        }
    }

    /// Emit one entry that just landed in the top frame's container: a
    /// scalar as itself, the container that completed last as nothing (it
    /// has been streamed), any other container whole, late.
    fn entry(&mut self, value: &Value) -> bool {
        if !is_container(value) {
            return self.scalar(value);
        }
        if self
            .last_completed
            .as_ref()
            .is_some_and(|last| same_container(value, last))
        {
            self.last_completed = None;
            return true;
        }
        self.walk(value)
    }

    /// The member announced on the frame `top_i` did not grow its map, so
    /// it replaced or merged an earlier member of the same name: stream
    /// what the map holds under that name now. A scalar is emitted; the
    /// container that completed last was streamed as it was built; any
    /// other container is the grammar's merge of both values, which cannot
    /// be streamed because the first half already was, so the run fails
    /// with `DUPLICATE_MEMBER`. A name the map does not hold at all means
    /// the grammar announced a member it never stored, which the adapter
    /// cannot follow.
    fn settle_pending(&mut self, node: &Value, top_i: usize) -> bool {
        let top = &mut self.frames[top_i];
        top.has_key = false;
        let key = std::mem::take(&mut top.key);
        let ok = self.settle(&key, member(node, &key));
        self.frames[top_i].key = key;
        ok
    }

    /// Like [`Adapter::settle_pending`], but the member is held rather than
    /// streamed: the announcing rule's close pass may be one the engine
    /// reports although a lifecycle action failed the parse in it, and the
    /// map then still holds the earlier value. [`Adapter::flush_held`]
    /// streams it at the next event, which a failed pass never sends.
    fn hold_pending(&mut self, node: &Value, top_i: usize) {
        let top = &mut self.frames[top_i];
        top.has_key = false;
        let key = top.key.clone();
        let value = member(node, &key).cloned();
        self.held = Some(Held { key, value });
    }

    /// Stream the member held at the last pass, if any. `false` means stop.
    fn flush_held(&mut self) -> bool {
        match self.held.take() {
            Some(held) => self.settle(&held.key, held.value.as_ref()),
            None => true,
        }
    }

    /// Stream what a map holds under a repeated member's name: a scalar as
    /// itself; the container that completed last as nothing; any other
    /// container fails the run as a merge; no value at all fails it as a
    /// member the grammar announced and never stored.
    fn settle(&mut self, key: &str, value: Option<&Value>) -> bool {
        match value {
            Some(value) => {
                if !is_container(value) {
                    self.scalar(value)
                } else if self
                    .last_completed
                    .as_ref()
                    .is_some_and(|last| same_container(value, last))
                {
                    self.last_completed = None;
                    true
                } else {
                    self.fail(merged_member(key));
                    false
                }
            }
            None => {
                self.fail(Fail::new(
                    Code::StreamabilityUnknown,
                    format!(
                        "the grammar announced member {key:?} and never stored it, which the \
                         incremental source cannot follow; run it with SourceMode::Materialize"
                    ),
                ));
                false
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
        "the grammar did not build its value through rule events the incremental source can \
         follow (the events did not amount to one whole document); run it with \
         SourceMode::Materialize",
    )
}

/// The failure for a container that starts at the root after a root value
/// has completed: the grammar is wrapping a value already streamed as the
/// document (a YAML stream of several documents, a jsonic top-level
/// implicit list whose first element is a container), and the events that
/// left cannot be taken back.
pub(crate) fn wrapped_root() -> Fail {
    Fail::new(
        Code::StreamabilityUnknown,
        "the grammar wrapped a value already streamed as the document's root in a list (a YAML \
         stream of several documents, a jsonic top-level implicit list); the incremental source \
         cannot take the root back, so run it with SourceMode::Materialize",
    )
}

/// The failure for a parse root whose cell no longer holds the value
/// streamed as the document when the root rule closes for good: the
/// grammar replaced it from the closing rule's actions (YAML's stream rule
/// wraps every document in a list when the source ends, whatever their
/// shapes), and the events that left cannot be taken back.
pub(crate) fn rewritten_root() -> Fail {
    Fail::new(
        Code::StreamabilityUnknown,
        "the grammar replaced the document's root after the incremental source streamed it (a \
         YAML stream of several documents is wrapped in a list when the source ends); the \
         incremental source cannot take the root back, so run it with SourceMode::Materialize",
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

fn is_container(v: &Value) -> bool {
    matches!(
        v,
        Value::Object(_) | Value::Array(_) | Value::MapRef(_) | Value::ListRef(_)
    )
}

/// Whether two values are the same shared container.
fn same_container(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Object(x), Value::Object(y)) => Arc::ptr_eq(x, y),
        (Value::Array(x), Value::Array(y)) => Arc::ptr_eq(x, y),
        (Value::MapRef(x), Value::MapRef(y)) => Arc::ptr_eq(x, y),
        (Value::ListRef(x), Value::ListRef(y)) => Arc::ptr_eq(x, y),
        _ => false,
    }
}

/// The member names of a map, in its order.
fn member_names(v: &Value) -> impl Iterator<Item = &str> {
    let names: Box<dyn Iterator<Item = &str>> = match v {
        Value::Object(m) => Box::new(m.keys().map(String::as_str)),
        Value::MapRef(m) => Box::new(m.value.keys().map(String::as_str)),
        _ => Box::new(std::iter::empty()),
    };
    names
}

/// The failure for a map whose members the grammar rewrote after the
/// adapter streamed them (YAML's `<<` merge keys are resolved when the
/// mapping closes): the stream would contradict the walk.
pub(crate) fn rewritten_map() -> Fail {
    Fail::new(
        Code::StreamabilityUnknown,
        "the grammar rewrote the members of a map after the incremental source streamed them \
         (a YAML merge key does), so the stream would not be the document's; run it with \
         SourceMode::Materialize",
    )
}

/// The member of a map under `key`.
fn member<'v>(v: &'v Value, key: &str) -> Option<&'v Value> {
    match v {
        Value::Object(m) => m.get(key),
        Value::MapRef(m) => m.value.get(key),
        _ => None,
    }
}

/// The failure for a repeated member whose values the grammar merged: the
/// first value has already been streamed, so the merged one cannot be.
pub(crate) fn merged_member(key: &str) -> Fail {
    Fail::new(
        Code::DuplicateMember,
        format!(
            "member {key:?} appears twice and the grammar merged the two values; the first was \
             already streamed, so the incremental source cannot emit the merged member: run it \
             with SourceMode::Materialize"
        ),
    )
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
