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
//! - the rule's `name`, its depth `d` and instance `i` (the engine's,
//!   which numbers rules from 1, the start rule's, in the order it makes
//!   them), the alternate that matched (`alt`: `p`, `r`, `b`, and its groups `g`,
//!   sorted, since they are tags whose order the runtimes' engines keep
//!   differently), whether recovery forced the close, and its named
//!   counters `n`, sorted by name;
//! - the `tokens` the pass consumed: those its alternate matched, less the
//!   `b` it gave back from their end, which are the next rule's to
//!   consume, and none when `b` is more than it recorded, since a close
//!   alternate can back up a token it did not record. A token is its
//!   name, its value, its source text, its 1-based row and column and its
//!   byte offset (where it starts, in UTF-8 bytes from the document's
//!   start: the engine's `site.si`); a token with no value (the end of the
//!   source, or a fixed token such as `[`) has `null`, and a number
//!   carries its source text as its lexeme when that text is a JSON number
//!   spelling the value (RFC 8259), so a renderer can write it as it
//!   stands;
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
//! A sink that asks for them ([`ParserSink::wants_tokens`]), or a source
//! told to send them ([`ParserEventSource::tokens`]), also gets a
//! [`ParserEvent::Token`] for each token the lexer gave and the parser did
//! not take: one of the grammar's ignored tokens (its `IGNORE` token set:
//! white space, a line break, a comment). The source then subscribes to the
//! engine's lexer (`subscribe_lex`), takes each ignored token as the lexer
//! cuts it, and holds it until the first pass that consumed a token after
//! it, which it sends it before; those still held when the parse returns
//! go before the `End`. So a token item is in source order among the
//! passes, and the source holds no more than the ignored tokens between
//! two passes. A token lexed while a probe is open is the document's all
//! the same: a probe's rewind replays the tokens the parser consumed from
//! the engine's history, and the lexer never cuts that text again, so a
//! token item dropped there would be lost. A token the lexer cuts again (a
//! relex re-cuts the text after a token it renegotiates) starts before the
//! furthest one taken, and is dropped as the one already held. A sink that
//! does not ask gets the passes alone, and the source subscribes to no
//! lexer event for it.
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
//!
//! The engine builds its value as it parses, so a long document's tree
//! holds every record, though no pass's value is a container and nothing
//! reads the value the parse returns. [`ParserEventSource::prune`] empties
//! the tree behind the stream. Once the sink has seen a close pass:
//! - whose rule's node is an array, the source empties that array, so the
//!   tree holds the elements of the passes since the last close on it and
//!   no more;
//! - whose rule's node is an element of an array its parent rule's node
//!   holds (directly, or as a member's or an element's value), the source
//!   empties the elements before it, which every close before has already
//!   passed: an array no rule's node is, such as the one a grammar appends
//!   each table or block to, which the first case never reaches. Each
//!   element is set to `Undefined`, so the array keeps its length and the
//!   node keeps its place, for a grammar that finds it by position;
//! - whose rule's node is a map, for a grammar
//!   [`super::capability::prunes_maps`] lists, the source empties that map
//!   too, so a map's members (an INI document's sections, a JSON object's
//!   members) go as an array's elements do.
//!
//! A container the source cannot empty alone, one also held elsewhere, is
//! left as it is. That alters the value the engine returns, which this
//! source never reads, and it is sound for a grammar whose passes do not
//! depend on what its tree holds, which a grammar's actions or conditions
//! could make them: yaml, toml, xml, zon and feed read their maps back,
//! to find a key or a table defined twice or a name to merge or resolve,
//! which is why their maps are left. One difference is the contract: a
//! token whose value is a container of the tree (a yaml alias, which the
//! lexer resolves to the anchored node) carries what the tree holds when
//! its pass is made, so the arrays in it may have been emptied, and its
//! maps too where the grammar's are, while its kind is as it was; a part
//! reads such a token's kind, never its elements. So pruning is a
//! verified capability, never an assumption: the differential suite
//! (`tests/prune_test.rs`) runs every document of every grammar in the
//! dev-dependencies pruned and not, and with its maps emptied and not, and
//! [`super::capability::prunable`] and
//! [`super::capability::prunes_maps`] list the grammars whose passes and
//! outcomes never differed but by that. A run that prunes is gated on the
//! first list, by the grammar's name ([`ParserEventSource::grammar`]), and
//! without a listed name it fails with `STREAMABILITY_UNKNOWN` before the
//! parse, having sent nothing; [`ParserEventSource::unverified`] lifts the
//! gate for the suite, and [`ParserEventSource::unverified_maps`] empties
//! the maps of any grammar, for the suite too.

use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use tabnas::{Context, Rule, RuleDone, RuleState, Tabnas, Tin, Token, Value};

use crate::datum::Datum;
use crate::error::{Code, Fail};
use crate::limits::{AbortFlag, Limits, Metrics};
use crate::sink::Flow;
use crate::source::{capability, engine_failure};
use crate::source::{is_json_number, GUARD};
use crate::{ParserEvent, ParserSink, PassAlt, PassToken, RulePass};

/// A tabnas parser applied to one text, as a source of its rule passes.
pub struct ParserEventSource<'s> {
    parser: Tabnas,
    text: &'s str,
    /// A reader the parser reads its source from, in place of `text`
    /// ([`ParserEventSource::from_reader`]).
    reader: Option<Box<dyn std::io::Read + Send + 's>>,
    limits: Limits,
    abort: AbortFlag,
    metrics: Arc<Metrics>,
    grammar: Option<Box<str>>,
    prune: bool,
    unverified: bool,
    /// Empty maps too, whatever the grammar.
    unverified_maps: bool,
    /// Send token items whether or not the sink asks.
    tokens: bool,
}

impl<'s> ParserEventSource<'s> {
    /// A source with default limits, its own abort flag and fresh metrics,
    /// that prunes nothing; the builder methods change each.
    pub fn new(parser: Tabnas, text: &'s str) -> ParserEventSource<'s> {
        ParserEventSource {
            parser,
            text,
            reader: None,
            limits: Limits::default(),
            abort: AbortFlag::new(),
            metrics: Metrics::new(),
            grammar: None,
            prune: false,
            unverified: false,
            unverified_maps: false,
            tokens: false,
        }
    }

    /// A source of the rule passes of what `reader` gives, which the parser
    /// reads with `Tabnas::parse_reader` (admin ADR-30): a grammar that
    /// declares `lex.window` a window at a time, so that a long document
    /// streams holding a window of its text rather than all of it, and any
    /// other grammar whole first. The passes, the value and the failures are
    /// those of [`ParserEventSource::new`] over the same text, and a reader
    /// that fails, or bytes that are not UTF-8, end the run with
    /// `INPUT_INVALID`, the engine's `unreadable` and where the source
    /// failed.
    pub fn from_reader(
        parser: Tabnas,
        reader: impl std::io::Read + Send + 's,
    ) -> ParserEventSource<'s> {
        ParserEventSource {
            reader: Some(Box::new(reader)),
            ..ParserEventSource::new(parser, "")
        }
    }

    /// The grammar the parser implements, by the name of the format it
    /// reads: its crate's (`tabnas-<name>`), but `pgn` for tabnas-chess, as
    /// [`capability::PRUNABLE`] names them. A run that prunes needs a name
    /// [`capability::prunable`] lists; one that does not prune needs no
    /// name.
    pub fn grammar(mut self, name: &str) -> Self {
        self.grammar = Some(name.into());
        self
    }

    /// Empty the engine's tree behind the stream, so that it does not hold
    /// the whole document: each array once the sink has seen a close pass
    /// on it, an array's elements before one whose close pass the sink has
    /// seen, and, for a grammar [`capability::prunes_maps`] lists, each map
    /// once the sink has seen a close pass on it (the module's docs have
    /// the details). The passes are unchanged for a grammar
    /// [`capability::prunable`] lists, and [`ParserEventSource::run_owned`]
    /// refuses to prune any other, with `STREAMABILITY_UNKNOWN` before the
    /// parse.
    pub fn prune(mut self) -> Self {
        self.prune = true;
        self
    }

    /// Send a [`ParserEvent::Token`] for each token the parser did not
    /// take, whether or not the sink asks for them
    /// ([`ParserSink::wants_tokens`], which a run also follows).
    pub fn tokens(mut self) -> Self {
        self.tokens = true;
        self
    }

    /// Prune whatever the verified list says. This exists for the
    /// differential suite that maintains the list and for nothing else: a
    /// grammar that reads its arrays back may make other passes once they
    /// are emptied, and the run still returns `Ok`.
    pub fn unverified(mut self) -> Self {
        self.unverified = true;
        self
    }

    /// [`ParserEventSource::unverified`], and empty the grammar's maps as
    /// well as its arrays, whatever [`capability::prunes_maps`] says. This
    /// exists for the differential suite that maintains that list and for
    /// nothing else: a grammar that reads its maps back may make other
    /// passes once they are emptied.
    pub fn unverified_maps(mut self) -> Self {
        self.unverified = true;
        self.unverified_maps = true;
        self
    }

    /// Why the run may not prune, when it may not.
    fn gate(&self) -> Option<Fail> {
        if !self.prune || self.unverified {
            return None;
        }
        match self.grammar.as_deref() {
            Some(name) if capability::prunable(name) => None,
            Some(name) => Some(Fail::new(
                Code::StreamabilityUnknown,
                format!(
                    "grammar {name:?} is not in capability::prunable: the differential suite \
                     has not verified that its passes are the same when its arrays are emptied \
                     behind the stream; run it without ParserEventSource::prune"
                ),
            )),
            None => Some(Fail::new(
                Code::StreamabilityUnknown,
                "ParserEventSource::prune needs the grammar's name (ParserEventSource::grammar) \
                 to check capability::prunable; without one, run it without pruning",
            )),
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
        let (outcome, sink, _) = self.run_owned_with_value(sink);
        (outcome, sink)
    }

    /// [`ParserEventSource::run_owned`], also handing back the value the
    /// engine returned, when the parse returned one: the engine's tree
    /// after pruning, when the run prunes. It is not the run's result (the
    /// passes are): it exists so a test can measure what pruning left in
    /// the tree, and nothing else should read it.
    pub fn run_owned_with_value<S: ParserSink + Send + 'static>(
        self,
        sink: S,
    ) -> (Result<Flow, Fail>, S, Option<Value>) {
        if let Some(refused) = self.gate() {
            return (Err(refused), sink, None);
        }
        let ParserEventSource {
            mut parser,
            text,
            reader,
            limits,
            abort,
            metrics,
            grammar,
            prune,
            unverified_maps,
            tokens,
            ..
        } = self;
        let prune_maps =
            prune && (unverified_maps || grammar.as_deref().is_some_and(capability::prunes_maps));
        let tokens = (tokens || sink.wants_tokens()).then(|| TokenQueue {
            ignored: parser.token_set("IGNORE").unwrap_or_default(),
            held: VecDeque::new(),
            reach: 0,
        });
        let lex = tokens.is_some();
        let stop = AbortFlag::new();
        let relay = Arc::new(Mutex::new(Relay {
            sink,
            status: Status::Running,
            max_scalar_bytes: limits.max_scalar_bytes,
            abort: abort.clone(),
            stop: stop.clone(),
            events: 0,
            prune,
            prune_maps,
            tokens,
        }));
        install(
            &mut parser,
            Arc::downgrade(&relay),
            abort.clone(),
            stop.clone(),
            lex,
        );
        let parsed = match reader {
            Some(reader) => parser.parse_reader(reader),
            None => parser.parse(text),
        };
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
                Ok(_) => match relay.release(u64::MAX) {
                    Ok(Flow::Continue) => relay.send(&ParserEvent::End),
                    other => other,
                },
                Err(e) => Err(engine_failure(e, &abort)),
            },
        };
        Metrics::add(&metrics.events, relay.events);
        (outcome, relay.sink, parsed.ok())
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
    /// Empty the tree behind the stream, once the sink has seen a close
    /// pass.
    prune: bool,
    /// Empty a map a close pass's rule builds too.
    prune_maps: bool,
    /// The token items, when the run sends them.
    tokens: Option<TokenQueue>,
}

/// The ignored tokens the lexer has cut and no pass has yet been sent
/// after, in source order.
struct TokenQueue {
    /// The grammar's `IGNORE` token set: the tokens the parser does not take.
    ignored: Vec<Tin>,
    held: VecDeque<PassToken>,
    /// The byte offset past the furthest token taken: a token that starts
    /// before it is one the lexer cut again.
    reach: u64,
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

    /// Send the token items held that start before `before`, a byte
    /// offset, in order.
    fn release(&mut self, before: u64) -> Result<Flow, Fail> {
        loop {
            let Some(queue) = &mut self.tokens else {
                return Ok(Flow::Continue);
            };
            match queue.held.front() {
                Some(token) if token.offset < before => {}
                _ => return Ok(Flow::Continue),
            }
            let token = queue.held.pop_front().expect("a token is held");
            match self.send(&ParserEvent::Token(token))? {
                Flow::Continue => {}
                Flow::Stop => return Ok(Flow::Stop),
            }
        }
    }

    /// One token the lexer cut: held when it is an ignored token the run
    /// has not taken yet.
    fn on_lex(&mut self, token: &Token) {
        if !matches!(self.status, Status::Running) {
            return;
        }
        let Some(queue) = &mut self.tokens else {
            return;
        };
        if !queue.ignored.contains(&token.tin) {
            return;
        }
        let offset = token.site.si as u64;
        if offset < queue.reach {
            return;
        }
        queue.reach = offset + token.src.as_str().len() as u64;
        match pass_token(token, self.max_scalar_bytes) {
            Ok(item) => {
                if let Some(queue) = &mut self.tokens {
                    queue.held.push_back(item);
                }
            }
            Err(fail) => self.fail(fail),
        }
    }

    /// One pass the engine completed.
    fn on_done(&mut self, rule: &Rule, context: &Context, done: &RuleDone) {
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
        // The token items before the last token this pass consumed come
        // first.
        if let Some(last) = pass.tokens.last() {
            match self.release(last.offset) {
                Ok(Flow::Continue) => {}
                Ok(Flow::Stop) => {
                    self.status = Status::Stopped;
                    self.stop.abort();
                    return;
                }
                Err(fail) => return self.fail(fail),
            }
        }
        let ev = if open {
            ParserEvent::Open(pass)
        } else {
            ParserEvent::Close(pass)
        };
        match self.send(&ev) {
            Ok(Flow::Continue) => {
                if self.prune && !open && !pruning_held(context) {
                    empty_node(rule, self.prune_maps);
                    empty_before(rule);
                }
            }
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
    lex: bool,
) {
    if lex {
        let relay = relay.clone();
        parser.subscribe_lex(move |token, _, _| {
            if let Some(relay) = relay.upgrade() {
                lock(&relay).on_lex(token);
            }
        });
    }
    parser.subscribe_rule_done(move |rule, context, done| {
        if let Some(relay) = relay.upgrade() {
            lock(&relay).on_done(rule, context, done);
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

/// The key in the parse context's bag (`Context::u`) under which a grammar
/// holds pruning: while its count is above zero the source empties no
/// array. A grammar raises it while its actions may still read what an
/// array holds after the array's close pass, as tabnas-yaml does while an
/// anchored value is open, since it records the anchor only when the value
/// closes (tabnas/yaml#138). The same key in every runtime.
pub const PRUNE_HOLD: &str = "pruneHold";

/// Whether a grammar holds pruning at this pass ([`PRUNE_HOLD`]).
fn pruning_held(context: &Context) -> bool {
    matches!(context.u.get(PRUNE_HOLD), Some(Value::Number(n)) if *n > 0.0)
}

/// Empty the array `rule`'s node is, or the map, when `maps` and the
/// source can empty it alone. Every element or member in it was built by
/// passes the sink has seen, the last of them the close pass just sent,
/// and what those passes carry is held by the sink, so nothing sent so
/// far changes. A container also held elsewhere (in a rule's state, or by
/// a container that took it already) is left as it is: emptying the
/// node's copy would free nothing, and would copy the container.
fn empty_node(rule: &Rule, maps: bool) {
    let Ok(mut node) = rule.node.try_borrow_mut() else {
        return;
    };
    match &mut *node {
        Value::Array(items) => {
            if let Some(items) = Arc::get_mut(items) {
                items.clear();
            }
        }
        Value::ListRef(list) => {
            if let Some(list) = Arc::get_mut(list) {
                list.value.clear();
            }
        }
        Value::Object(members) if maps => {
            if let Some(members) = Arc::get_mut(members) {
                members.clear();
            }
        }
        Value::MapRef(map) if maps => {
            if let Some(map) = Arc::get_mut(map) {
                map.value.clear();
            }
        }
        _ => {}
    }
}

/// Where a container's contents live, for telling one apart from another;
/// `None` for a scalar.
fn container(value: &Value) -> Option<*const ()> {
    match value {
        Value::Array(items) => Some(Arc::as_ptr(items).cast()),
        Value::ListRef(list) => Some(Arc::as_ptr(list).cast()),
        Value::Object(members) => Some(Arc::as_ptr(members).cast()),
        Value::MapRef(map) => Some(Arc::as_ptr(map).cast()),
        _ => None,
    }
}

/// When `rule`'s node is a container that an array within its parent's
/// node holds, set that array's elements before it to `Undefined`: each
/// was closed before it, by passes the sink has seen. The array is looked
/// for in the parent's node, its members' values and its elements, the
/// places a grammar appends a finished node to (the array of tables a
/// toml document's root holds, a document's blocks). An element keeps its
/// place, for a grammar that finds a node by position. A container also
/// held elsewhere is left as it is, as [`empty_node`] leaves it.
fn empty_before(rule: &Rule) {
    let Some(node) = rule
        .node
        .try_borrow()
        .ok()
        .and_then(|node| container(&node))
    else {
        return;
    };
    let Some(parent) = &rule.parent_node else {
        return;
    };
    if Rc::ptr_eq(parent, &rule.node) {
        return;
    }
    if let Ok(mut parent) = parent.try_borrow_mut() {
        if !empty_before_in(&mut parent, node) {
            let held = match &mut *parent {
                Value::Array(items) => Arc::get_mut(items).map(|items| items.as_mut_slice()),
                Value::ListRef(list) => Arc::get_mut(list).map(|list| list.value.as_mut_slice()),
                _ => None,
            };
            if let Some(items) = held {
                for item in items {
                    if empty_before_in(item, node) {
                        return;
                    }
                }
            }
            let members = match &mut *parent {
                Value::Object(members) => Arc::get_mut(members).map(|m| m.values_mut()),
                _ => None,
            };
            if let Some(values) = members {
                for value in values {
                    if empty_before_in(value, node) {
                        return;
                    }
                }
            }
        }
    }
}

/// When `array` is an array holding `node`, set its elements before the
/// last place it holds `node` to `Undefined`, and answer that it was found.
fn empty_before_in(array: &mut Value, node: *const ()) -> bool {
    let items = match array {
        Value::Array(items) => Arc::get_mut(items).map(|items| items.as_mut_slice()),
        Value::ListRef(list) => Arc::get_mut(list).map(|list| list.value.as_mut_slice()),
        _ => None,
    };
    let Some(items) = items else {
        return false;
    };
    match items.iter().rposition(|item| container(item) == Some(node)) {
        Some(at) => {
            for item in &mut items[..at] {
                *item = Value::Undefined;
            }
            true
        }
        None => false,
    }
}

/// A token as a pass and a token item carry it, its sizes held to
/// `max_scalar_bytes`.
fn pass_token(t: &Token, max: usize) -> Result<PassToken, Fail> {
    let src = t.src.as_str();
    held(src.len(), "a token's source text", max)?;
    let val = scalar_of(&t.val, Some(src));
    if let Datum::String(s) = &val {
        held(s.len(), "a token's value", max)?;
    }
    Ok(PassToken {
        name: t.name.as_str().into(),
        val,
        src: src.into(),
        row: t.site.ri as u64,
        col: t.site.ci as u64,
        offset: t.site.si as u64,
    })
}

/// The pass as alchemy's [`RulePass`], its sizes held to
/// `max_scalar_bytes`.
fn pass(rule: &Rule, done: &RuleDone, open: bool, max: usize) -> Result<RulePass, Fail> {
    let matched = if open { &rule.o } else { &rule.c };
    let b = done.alt.as_ref().map_or(0, |a| a.b);
    let kept = &matched[..matched.len().saturating_sub(b)];
    let mut tokens = Vec::with_capacity(kept.len());
    for t in kept {
        tokens.push(pass_token(t, max)?);
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
        alt: done.alt.as_ref().map(|a| {
            let mut g: Vec<Box<str>> = a.g.iter().map(|g| g.as_str().into()).collect();
            g.sort();
            PassAlt {
                p: a.p.as_str().into(),
                r: a.r.as_str().into(),
                b: a.b as u64,
                g,
            }
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
                r##"["open",{"name":"val","d":0,"i":1,"alt":{"p":"","r":"","b":0,"g":["json","val"]},"tokens":[["#NR",2.5,"2.50",1,2,1]],"forced":false,"n":{}}]"##,
                r##"["close",{"name":"val","d":0,"i":1,"alt":{"p":"","r":"","b":0,"g":["end","json"]},"tokens":[["#ZZ",null,"",1,7,6]],"forced":false,"n":{},"value":2.5}]"##,
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

    /// A sink that asks for token items, recording what it got.
    #[derive(Default)]
    struct Lexed(Vec<ParserEvent>);

    impl ParserSink for Lexed {
        fn event(&mut self, ev: &ParserEvent) -> Result<Flow, Fail> {
            self.0.push(ev.clone());
            Ok(Flow::Continue)
        }

        fn wants_tokens(&self) -> bool {
            true
        }
    }

    /// Each event as a word: a pass by its rule, its phase and the byte
    /// offsets of its tokens, a token item by its name and offset.
    fn words(events: &[ParserEvent]) -> String {
        events
            .iter()
            .map(|ev| match ev {
                ParserEvent::Open(p) | ParserEvent::Close(p) => {
                    let mut word = format!("{}:{}", p.name, ev.name());
                    for t in &p.tokens {
                        word.push('@');
                        word.push_str(&t.offset.to_string());
                    }
                    word
                }
                ParserEvent::Token(t) => format!("{}@{}", t.name, t.offset),
                ParserEvent::End => "end".to_string(),
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    #[test]
    fn a_sink_that_asks_gets_the_ignored_tokens_in_source_order_with_byte_offsets() {
        // Each ignored token comes before the first pass that consumed a
        // token after it: the leading space before the list's open pass,
        // which consumed the bracket (the root's open consumed nothing, as
        // its alternate gave the bracket back), the space after the comma
        // before the second number's open, and the line break before the
        // close that consumed the end of the source. Offsets count UTF-8
        // bytes: the string holds a two-byte character.
        let (r, seen) = ParserEventSource::new(tabnas_json::make(), " [\"é\", 1]\n")
            .run_owned(Lexed::default());
        assert_eq!(r.unwrap(), Flow::Continue);
        assert_eq!(
            words(&seen.0),
            "val:open #SP@0 list:open@1 elem:open val:open@2 val:close elem:close@6 \
             elem:open #SP@7 val:open@8 val:close elem:close list:close@9 #LN@10 val:close@11 end"
        );
        // The same document to a sink that does not ask: the passes alone,
        // their offsets the same.
        let (r, plain) = passes(" [\"é\", 1]\n");
        assert_eq!(r.unwrap(), Flow::Continue);
        assert_eq!(
            words(&plain),
            words(&seen.0)
                .replace("#SP@0 ", "")
                .replace("#SP@7 ", "")
                .replace("#LN@10 ", "")
        );
        // A source told to send them sends them to any sink.
        let (r, told) = ParserEventSource::new(tabnas_json::make(), " [\"é\", 1]\n")
            .tokens()
            .run_owned(Seen::default());
        assert_eq!(r.unwrap(), Flow::Continue);
        assert_eq!(words(&told.0), words(&seen.0));
    }

    /// DIVERGENCE.md 6: the engine's own space, line and comment tokens
    /// carry their text as their value in Rust, and none in TypeScript and
    /// Go. A part reads a token item's text from its `src`.
    #[test]
    fn a_space_token_item_carries_its_text_as_its_value() {
        let (r, seen) =
            ParserEventSource::new(tabnas_json::make(), "  1").run_owned(Lexed::default());
        assert_eq!(r.unwrap(), Flow::Continue);
        let Some(ParserEvent::Token(space)) = seen.0.iter().find(|e| e.name() == "token") else {
            panic!("a token item");
        };
        assert_eq!(
            (space.name.as_ref(), &space.val, space.src.as_ref()),
            ("#SP", &Datum::String("  ".into()), "  ")
        );
    }

    #[test]
    fn a_token_item_past_max_scalar_bytes_is_refused_by_name() {
        let limits = Limits {
            max_scalar_bytes: 3,
            ..Limits::default()
        };
        let (r, seen) = ParserEventSource::new(tabnas_json::make(), "[1,    2]")
            .limits(limits)
            .run_owned(Lexed::default());
        let err = r.unwrap_err();
        assert_eq!(err.code, Code::ResourceLimitExceeded);
        assert_eq!(err.limit.unwrap().name, "max_scalar_bytes");
        assert!(!seen.0.iter().any(|e| matches!(e, ParserEvent::Token(_))));
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
    fn pruning_empties_the_trees_arrays_and_leaves_the_passes_as_they_were() {
        let src = r#"{"a":[1,2,3],"b":[[4],[5,{"c":[6]}]],"d":{"e":"f"}}"#;
        // Not pruned; pruned without a name, its arrays alone; and pruned as
        // a host prunes json, which `capability::prunes_maps` lists.
        let run = |how: u8| {
            let source = ParserEventSource::new(tabnas_json::make(), src);
            let source = match how {
                0 => source.grammar("json"),
                1 => source.prune().unverified(),
                _ => source.grammar("json").prune(),
            };
            let (r, seen, value) = source.run_owned_with_value(Seen::default());
            assert_eq!(r.unwrap(), Flow::Continue);
            (
                encoded(&seen.0),
                serde_json::to_string(&value.unwrap()).unwrap(),
            )
        };
        let canon = |text: &str| serde_json::to_string(&tabnas_json::make().parse(text).unwrap());
        let (whole, kept) = run(0);
        let (pruned, left) = run(1);
        assert_eq!(pruned, whole);
        assert_eq!(kept, canon(src).unwrap());
        // Every array is emptied, and the maps keep their members.
        assert_eq!(left, canon(r#"{"a":[],"b":[],"d":{"e":"f"}}"#).unwrap());
        // json's maps are emptied too.
        let (pruned, left) = run(2);
        assert_eq!(pruned, whole);
        assert_eq!(left, "{}");
    }

    #[test]
    fn a_run_that_prunes_needs_a_listed_grammar_and_sends_nothing_without_one() {
        for named in [None, Some("chess"), Some("Json"), Some("")] {
            let mut source = ParserEventSource::new(tabnas_json::make(), "[1]").prune();
            if let Some(name) = named {
                source = source.grammar(name);
            }
            let (r, seen) = source.run_owned(Seen::default());
            let err = r.unwrap_err();
            assert_eq!(err.code, Code::StreamabilityUnknown, "{named:?}");
            assert!(err.message.contains("capability::prunable"), "{named:?}");
            assert!(seen.0.is_empty(), "{named:?}");
        }
        // A listed name opens the gate, the suite's switch lifts it, and a
        // run that does not prune needs no name.
        for source in [
            ParserEventSource::new(tabnas_json::make(), "[1]")
                .grammar("json")
                .prune(),
            ParserEventSource::new(tabnas_json::make(), "[1]")
                .prune()
                .unverified(),
            ParserEventSource::new(tabnas_json::make(), "[1]").grammar("chess"),
        ] {
            let (r, seen) = source.run_owned(Seen::default());
            assert_eq!(r.unwrap(), Flow::Continue);
            assert_eq!(seen.0.last(), Some(&ParserEvent::End));
        }
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
