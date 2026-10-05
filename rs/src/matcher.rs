//! Shared-prefix matching of many selectors in one pass.
//!
//! The router has to recognize every capture's selector over one event
//! stream, and a document can be hundreds of megabytes, so the matcher
//! does no per-event allocation and never spells out a path: the
//! selectors are compiled into one trie of [`Step`]s, and the matcher
//! walks the document with a stack of open containers (a [`Level`] per
//! container: its kind, how many values it has started, the current
//! member's key) and, per level, the set of trie nodes that named that
//! container. A value's position names at most one child per node
//! (`Property(k)` or `Index(i)`) plus the wildcard child (`EachMember`
//! or `EachIndex`), so the sets stay tiny: one or two nodes for the
//! usual plan, and they live in one flat vector indexed by level rather
//! than in a set per level.
//!
//! The matcher also validates the protocol as it goes, because it holds
//! the state that makes a malformed stream visible (a key outside an
//! object, an array ending an object, a value after the root). Every
//! stage downstream of it can therefore trust the sequence. A concrete
//! [`Path`] is built only on request, for a delivered match or a
//! failure, from the levels' current keys and indexes.

use crate::error::Fail;
use crate::event::JsonEvent;
use crate::selector::{Path, Segment, Selector, Step};

/// Which selector matched: its position in the slice given to
/// [`Matcher::new`]. One of alchemy's shared types.
pub use tabnas_alchemy::shared::matcher::CaptureId;

type NodeId = usize;

const ROOT: NodeId = 0;

/// One trie node: the selectors' next steps below one position.
#[derive(Debug, Default)]
struct Node {
    properties: Vec<(Box<str>, NodeId)>,
    indexes: Vec<(usize, NodeId)>,
    each_index: Option<NodeId>,
    each_member: Option<NodeId>,
    /// The selectors that end here, in id order.
    terminals: Vec<CaptureId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Array,
    Object,
}

/// One open container. Levels are reused across the run, so the key
/// buffer keeps its capacity and a member costs no allocation.
#[derive(Debug)]
struct Level {
    kind: Kind,
    /// Values started in this container so far; the current or last one
    /// is at index `started - 1`.
    started: usize,
    /// The current member's key, for objects.
    key: String,
    /// Between a key and its value.
    in_value: bool,
    /// The trie nodes that named this container: `node_stack[start..end]`.
    start: usize,
    end: usize,
}

impl Level {
    fn reset(&mut self, kind: Kind, start: usize, end: usize) {
        self.kind = kind;
        self.started = 0;
        self.key.clear();
        self.in_value = false;
        self.start = start;
        self.end = end;
    }
}

/// What one event did to the document's structure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HitKind {
    /// A member name; the value follows.
    Key,
    /// A container began.
    Start,
    /// A whole scalar value: it began and completed here.
    Scalar,
    /// A container completed.
    Close,
    /// The document completed.
    End,
}

/// The matcher's answer for one event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Hit {
    pub kind: HitKind,
    /// How many captures begin with this event: their ids are
    /// [`Matcher::begins`], valid until the next event. Zero unless `kind`
    /// is `Start` or `Scalar`. A count rather than a borrowed slice, so a
    /// stage can hold the hit while it asks the matcher for a path.
    pub begins: usize,
    /// For `Start` and `Scalar`, the number of containers enclosing the
    /// value; for `Close`, the number enclosing the container that
    /// closed. The value that began at a depth completes at the same
    /// depth, which is how a stage pairs the two without keeping a path.
    pub depth: usize,
}

/// Recognizes every selector of a set over one event stream.
#[derive(Debug)]
pub struct Matcher {
    nodes: Vec<Node>,
    levels: Vec<Level>,
    depth: usize,
    /// The active nodes of every open level, then the nodes of the value
    /// being started, as one stack.
    node_stack: Vec<NodeId>,
    /// Scratch for [`Hit::begins`].
    begins: Vec<CaptureId>,
    root_done: bool,
    ended: bool,
}

impl Matcher {
    /// Compile the selectors; their positions are the capture ids.
    pub fn new(selectors: &[Selector]) -> Matcher {
        let mut nodes = vec![Node::default()];
        for (id, selector) in selectors.iter().enumerate() {
            let mut at = ROOT;
            for step in selector.steps() {
                at = child(&mut nodes, at, step);
            }
            nodes[at].terminals.push(id);
        }
        Matcher {
            nodes,
            levels: Vec::new(),
            depth: 0,
            node_stack: Vec::with_capacity(8),
            begins: Vec::new(),
            root_done: false,
            ended: false,
        }
    }

    /// Open containers right now.
    pub fn depth(&self) -> usize {
        self.depth
    }

    /// Whether `End` has been seen.
    pub fn ended(&self) -> bool {
        self.ended
    }

    /// The concrete path of the value at `depth` enclosing containers: the
    /// current position of each of those containers. Asked at a `Start`
    /// or `Scalar` hit it names the value; asked at a `Close` hit it names
    /// the container that closed. Allocates, so it is for deliveries and
    /// failures, not for every event.
    pub fn path(&self, depth: usize) -> Path {
        let mut path = Path::root();
        for level in self.levels.iter().take(depth.min(self.depth)) {
            path.push(match level.kind {
                Kind::Array => Segment::Index(level.started.saturating_sub(1)),
                Kind::Object => Segment::Key(level.key.as_str().into()),
            });
        }
        path
    }

    /// The captures that began with the last event, in id order.
    pub fn begins(&self) -> &[CaptureId] {
        &self.begins
    }

    /// One event. A malformed sequence is a `PROTOCOL_ORDER_ERROR`.
    pub fn event(&mut self, ev: JsonEvent<'_>) -> Result<Hit, Fail> {
        if self.ended {
            return Err(Fail::protocol("an event arrived after the document ended"));
        }
        match ev {
            JsonEvent::Key(k) => {
                let level = self
                    .depth
                    .checked_sub(1)
                    .map(|d| &mut self.levels[d])
                    .filter(|l| l.kind == Kind::Object)
                    .ok_or_else(|| Fail::protocol("a key arrived outside an object"))?;
                if level.in_value {
                    return Err(Fail::protocol("a key arrived where a value was expected"));
                }
                level.key.clear();
                level.key.push_str(k);
                level.in_value = true;
                self.begins.clear();
                Ok(Hit {
                    kind: HitKind::Key,
                    begins: 0,
                    depth: self.depth,
                })
            }
            JsonEvent::ObjectStart | JsonEvent::ArrayStart => {
                let depth = self.depth;
                let start = self.begin_value()?;
                let end = self.node_stack.len();
                let kind = if ev == JsonEvent::ObjectStart {
                    Kind::Object
                } else {
                    Kind::Array
                };
                if depth == self.levels.len() {
                    self.levels.push(Level {
                        kind,
                        started: 0,
                        key: String::new(),
                        in_value: false,
                        start,
                        end,
                    });
                } else {
                    self.levels[depth].reset(kind, start, end);
                }
                self.depth += 1;
                Ok(Hit {
                    kind: HitKind::Start,
                    begins: self.begins.len(),
                    depth,
                })
            }
            JsonEvent::ObjectEnd | JsonEvent::ArrayEnd => {
                let want = if ev == JsonEvent::ObjectEnd {
                    Kind::Object
                } else {
                    Kind::Array
                };
                let level = self
                    .depth
                    .checked_sub(1)
                    .map(|d| &self.levels[d])
                    .ok_or_else(|| Fail::protocol("a container ended that had not started"))?;
                if level.kind != want {
                    return Err(Fail::protocol(match want {
                        Kind::Object => "an object ended inside an array",
                        Kind::Array => "an array ended inside an object",
                    }));
                }
                if level.in_value {
                    return Err(Fail::protocol(
                        "an object ended after a key without its value",
                    ));
                }
                let start = level.start;
                self.depth -= 1;
                self.node_stack.truncate(start);
                self.complete_value();
                self.begins.clear();
                Ok(Hit {
                    kind: HitKind::Close,
                    begins: 0,
                    depth: self.depth,
                })
            }
            JsonEvent::End => {
                if self.depth > 0 {
                    return Err(Fail::protocol("the document ended inside a container"));
                }
                if !self.root_done {
                    return Err(Fail::protocol("the document ended before its root value"));
                }
                self.ended = true;
                self.begins.clear();
                Ok(Hit {
                    kind: HitKind::End,
                    begins: 0,
                    depth: 0,
                })
            }
            JsonEvent::Null | JsonEvent::Bool(_) | JsonEvent::Number(_) | JsonEvent::String(_) => {
                let depth = self.depth;
                let start = self.begin_value()?;
                self.node_stack.truncate(start);
                self.complete_value();
                Ok(Hit {
                    kind: HitKind::Scalar,
                    begins: self.begins.len(),
                    depth,
                })
            }
        }
    }

    /// A value begins at the current position: check that one is allowed
    /// here, push the trie nodes naming it onto the node stack, and fill
    /// `begins`. Returns where on the node stack the value's nodes start.
    fn begin_value(&mut self) -> Result<usize, Fail> {
        let base = match self.depth.checked_sub(1) {
            None => {
                if self.root_done {
                    return Err(Fail::protocol("a second root value arrived"));
                }
                self.node_stack.clear();
                self.node_stack.push(ROOT);
                0
            }
            Some(d) => {
                let level = &mut self.levels[d];
                if level.kind == Kind::Object && !level.in_value {
                    return Err(Fail::protocol(
                        "a value arrived inside an object without a key",
                    ));
                }
                let (start, end, kind, index) = (level.start, level.end, level.kind, level.started);
                level.started += 1;
                self.node_stack.truncate(end);
                for at in start..end {
                    let node = &self.nodes[self.node_stack[at]];
                    match kind {
                        Kind::Array => {
                            if let Some(c) = node.each_index {
                                self.node_stack.push(c);
                            }
                            if let Some((_, c)) = node.indexes.iter().find(|(i, _)| *i == index) {
                                self.node_stack.push(*c);
                            }
                        }
                        Kind::Object => {
                            if let Some(c) = node.each_member {
                                self.node_stack.push(c);
                            }
                            let key = self.levels[d].key.as_str();
                            if let Some((_, c)) = node.properties.iter().find(|(k, _)| &**k == key)
                            {
                                self.node_stack.push(*c);
                            }
                        }
                    }
                }
                end
            }
        };
        self.begins.clear();
        for &node in &self.node_stack[base..] {
            self.begins.extend_from_slice(&self.nodes[node].terminals);
        }
        if self.begins.len() > 1 {
            self.begins.sort_unstable();
        }
        Ok(base)
    }

    /// The value at the current position is complete.
    fn complete_value(&mut self) {
        match self.depth.checked_sub(1) {
            None => self.root_done = true,
            Some(d) => self.levels[d].in_value = false,
        }
    }
}

/// The child of `at` for `step`, created on first use.
fn child(nodes: &mut Vec<Node>, at: NodeId, step: &Step) -> NodeId {
    let existing = match step {
        Step::Property(name) => nodes[at]
            .properties
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, c)| *c),
        Step::Index(i) => nodes[at]
            .indexes
            .iter()
            .find(|(j, _)| j == i)
            .map(|(_, c)| *c),
        Step::EachIndex => nodes[at].each_index,
        Step::EachMember => nodes[at].each_member,
    };
    if let Some(c) = existing {
        return c;
    }
    let c = nodes.len();
    nodes.push(Node::default());
    match step {
        Step::Property(name) => nodes[at].properties.push((name.clone(), c)),
        Step::Index(i) => nodes[at].indexes.push((*i, c)),
        Step::EachIndex => nodes[at].each_index = Some(c),
        Step::EachMember => nodes[at].each_member = Some(c),
    }
    c
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Code;
    use crate::event::{Number, OwnedJsonEvent};

    /// Feed a document; collect `(path, ids)` for every value that begins
    /// a capture, and every path the matcher reports on `Close`.
    fn run(selectors: &[Selector], events: &[OwnedJsonEvent]) -> Vec<(String, Vec<CaptureId>)> {
        let mut m = Matcher::new(selectors);
        let mut out = Vec::new();
        for ev in events {
            let hit = m.event(ev.as_event()).unwrap();
            if hit.begins > 0 {
                let path = m.path(hit.depth).to_string();
                out.push((path, m.begins().to_vec()));
            }
        }
        assert!(m.ended());
        out
    }

    fn doc(json: &str) -> Vec<OwnedJsonEvent> {
        let d = crate::Datum::from_json(&serde_json::from_str(json).unwrap());
        let mut rec: Vec<OwnedJsonEvent> = Vec::new();
        crate::walk_datum(&d, &mut rec).unwrap();
        rec.push(OwnedJsonEvent::End);
        rec
    }

    fn sel() -> Selector {
        Selector::root()
    }

    #[test]
    fn a_property_chain_names_one_value() {
        let hits = run(
            &[sel().property("a").property("b")],
            &doc(r#"{"a":{"b":[1,2],"c":3},"b":{"b":4}}"#),
        );
        assert_eq!(hits, vec![(".a.b".to_string(), vec![0])]);
    }

    #[test]
    fn each_index_names_every_element_with_its_index() {
        let hits = run(
            &[sel().property("xs").each_index()],
            &doc(r#"{"xs":[10,{"y":1},[2]],"ys":[9]}"#),
        );
        assert_eq!(
            hits,
            vec![
                (".xs[0]".to_string(), vec![0]),
                (".xs[1]".to_string(), vec![0]),
                (".xs[2]".to_string(), vec![0]),
            ]
        );
    }

    #[test]
    fn each_member_names_every_member_value() {
        let hits = run(
            &[sel().each_member()],
            &doc(r#"{"a":1,"odd key":{"z":2},"c":[3]}"#),
        );
        assert_eq!(
            hits,
            vec![
                (".a".to_string(), vec![0]),
                (".\"odd key\"".to_string(), vec![0]),
                (".c".to_string(), vec![0]),
            ]
        );
    }

    #[test]
    fn an_index_names_one_element() {
        let hits = run(&[sel().index(1)], &doc(r#"[[0,1],[2,3],[4,5]]"#));
        assert_eq!(hits, vec![("[1]".to_string(), vec![0])]);
    }

    #[test]
    fn the_root_selector_names_the_root_value() {
        assert_eq!(
            run(&[sel()], &doc(r#"{"a":[1]}"#)),
            vec![(".".to_string(), vec![0])]
        );
        assert_eq!(run(&[sel()], &doc("42")), vec![(".".to_string(), vec![0])]);
    }

    #[test]
    fn two_selectors_sharing_a_prefix_match_independently() {
        let selectors = [
            sel().property("response").property("metadata"),
            sel().property("response").property("records").each_index(),
        ];
        let hits = run(
            &selectors,
            &doc(r#"{"response":{"metadata":[1],"records":[{"id":1},{"id":2}]},"records":[0]}"#),
        );
        assert_eq!(
            hits,
            vec![
                (".response.metadata".to_string(), vec![0]),
                (".response.records[0]".to_string(), vec![1]),
                (".response.records[1]".to_string(), vec![1]),
            ]
        );
    }

    #[test]
    fn two_selectors_naming_the_same_value_both_begin_in_id_order() {
        let selectors = [sel().property("a"), sel().each_member()];
        let hits = run(&selectors, &doc(r#"{"a":1,"b":2}"#));
        assert_eq!(
            hits,
            vec![(".a".to_string(), vec![0, 1]), (".b".to_string(), vec![1])]
        );
    }

    #[test]
    fn a_selector_that_matches_nothing_is_silent() {
        let hits = run(
            &[sel().property("missing").each_index(), sel().index(5)],
            &doc(r#"{"a":[1,2,3]}"#),
        );
        assert!(hits.is_empty());
    }

    #[test]
    fn nested_arrays_track_each_level_independently() {
        let hits = run(
            &[sel().each_index().each_index()],
            &doc(r#"[[1,2],[],[[3]]]"#),
        );
        assert_eq!(
            hits,
            vec![
                ("[0][0]".to_string(), vec![0]),
                ("[0][1]".to_string(), vec![0]),
                ("[2][0]".to_string(), vec![0]),
            ]
        );
    }

    #[test]
    fn indexes_and_properties_do_not_cross_kinds() {
        let hits = run(&[sel().index(0), sel().property("0")], &doc(r#"{"0":[7]}"#));
        assert_eq!(hits, vec![(".\"0\"".to_string(), vec![1])]);
    }

    #[test]
    fn a_close_reports_the_path_of_the_container_that_closed() {
        let mut m = Matcher::new(&[]);
        let mut closes = Vec::new();
        for ev in doc(r#"{"a":[{"b":1},{"c":[]}]}"#) {
            let hit = m.event(ev.as_event()).unwrap();
            if hit.kind == HitKind::Close {
                closes.push(m.path(hit.depth).to_string());
            }
        }
        assert_eq!(closes, vec![".a[0]", ".a[1].c", ".a[1]", ".a", "."]);
    }

    #[test]
    fn depth_is_the_number_of_enclosing_containers() {
        let mut m = Matcher::new(&[]);
        let mut seen = Vec::new();
        for ev in doc(r#"[1,[2]]"#) {
            let hit = m.event(ev.as_event()).unwrap();
            seen.push((hit.kind, hit.depth));
        }
        assert_eq!(
            seen,
            vec![
                (HitKind::Start, 0),
                (HitKind::Scalar, 1),
                (HitKind::Start, 1),
                (HitKind::Scalar, 2),
                (HitKind::Close, 1),
                (HitKind::Close, 0),
                (HitKind::End, 0),
            ]
        );
    }

    fn protocol_error(events: &[JsonEvent<'_>]) -> Code {
        let mut m = Matcher::new(&[sel()]);
        let mut last = None;
        for ev in events {
            match m.event(*ev) {
                Ok(_) => {}
                Err(e) => {
                    last = Some(e.code);
                    break;
                }
            }
        }
        last.expect("the stream should have been rejected")
    }

    #[test]
    fn malformed_streams_are_protocol_errors() {
        use JsonEvent::{ArrayEnd, ArrayStart, End, Key, ObjectEnd, ObjectStart};
        let one = JsonEvent::Number(Number::new(1.0));
        let cases: &[&[JsonEvent<'_>]] = &[
            &[Key("a")],
            &[ObjectStart, one],
            &[ObjectStart, Key("a"), Key("b")],
            &[ObjectStart, Key("a"), ObjectEnd],
            &[ArrayStart, ObjectEnd],
            &[ObjectStart, ArrayEnd],
            &[ArrayEnd],
            &[one, one],
            &[End],
            &[ArrayStart, End],
            &[one, End, End],
            &[ArrayStart, Key("a")],
        ];
        for case in cases {
            assert_eq!(protocol_error(case), Code::ProtocolOrderError, "{case:?}");
        }
    }

    #[test]
    fn levels_are_reused_without_stale_state() {
        let hits = run(
            &[sel().each_index().property("k")],
            &doc(r#"[{"k":1,"other":2},{"other":3},{"k":4}]"#),
        );
        assert_eq!(
            hits,
            vec![
                ("[0].k".to_string(), vec![0]),
                ("[2].k".to_string(), vec![0])
            ]
        );
    }
}
