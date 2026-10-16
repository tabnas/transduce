//! [`TransduceTree`]: alchemy's `transduce-tree` run natively, the
//! parser's rule passes (`ParserEvents/1`) in and a tree's `JsonEvents/1`
//! out, as a [`TreeSpec`] keyed by rule name says.
//!
//! The library's text (`stdlib/transduce.alc`) is the specification, and
//! alchemy's `TreeSpec` documents what a native must do: each pass is
//! looked up by its rule's name, one lookup per pass whatever the spec's
//! size; an open pass announces its key, then begins its container or
//! yields its scalar from one of its tokens; a close pass ends its
//! container, or yields its own value; and a container whose rule is
//! replaced by a rule the spec names a container is one container, so that
//! close ends nothing and the replacing open begins nothing. The state is
//! the count of open containers and whether a close handed its container
//! on, so the native retains the document's nesting and nothing of its
//! width or length, as the library's step does. alchemy-cli's differential
//! test holds the two to the same bytes.
//!
//! A pass's events are worked out before the first of them is written, as
//! the library's step returns them together: a pass that fails writes
//! none. The failures are the library's, with its codes and texts.

use crate::error::{Code, Fail};
use crate::event::{JsonEvent, Number};
use crate::sink::{Flow, Sink};
use crate::{
    Datum, ParserEvent, ParserSink, RulePass, TreeContainer, TreeKey, TreeScalar, TreeSpec,
};

/// The library's `transduce-tree`, natively, writing to `down`.
pub struct TransduceTree<S> {
    spec: TreeSpec,
    /// Containers open.
    open: usize,
    /// A close handed its container on to the rule replacing it.
    carry: bool,
    down: S,
}

impl<S: Sink> TransduceTree<S> {
    pub fn new(spec: TreeSpec, down: S) -> TransduceTree<S> {
        TransduceTree {
            spec,
            open: 0,
            carry: false,
            down,
        }
    }

    /// The sink the events go to.
    pub fn into_inner(self) -> S {
        self.down
    }

    fn open_pass(&mut self, pass: &RulePass) -> Result<Flow, Fail> {
        let Some(rule) = self.spec.rule(&pass.name) else {
            return Ok(Flow::Continue);
        };
        let key: Option<&str> = match &rule.key {
            None => None,
            Some(TreeKey::Name(name)) => Some(name),
            Some(TreeKey::Token(i)) => match pass.tokens.get(*i) {
                None => None,
                Some(token) => match &token.val {
                    Datum::String(name) => Some(name),
                    other => {
                        return Err(type_error(format!(
                            "key: the name must be a string, not {}",
                            kind(other)
                        )))
                    }
                },
            },
        };
        let start = match rule.container {
            Some(TreeContainer::Object) => Some(JsonEvent::ObjectStart),
            Some(TreeContainer::Array) => Some(JsonEvent::ArrayStart),
            None => None,
        };
        let mut then: Option<JsonEvent<'_>> = None;
        match start {
            Some(start) => {
                if self.carry {
                    self.carry = false;
                } else {
                    self.open += 1;
                    then = Some(start);
                }
            }
            None => {
                if let Some(TreeScalar::Token(i)) = rule.scalar {
                    if let Some(token) = pass.tokens.get(i) {
                        then = Some(scalar(&token.val)?);
                    }
                }
            }
        }
        if let Some(key) = key {
            if self.down.event(JsonEvent::Key(key))? == Flow::Stop {
                return Ok(Flow::Stop);
            }
        }
        match then {
            Some(ev) => self.down.event(ev),
            None => Ok(Flow::Continue),
        }
    }

    fn close_pass(&mut self, pass: &RulePass) -> Result<Flow, Fail> {
        let Some(rule) = self.spec.rule(&pass.name) else {
            return Ok(Flow::Continue);
        };
        let end = match rule.container {
            Some(TreeContainer::Object) => JsonEvent::ObjectEnd,
            Some(TreeContainer::Array) => JsonEvent::ArrayEnd,
            None => {
                return match (rule.scalar, &pass.value) {
                    (Some(TreeScalar::Value), Some(value)) => {
                        let ev = scalar(value)?;
                        self.down.event(ev)
                    }
                    _ => Ok(Flow::Continue),
                };
            }
        };
        if pass
            .alt
            .as_ref()
            .is_some_and(|alt| self.spec.is_container(&alt.r))
        {
            self.carry = true;
            return Ok(Flow::Continue);
        }
        let Some(open) = self.open.checked_sub(1) else {
            return Err(type_error(
                "pop: the vector is empty; there is no last item to remove",
            ));
        };
        self.open = open;
        self.carry = false;
        self.down.event(end)
    }
}

impl<S: Sink> ParserSink for TransduceTree<S> {
    fn event(&mut self, ev: &ParserEvent) -> Result<Flow, Fail> {
        match ev {
            ParserEvent::Open(pass) => self.open_pass(pass),
            ParserEvent::Close(pass) => self.close_pass(pass),
            ParserEvent::End => {
                if self.open > 0 {
                    return Err(Fail::new(
                        Code::ProtocolOrderError,
                        "the rule passes ended inside a container, which a tree's never do",
                    ));
                }
                self.down.event(JsonEvent::End)
            }
        }
    }
}

/// A value as the library's `scalar` takes it, or its refusal.
fn scalar(value: &Datum) -> Result<JsonEvent<'_>, Fail> {
    Ok(match value {
        Datum::Null => JsonEvent::Null,
        Datum::Bool(b) => JsonEvent::Bool(*b),
        Datum::Number { value, lexeme } => JsonEvent::Number(Number {
            value: *value,
            lexeme: lexeme.as_deref(),
        }),
        Datum::String(s) => JsonEvent::String(s),
        other => {
            return Err(type_error(format!(
                "scalar: the value must be null, a boolean, a number or a string, not {}",
                kind(other)
            )))
        }
    })
}

/// What a value is, in the runtime's words.
fn kind(value: &Datum) -> &'static str {
    match value {
        Datum::Null => "null",
        Datum::Bool(_) => "a boolean",
        Datum::Number { .. } => "a number",
        Datum::String(_) => "a string",
        Datum::Array(_) => "a vector",
        Datum::Object(_) => "a record",
    }
}

/// The runtime's type error: `DSL_TYPE_ERROR`, the `type_mismatch` word
/// first.
fn type_error(message: impl std::fmt::Display) -> Fail {
    Fail::new(Code::DslTypeError, format!("type_mismatch: {message}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::OwnedJsonEvent;
    use crate::{
        OwnedJsonEvent as Ev, ParserEventSource, ParserSource, PassAlt, PassToken, SourceMode,
        TreeRule,
    };

    fn json_spec() -> TreeSpec {
        let rule = |container, key, scalar| TreeRule {
            container,
            key,
            scalar,
        };
        TreeSpec::new()
            .with("map", rule(Some(TreeContainer::Object), None, None))
            .with("list", rule(Some(TreeContainer::Array), None, None))
            .with("pair", rule(None, Some(TreeKey::Token(0)), None))
            .with("val", rule(None, None, Some(TreeScalar::Token(0))))
    }

    fn tree(src: &str) -> (Result<Flow, Fail>, Vec<OwnedJsonEvent>) {
        let (r, native) = ParserEventSource::new(tabnas_json::make(), src).run_owned(
            TransduceTree::new(json_spec(), Vec::<OwnedJsonEvent>::new()),
        );
        (r, native.into_inner())
    }

    /// The json part's spec over tabnas-json's own passes makes the
    /// document's events, lexemes kept, as the incremental source makes
    /// them.
    #[test]
    fn the_json_spec_makes_the_documents_events() {
        for src in [
            r#"{"a":[1,2.50,"x",{"b":null}],"c":{},"d":[],"e":1e21,"f":true}"#,
            "[]",
            "{}",
            "[[[]],[{}]]",
            r#"{"a":{"b":{"c":[1,[2,[3]]]}}}"#,
            " 42 ",
            r#""s""#,
            "null",
        ] {
            let (r, native) = tree(src);
            assert_eq!(r.unwrap(), Flow::Continue, "{src}");
            let (r, incremental) = ParserSource::new(tabnas_json::make(), src)
                .grammar("json")
                .mode(SourceMode::Incremental {
                    prune: crate::Prune::Never,
                })
                .run_owned(Vec::<OwnedJsonEvent>::new());
            r.unwrap();
            assert_eq!(native, incremental, "{src}");
        }
    }

    fn pass(name: &str, tokens: Vec<PassToken>, r: &str, value: Option<Datum>) -> RulePass {
        RulePass {
            name: name.into(),
            d: 1,
            i: 1,
            alt: Some(PassAlt {
                p: "".into(),
                r: r.into(),
                b: 0,
                g: vec![],
            }),
            tokens,
            forced: false,
            n: vec![],
            value,
        }
    }

    fn token(val: Datum) -> PassToken {
        PassToken {
            name: "#T".into(),
            val,
            src: "".into(),
            row: 1,
            col: 1,
        }
    }

    fn run(spec: TreeSpec, events: &[ParserEvent]) -> (Result<Flow, Fail>, Vec<OwnedJsonEvent>) {
        let mut native = TransduceTree::new(spec, Vec::<OwnedJsonEvent>::new());
        for ev in events {
            if let Err(fail) = native.event(ev) {
                return (Err(fail), native.into_inner());
            }
        }
        (Ok(Flow::Continue), native.into_inner())
    }

    /// A key named by the spec is announced as it stands; a scalar from a
    /// close pass's value; a rule the spec does not name yields nothing.
    #[test]
    fn a_named_key_a_closes_value_and_an_unnamed_rule() {
        let spec = TreeSpec::new()
            .with(
                "doc",
                TreeRule {
                    container: Some(TreeContainer::Object),
                    ..TreeRule::default()
                },
            )
            .with(
                "v",
                TreeRule {
                    key: Some(TreeKey::Name("k".into())),
                    scalar: Some(TreeScalar::Value),
                    ..TreeRule::default()
                },
            );
        let n = |v: f64| Datum::Number {
            value: v,
            lexeme: None,
        };
        let (r, out) = run(
            spec,
            &[
                ParserEvent::Open(pass("doc", vec![], "", None)),
                ParserEvent::Open(pass("v", vec![token(n(9.0))], "", None)),
                ParserEvent::Open(pass("other", vec![token(n(8.0))], "", None)),
                ParserEvent::Close(pass("other", vec![], "", Some(n(8.0)))),
                ParserEvent::Close(pass("v", vec![], "", Some(n(7.0)))),
                ParserEvent::Close(pass("doc", vec![], "", None)),
                ParserEvent::End,
            ],
        );
        r.unwrap();
        assert_eq!(
            out,
            [
                Ev::ObjectStart,
                Ev::Key("k".into()),
                Ev::Number {
                    value: 7.0,
                    lexeme: None
                },
                Ev::ObjectEnd,
                Ev::End
            ]
        );
    }

    /// The library's refusals, with its codes and texts: a key that is not
    /// a string, a scalar that is a container, a close with nothing open,
    /// and passes that end inside a container. A failing pass writes
    /// nothing, not even the key it would have announced.
    #[test]
    fn the_librarys_refusals() {
        let keyed = TreeSpec::new().with(
            "v",
            TreeRule {
                key: Some(TreeKey::Name("k".into())),
                scalar: Some(TreeScalar::Token(0)),
                ..TreeRule::default()
            },
        );
        let (r, out) = run(
            keyed,
            &[ParserEvent::Open(pass(
                "v",
                vec![token(Datum::Array(vec![]))],
                "",
                None,
            ))],
        );
        let err = r.unwrap_err();
        assert_eq!(err.code, Code::DslTypeError);
        assert_eq!(
            err.message,
            "type_mismatch: scalar: the value must be null, a boolean, a number or a string, not a vector"
        );
        assert!(out.is_empty());

        let (r, _) = run(
            json_spec(),
            &[ParserEvent::Open(pass(
                "pair",
                vec![token(Datum::Bool(true))],
                "",
                None,
            ))],
        );
        assert_eq!(
            r.unwrap_err().message,
            "type_mismatch: key: the name must be a string, not a boolean"
        );

        let (r, _) = run(
            json_spec(),
            &[ParserEvent::Close(pass("map", vec![], "", None))],
        );
        assert_eq!(
            r.unwrap_err().message,
            "type_mismatch: pop: the vector is empty; there is no last item to remove"
        );

        let (r, _) = run(
            json_spec(),
            &[
                ParserEvent::Open(pass("list", vec![], "", None)),
                ParserEvent::End,
            ],
        );
        let err = r.unwrap_err();
        assert_eq!(err.code, Code::ProtocolOrderError);
        assert_eq!(
            err.message,
            "the rule passes ended inside a container, which a tree's never do"
        );
    }

    /// A close whose alternate replaces its rule with a container rule
    /// hands the container on: one start, one end, across the two rules.
    #[test]
    fn a_replaced_container_is_one_container() {
        let spec = TreeSpec::new()
            .with(
                "a",
                TreeRule {
                    container: Some(TreeContainer::Array),
                    ..TreeRule::default()
                },
            )
            .with(
                "b",
                TreeRule {
                    container: Some(TreeContainer::Array),
                    ..TreeRule::default()
                },
            );
        let (r, out) = run(
            spec,
            &[
                ParserEvent::Open(pass("a", vec![], "", None)),
                ParserEvent::Close(pass("a", vec![], "b", None)),
                ParserEvent::Open(pass("b", vec![], "", None)),
                ParserEvent::Close(pass("b", vec![], "", None)),
                ParserEvent::End,
            ],
        );
        r.unwrap();
        assert_eq!(out, [Ev::ArrayStart, Ev::ArrayEnd, Ev::End]);
    }
}
