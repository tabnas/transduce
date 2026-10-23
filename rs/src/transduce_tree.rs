//! [`TransduceTree`]: alchemy's `transduce-tree` run natively, the
//! parser's rule passes (`ParserEvents/1`) in and a tree's `JsonEvents/1`
//! out, as a [`TreeSpec`] keyed by rule name says.
//!
//! The library's text (`stdlib/transduce.alc`) is the specification, and
//! alchemy's `TreeSpec` documents what a native must do: each pass is
//! looked up by its rule's name, one lookup per pass whatever the spec's
//! size; an open pass announces its key, then begins its container or
//! yields its scalar from one of its tokens, then announces its member; a
//! close pass yields its text, when its rule is a `:text` rule, then ends
//! its container, or yields its own value; and a container whose rule is
//! replaced by a rule the spec names a container is one container, so that
//! close ends nothing and the replacing open begins nothing. The state is
//! the count of open containers, whether a close handed its container on,
//! and the text each open `:text` rule has gathered, to which every pass
//! writes its tokens' source text, so the native retains the document's
//! nesting and an open rule's text, and nothing else of its width or
//! length, as the library's step does. alchemy-cli's differential test
//! holds the two to the same bytes.
//!
//! A text is held to the spec's `max_text_bytes` as it grows, at the joins
//! the library's `string-join` makes and in their order: a pass's tokens
//! together, then the innermost text with them, and a closed text with the
//! text around it, each refused past the bound with alchemy's
//! [`tree_text_refusal`]. The routers say this native reads `:member` and
//! `:text` ([`crate::routers()`]), which is what lets the lowering hand it a
//! spec that uses either.
//!
//! A pass's events are worked out before the first of them is written, as
//! the library's step returns them together: a pass that fails writes
//! none. The failures are the library's, with its codes and texts, and in
//! its order: an open pass's key, then what it begins or yields, then its
//! tokens' text, then its member; a close pass's text, then what it ends
//! or yields.

use crate::error::{Code, Fail};
use crate::event::{JsonEvent, Number};
use crate::sink::{Flow, Sink};
use crate::{
    tree_text_refusal, Datum, ParserEvent, ParserSink, RulePass, TreeContainer, TreeKey,
    TreeScalar, TreeSpec,
};

/// The library's `transduce-tree`, natively, writing to `down`.
pub struct TransduceTree<S> {
    spec: TreeSpec,
    /// Containers open.
    open: usize,
    /// A close handed its container on to the rule replacing it.
    carry: bool,
    /// The text each open `:text` rule has gathered, the innermost last.
    texts: Vec<String>,
    down: S,
}

impl<S: Sink> TransduceTree<S> {
    pub fn new(spec: TreeSpec, down: S) -> TransduceTree<S> {
        TransduceTree {
            spec,
            open: 0,
            carry: false,
            texts: Vec::new(),
            down,
        }
    }

    /// The sink the events go to.
    pub fn into_inner(self) -> S {
        self.down
    }

    fn open_pass(&mut self, pass: &RulePass) -> Result<Flow, Fail> {
        let max = self.spec.max_text_bytes;
        let Some(rule) = self.spec.rule(&pass.name) else {
            spell(&mut self.texts, pass, max)?;
            return Ok(Flow::Continue);
        };
        let key = named(&rule.key, pass, "key")?;
        let start = match rule.container {
            Some(TreeContainer::Object) => Some(JsonEvent::ObjectStart),
            Some(TreeContainer::Array) => Some(JsonEvent::ArrayStart),
            None => None,
        };
        let mut then: Option<JsonEvent<'_>> = None;
        let mut begins = false;
        match start {
            Some(start) => {
                if !self.carry {
                    begins = true;
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
        spell(&mut self.texts, pass, max)?;
        let member = named(&rule.member, pass, "key")?;
        if start.is_some() {
            if begins {
                self.open += 1;
            } else {
                self.carry = false;
            }
        }
        if rule.text {
            self.texts.push(String::new());
        }
        for ev in [key.map(JsonEvent::Key), then, member.map(JsonEvent::Key)]
            .into_iter()
            .flatten()
        {
            if self.down.event(ev)? == Flow::Stop {
                return Ok(Flow::Stop);
            }
        }
        Ok(Flow::Continue)
    }

    fn close_pass(&mut self, pass: &RulePass) -> Result<Flow, Fail> {
        let max = self.spec.max_text_bytes;
        let Some(rule) = self.spec.rule(&pass.name) else {
            spell(&mut self.texts, pass, max)?;
            return Ok(Flow::Continue);
        };
        // A `:text` rule's text closes first, and joins the one around it
        // before this pass's own tokens do; it is yielded before anything
        // else, once the rest of the pass is known not to fail.
        let text = if rule.text {
            let Some(text) = self.texts.pop() else {
                return Err(type_error(
                    "top: the vector is empty; there is no last item",
                ));
            };
            if let Some(around) = self.texts.last_mut() {
                write(around, &text, max)?;
            }
            Some(text)
        } else {
            None
        };
        spell(&mut self.texts, pass, max)?;
        let then: Option<JsonEvent<'_>> = match rule.container {
            Some(container) => {
                if pass
                    .alt
                    .as_ref()
                    .is_some_and(|alt| self.spec.is_container(&alt.r))
                {
                    self.carry = true;
                    None
                } else {
                    let Some(open) = self.open.checked_sub(1) else {
                        return Err(type_error(
                            "pop: the vector is empty; there is no last item to remove",
                        ));
                    };
                    self.open = open;
                    self.carry = false;
                    Some(match container {
                        TreeContainer::Object => JsonEvent::ObjectEnd,
                        TreeContainer::Array => JsonEvent::ArrayEnd,
                    })
                }
            }
            None => match (rule.scalar, &pass.value) {
                (Some(TreeScalar::Value), Some(value)) => Some(scalar(value)?),
                _ => None,
            },
        };
        if let Some(text) = &text {
            if self.down.event(JsonEvent::String(text))? == Flow::Stop {
                return Ok(Flow::Stop);
            }
        }
        match then {
            Some(ev) => self.down.event(ev),
            None => Ok(Flow::Continue),
        }
    }
}

/// The pass's tokens written to the innermost open text, if one is and the
/// pass consumed any, as the library's `transduce-spell` writes them: the
/// tokens' text joined, then written to the innermost, each join held to
/// `max` bytes.
fn spell(texts: &mut [String], pass: &RulePass, max: usize) -> Result<(), Fail> {
    let Some(text) = texts.last_mut() else {
        return Ok(());
    };
    if pass.tokens.is_empty() {
        return Ok(());
    }
    let more: usize = pass.tokens.iter().map(|token| token.src.len()).sum();
    if more > max {
        return Err(tree_text_refusal(more, max));
    }
    let len = text.len() + more;
    if len > max {
        return Err(tree_text_refusal(len, max));
    }
    for token in &pass.tokens {
        text.push_str(&token.src);
    }
    Ok(())
}

/// `more` written at the end of `text`, as the library's `transduce-write`
/// joins the two, held to `max` bytes.
fn write(text: &mut String, more: &str, max: usize) -> Result<(), Fail> {
    let len = text.len() + more.len();
    if len > max {
        return Err(tree_text_refusal(len, max));
    }
    text.push_str(more);
    Ok(())
}

/// The member name a field of a rule's entry names for a pass: the string,
/// or the `:val` of the pass's token at that index, nothing when the pass
/// consumed no such token; the library's `key` refuses a name that is not
/// a string.
fn named<'p>(
    field: &'p Option<TreeKey>,
    pass: &'p RulePass,
    native: &str,
) -> Result<Option<&'p str>, Fail> {
    Ok(match field {
        None => None,
        Some(TreeKey::Name(name)) => Some(name),
        Some(TreeKey::Token(i)) => match pass.tokens.get(*i) {
            None => None,
            Some(token) => match &token.val {
                Datum::String(name) => Some(name),
                other => {
                    return Err(type_error(format!(
                        "{native}: the name must be a string, not {}",
                        kind(other)
                    )))
                }
            },
        },
    })
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
        OwnedJsonEvent as Ev, ParserEventSource, ParserSource, PassAlt, PassToken, TreeRule,
    };

    fn json_spec() -> TreeSpec {
        let rule = |container, key, scalar| TreeRule {
            container,
            key,
            scalar,
            ..TreeRule::default()
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
    /// document's events: the walk's, each number with the lexeme the
    /// document spelled it with, which the walk cannot keep.
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
            let (r, walked) =
                ParserSource::new(tabnas_json::make(), src).run_owned(Vec::<OwnedJsonEvent>::new());
            r.unwrap();
            let unspelled: Vec<OwnedJsonEvent> = native
                .iter()
                .map(|e| match e {
                    Ev::Number { value, .. } => Ev::Number {
                        value: *value,
                        lexeme: None,
                    },
                    other => other.clone(),
                })
                .collect();
            assert_eq!(unspelled, walked, "{src}");
        }
        let (_, native) = tree(r#"[2.50, 1e21]"#);
        assert!(native.contains(&Ev::Number {
            value: 2.5,
            lexeme: Some("2.50".into())
        }));
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

    /// A token whose source text is `src`, its value that text.
    fn spelled(src: &str) -> PassToken {
        PassToken {
            src: src.into(),
            ..token(Datum::String(src.into()))
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
    /// a string, a scalar that is a container, a close that ends a
    /// container with nothing open, and passes that end inside a container. A failing pass writes
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

    /// A member is announced after what its rule begins, here a container
    /// one rule begins and a rule it replaces carries on; a `:text` rule's
    /// close yields first the text the passes between its open and it
    /// consumed, its own open's and close's tokens not among them, a rule
    /// the spec does not name writing its tokens too.
    #[test]
    fn a_member_after_the_start_and_a_rules_text() {
        let spec = TreeSpec::new()
            .with(
                "row",
                TreeRule {
                    container: Some(TreeContainer::Object),
                    member: Some(TreeKey::Name("a".into())),
                    text: true,
                    ..TreeRule::default()
                },
            )
            .with(
                "next",
                TreeRule {
                    container: Some(TreeContainer::Object),
                    member: Some(TreeKey::Token(0)),
                    text: true,
                    ..TreeRule::default()
                },
            );
        let (r, out) = run(
            spec,
            &[
                ParserEvent::Open(pass("row", vec![spelled("<")], "", None)),
                ParserEvent::Open(pass("cell", vec![spelled("1"), spelled("2")], "", None)),
                ParserEvent::Close(pass("cell", vec![spelled("3")], "", None)),
                ParserEvent::Close(pass("row", vec![spelled(",")], "next", None)),
                ParserEvent::Open(pass("next", vec![spelled("b")], "", None)),
                ParserEvent::Open(pass("cell", vec![spelled("x")], "", None)),
                ParserEvent::Close(pass("cell", vec![], "", None)),
                ParserEvent::Close(pass("next", vec![spelled(">")], "", None)),
                ParserEvent::End,
            ],
        );
        r.unwrap();
        assert_eq!(
            out,
            [
                Ev::ObjectStart,
                Ev::Key("a".into()),
                Ev::String("123".into()),
                Ev::Key("b".into()),
                Ev::String("x".into()),
                Ev::ObjectEnd,
                Ev::End
            ]
        );
    }

    /// A `:text` rule closed inside another joins its text to the one
    /// around it, its own open's tokens before it and its close's after.
    #[test]
    fn a_text_joins_the_text_around_it() {
        let text = TreeRule {
            text: true,
            ..TreeRule::default()
        };
        let spec = TreeSpec::new()
            .with("outer", text.clone())
            .with("inner", text);
        let (r, out) = run(
            spec,
            &[
                ParserEvent::Open(pass("outer", vec![spelled("[")], "", None)),
                ParserEvent::Open(pass("x", vec![spelled("a")], "", None)),
                ParserEvent::Close(pass("x", vec![], "", None)),
                ParserEvent::Open(pass("inner", vec![spelled("(")], "", None)),
                ParserEvent::Open(pass("x", vec![spelled("b")], "", None)),
                ParserEvent::Close(pass("x", vec![], "", None)),
                ParserEvent::Close(pass("inner", vec![spelled(")")], "", None)),
                ParserEvent::Close(pass("outer", vec![spelled("]")], "", None)),
                ParserEvent::End,
            ],
        );
        r.unwrap();
        assert_eq!(
            out,
            [Ev::String("b".into()), Ev::String("a(b)".into()), Ev::End]
        );
    }

    /// The library's refusals of the two fields, with its codes and texts:
    /// a member whose token is not a string, before anything the pass
    /// would write, and a `:text` rule's close with no text open.
    /// A text is held to the spec's bound at the library's joins, in its
    /// order, and refused as its `string-join` refuses: a pass's tokens
    /// together, then the innermost text with them, then a closed text
    /// with the text around it; a pass that fails writes nothing, and a
    /// text at the bound is not refused.
    #[test]
    fn a_text_is_held_to_its_bound_as_the_library_holds_it() {
        let text = TreeRule {
            text: true,
            ..TreeRule::default()
        };
        let spec = TreeSpec::new()
            .with("outer", text.clone())
            .with("inner", text)
            .with_max_text_bytes(3);
        let open = |name: &str, srcs: &[&str]| {
            ParserEvent::Open(pass(
                name,
                srcs.iter().map(|s| spelled(s)).collect(),
                "",
                None,
            ))
        };
        let close = |name: &str| ParserEvent::Close(pass(name, vec![], "", None));

        // A pass's tokens together, past the bound.
        let (r, out) = run(
            spec.clone(),
            &[open("outer", &[]), open("x", &["ab", "cd"])],
        );
        assert_eq!(r.unwrap_err(), tree_text_refusal(4, 3));
        assert!(out.is_empty());

        // The innermost text with a pass's tokens.
        let (r, _) = run(
            spec.clone(),
            &[
                open("outer", &[]),
                open("x", &["ab"]),
                close("x"),
                open("x", &["cd"]),
            ],
        );
        assert_eq!(r.unwrap_err(), tree_text_refusal(4, 3));

        // A closed text with the text around it: the inner text, within
        // the bound, is not yielded when its join fails.
        let (r, out) = run(
            spec.clone(),
            &[
                open("outer", &[]),
                open("x", &["ab"]),
                close("x"),
                open("inner", &[]),
                open("x", &["cd"]),
                close("x"),
                close("inner"),
            ],
        );
        assert_eq!(r.unwrap_err(), tree_text_refusal(4, 3));
        assert!(out.is_empty());

        // At the bound, nothing is refused.
        let (r, out) = run(
            spec,
            &[
                open("outer", &[]),
                open("x", &["a"]),
                close("x"),
                open("inner", &[]),
                open("x", &["bc"]),
                close("x"),
                close("inner"),
                close("outer"),
                ParserEvent::End,
            ],
        );
        r.unwrap();
        assert_eq!(
            out,
            [Ev::String("bc".into()), Ev::String("abc".into()), Ev::End]
        );

        // An open pass's key is refused before its tokens' text, and its
        // tokens' text before its member.
        let spec = TreeSpec::new()
            .with(
                "outer",
                TreeRule {
                    text: true,
                    ..TreeRule::default()
                },
            )
            .with(
                "keyed",
                TreeRule {
                    key: Some(TreeKey::Token(0)),
                    ..TreeRule::default()
                },
            )
            .with(
                "member",
                TreeRule {
                    member: Some(TreeKey::Token(0)),
                    ..TreeRule::default()
                },
            )
            .with_max_text_bytes(3);
        let wide = |name: &str| {
            let mut t = token(Datum::Bool(true));
            t.src = "true".into();
            ParserEvent::Open(pass(name, vec![t], "", None))
        };
        let (r, _) = run(spec.clone(), &[open("outer", &[]), wide("keyed")]);
        assert_eq!(r.unwrap_err().code, Code::DslTypeError);
        let (r, _) = run(spec, &[open("outer", &[]), wide("member")]);
        assert_eq!(r.unwrap_err(), tree_text_refusal(4, 3));
    }

    #[test]
    fn the_librarys_refusals_of_a_member_and_a_text() {
        let spec = TreeSpec::new().with(
            "row",
            TreeRule {
                container: Some(TreeContainer::Object),
                member: Some(TreeKey::Token(0)),
                ..TreeRule::default()
            },
        );
        let (r, out) = run(
            spec,
            &[ParserEvent::Open(pass(
                "row",
                vec![token(Datum::Bool(true))],
                "",
                None,
            ))],
        );
        let err = r.unwrap_err();
        assert_eq!(err.code, Code::DslTypeError);
        assert_eq!(
            err.message,
            "type_mismatch: key: the name must be a string, not a boolean"
        );
        assert!(out.is_empty());

        let spec = TreeSpec::new().with(
            "t",
            TreeRule {
                text: true,
                ..TreeRule::default()
            },
        );
        let (r, _) = run(spec, &[ParserEvent::Close(pass("t", vec![], "", None))]);
        let err = r.unwrap_err();
        assert_eq!(err.code, Code::DslTypeError);
        assert_eq!(
            err.message,
            "type_mismatch: top: the vector is empty; there is no last item"
        );
    }
}
