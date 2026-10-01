//! The differential suite behind `capability::incremental`.
//!
//! For every grammar in the dev-dependencies and every fixture that
//! grammar reads, the events `SourceMode::Incremental` produces must equal
//! the events `SourceMode::Materialize` produces (number lexemes aside:
//! the walk over a parsed value has none, so they are stripped before the
//! comparison and checked separately to spell the value they accompany).
//! Two outcomes short of identity are accepted, because they are the
//! contract the incremental source documents: a document that repeats a
//! member name streams every occurrence where the walk keeps the survivor,
//! so the two must agree once a `LastWins` router has built the value; and
//! a run the source REFUSES, with `STREAMABILITY_UNKNOWN` (a list wrapped
//! around a root already streamed, a map rewritten after streaming) or
//! `DUPLICATE_MEMBER` (a merged repeated member), after a protocol-valid
//! prefix and before `End`. A fixture the grammar itself refuses is checked
//! as well: the incremental run must fail with the same code at the same
//! position, after a protocol-valid prefix and before `End`, never having
//! streamed a member the grammar did not store. What is never accepted is
//! a completed stream that disagrees with the walk. Two refusals count
//! against a grammar rather than for it: the adapter refuses a container
//! the grammar opens inside a map before the member's key (its events
//! would leave before the key) and a container it streamed that the
//! grammar then never stored in the one around it, and a grammar that
//! builds its values so does it for every document, not for a shape of
//! one, so it is not verified; the nets stand for the documents no
//! fixture foresaw (a YAML `?` key whose value is a mapping,
//! tabnas/transduce#7; a jsonic pair inside a list, dropped when
//! `list.pair` is off). A grammar is
//! listed in `capability::INCREMENTAL` only when no fixture mismatches or
//! is refused that way, and this suite asserts BOTH directions: a listed
//! grammar that mismatches anywhere fails, and an unlisted grammar that
//! never does fails too, so the list can rot in neither. The suite runs the adapter with
//! `ParserSource::unverified`, since the source itself refuses an unlisted
//! grammar before parsing; that refusal is tested here as well.
//!
//! The fixtures are the copies under `tests/fixtures/` (aless's, the
//! OpenAPI YAML, and the shapes this suite exists to see: the YAML root
//! shapes, an empty document, a comment alone, one scalar, a lone `---`,
//! and document streams of every shape, scalars, sequences, mappings,
//! mixed, and empty documents between separators; a YAML merge key;
//! repeated member names, and the repeated fields zon refuses)
//! and four generated documents in the spec's worked-example shape: 2000
//! records as JSON, as JSON Lines, as CSV and as block YAML (the shape
//! whose large form mismatched under the prototype). The generated ones go
//! only to the grammars of their own family; the small ones are offered to
//! every grammar, and a grammar reads whatever parses.

mod support;

use std::fs;
use std::path::PathBuf;

use tabnas::Tabnas;
use tabnas_transduce::{
    capability, replay, CaptureSpec, Code, Datum, Duplicates, Fail, Flow, Limits, Matcher, Metrics,
    OwnedJsonEvent, ParserSource, Prune, Router, Selected, Selector, SourceMode,
};

const RECORDS: usize = 2000;

struct Grammar {
    name: &'static str,
    make: fn() -> Tabnas,
    /// Which generated documents this grammar is offered.
    generated: &'static [&'static str],
}

const GRAMMARS: &[Grammar] = &[
    Grammar {
        name: "json",
        make: tabnas_json::make,
        generated: &["records.json"],
    },
    Grammar {
        name: "jsonl",
        make: tabnas_jsonl::make,
        generated: &["records.jsonl"],
    },
    Grammar {
        name: "jsonic",
        make: tabnas_jsonic::make,
        generated: &["records.json"],
    },
    Grammar {
        name: "jsonc",
        make: tabnas_jsonc::make,
        generated: &["records.json"],
    },
    Grammar {
        name: "json5",
        make: tabnas_json5::make,
        generated: &["records.json"],
    },
    Grammar {
        name: "yaml",
        make: tabnas_yaml::make,
        generated: &["records.yaml"],
    },
    Grammar {
        name: "toml",
        make: tabnas_toml::make,
        generated: &[],
    },
    Grammar {
        name: "ini",
        make: tabnas_ini::make,
        generated: &[],
    },
    Grammar {
        name: "csv",
        make: tabnas_csv::make,
        generated: &["records.csv"],
    },
    Grammar {
        name: "xml",
        make: tabnas_xml::make,
        generated: &[],
    },
    Grammar {
        name: "zon",
        make: tabnas_zon::make,
        generated: &[],
    },
    Grammar {
        name: "markdown",
        make: tabnas_markdown::make,
        generated: &[],
    },
    Grammar {
        name: "feed",
        make: tabnas_feed::make,
        generated: &[],
    },
];

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// One committed fixture's text, by file name.
fn fixture(name: &str) -> String {
    fs::read_to_string(fixtures_dir().join(name)).unwrap_or_else(|e| panic!("{name}: {e}"))
}

/// Every committed fixture, by file name, in a stable order.
fn committed_fixtures() -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = fs::read_dir(fixtures_dir())
        .expect("tests/fixtures exists")
        .map(|entry| {
            let path = entry.expect("a directory entry").path();
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .expect("a UTF-8 file name")
                .to_string();
            let text = fs::read_to_string(&path).expect("a UTF-8 fixture");
            (name, text)
        })
        .collect();
    out.sort();
    out
}

fn generated(name: &str) -> String {
    match name {
        "records.json" => support::records_json(RECORDS),
        "records.jsonl" => support::records_jsonl(RECORDS),
        "records.csv" => support::records_csv(RECORDS),
        "records.yaml" => support::records_yaml(RECORDS),
        other => panic!("no generator for {other}"),
    }
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

fn run(
    make: fn() -> Tabnas,
    text: &str,
    mode: SourceMode,
) -> (Result<Flow, Fail>, Vec<OwnedJsonEvent>) {
    ParserSource::new(make(), text)
        .unverified()
        .mode(mode)
        .run_owned(Vec::new())
}

/// What one (grammar, fixture) pair did.
enum Outcome {
    /// The grammar refused the document, and so did the incremental run,
    /// with the grammar's failure or a documented refusal.
    NotRead {
        walk: Code,
        incremental: Code,
    },
    Match {
        events: usize,
        lexemes: usize,
    },
    /// The streams differ only where the document repeats a member name:
    /// a `LastWins` router builds the same value from both.
    MatchLastWins {
        events: usize,
    },
    /// The incremental run failed with a documented code after a
    /// protocol-valid prefix and before `End`.
    Refused {
        code: Code,
        events: usize,
    },
    /// The adapter refused the run because of how the grammar builds its
    /// values: a container opened inside a map before the member's key, or
    /// a container streamed as it was built and then never stored in the
    /// one around it. A protocol-valid prefix and no wrong stream, but not
    /// a shape of the document: a grammar that builds so does it for every
    /// document built that way, so one that does it on a fixture is not
    /// verified. The nets are for the documents no fixture foresaw.
    Unfollowed {
        events: usize,
    },
    Mismatch(String),
}

/// Whether a documented refusal is one of the adapter's nets over how a
/// grammar builds its values, by the sentence both carry.
fn unfollowed(fail: &Fail) -> bool {
    fail.code == Code::StreamabilityUnknown
        && fail
            .message
            .contains("the incremental source cannot follow a grammar that builds")
}

fn compare(grammar: &Grammar, text: &str) -> Outcome {
    let (whole, materialized) = run(grammar.make, text, SourceMode::Materialize);
    if let Err(fail) = whole {
        // The grammar refuses the document: the incremental run must fail
        // too, after a protocol-valid prefix and before End, either with
        // the grammar's own failure, code and position alike (zon's
        // repeated fields: the guard fails the pair's close before the
        // assignment, and the map's earlier value must not be streamed as
        // the repeated member), or with a documented refusal, since the
        // adapter refuses a shape as it meets it and cancels the parse,
        // which can come before the position where the grammar itself
        // would have failed (yaml reading merged.json5 sees a second root
        // before the syntax error). Never a completed stream.
        let (result, incremental) = run(
            grammar.make,
            text,
            SourceMode::Incremental {
                prune: Prune::Never,
            },
        );
        return match result {
            Err(inc)
                if well_formed(&incremental)
                    && !incremental.contains(&OwnedJsonEvent::End)
                    && unfollowed(&inc) =>
            {
                Outcome::Unfollowed {
                    events: incremental.len(),
                }
            }
            Err(inc)
                if well_formed(&incremental)
                    && !incremental.contains(&OwnedJsonEvent::End)
                    && ((inc.code == fail.code
                        && (inc.row, inc.column) == (fail.row, fail.column))
                        || matches!(
                            inc.code,
                            Code::StreamabilityUnknown | Code::DuplicateMember
                        )) =>
            {
                Outcome::NotRead {
                    walk: fail.code,
                    incremental: inc.code,
                }
            }
            Err(inc) => Outcome::Mismatch(format!(
                "the walk failed with {fail}; the incremental run failed with {inc} after {} \
                 events (protocol-valid prefix: {})",
                incremental.len(),
                well_formed(&incremental)
            )),
            Ok(flow) => Outcome::Mismatch(format!(
                "the walk failed with {fail}; the incremental run completed with Ok({flow:?}) \
                 after {} events",
                incremental.len()
            )),
        };
    }
    let (result, incremental) = run(
        grammar.make,
        text,
        SourceMode::Incremental {
            prune: Prune::Never,
        },
    );
    if let Err(fail) = result {
        let documented = matches!(
            fail.code,
            Code::StreamabilityUnknown | Code::DuplicateMember
        );
        if documented && well_formed(&incremental) && !incremental.contains(&OwnedJsonEvent::End) {
            if unfollowed(&fail) {
                return Outcome::Unfollowed {
                    events: incremental.len(),
                };
            }
            return Outcome::Refused {
                code: fail.code,
                events: incremental.len(),
            };
        }
        return Outcome::Mismatch(format!(
            "the incremental run failed with {fail} after {} events",
            incremental.len()
        ));
    }
    let mut lexemes = 0;
    for ev in &incremental {
        if let OwnedJsonEvent::Number {
            value,
            lexeme: Some(l),
        } = ev
        {
            lexemes += 1;
            if l.parse::<f64>().ok() != Some(*value) {
                return Outcome::Mismatch(format!("lexeme {l:?} does not spell the value {value}"));
            }
        }
    }
    let stripped = without_lexemes(&incremental);
    if stripped == materialized {
        return Outcome::Match {
            events: materialized.len(),
            lexemes,
        };
    }
    if well_formed(&stripped)
        && root_value(&stripped, Duplicates::LastWins).ok()
            == root_value(&materialized, Duplicates::Reject).ok()
    {
        return Outcome::MatchLastWins {
            events: stripped.len(),
        };
    }
    let first = stripped
        .iter()
        .zip(materialized.iter())
        .position(|(a, b)| a != b)
        .unwrap_or(stripped.len().min(materialized.len()));
    let show = |events: &[OwnedJsonEvent]| -> String {
        let from = first.saturating_sub(3);
        let to = (first + 4).min(events.len());
        events[from..to]
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join(" ")
    };
    Outcome::Mismatch(format!(
        "incremental produced {} events, the walk {}; first difference at event {first}: incremental [{}] vs walk [{}]",
        stripped.len(),
        materialized.len(),
        show(&stripped),
        show(&materialized)
    ))
}

/// Run one grammar over every fixture it reads and check the capability
/// list agrees with what happened.
fn verify(name: &str) {
    let grammar = GRAMMARS
        .iter()
        .find(|g| g.name == name)
        .expect("a known grammar");
    let mut fixtures = committed_fixtures();
    for g in grammar.generated {
        fixtures.push((format!("generated {g} ({RECORDS} records)"), generated(g)));
    }
    let total = fixtures.len();
    let mut read = 0;
    let mut mismatches: Vec<String> = Vec::new();
    let mut unfollowed: Vec<String> = Vec::new();
    for (i, (fixture, text)) in fixtures.iter().enumerate() {
        let started = std::time::Instant::now();
        let outcome = compare(grammar, text);
        let verdict = match &outcome {
            Outcome::NotRead { walk, incremental } => {
                format!("not read ({walk}; the incremental run failed with {incremental})")
            }
            Outcome::Match { events, lexemes } => {
                read += 1;
                format!("MATCH ({events} events, {lexemes} lexemes)")
            }
            Outcome::MatchLastWins { events } => {
                read += 1;
                format!(
                    "MATCH after LastWins ({events} events; the document repeats a member name)"
                )
            }
            Outcome::Refused { code, events } => {
                read += 1;
                format!("REFUSED with {code} after {events} events")
            }
            Outcome::Unfollowed { events } => {
                read += 1;
                unfollowed.push(format!("{fixture}: after {events} events"));
                format!("UNFOLLOWED: refused for how the grammar builds, after {events} events")
            }
            Outcome::Mismatch(why) => {
                read += 1;
                mismatches.push(format!("{fixture}: {why}"));
                format!("MISMATCH: {why}")
            }
        };
        println!(
            "incremental {name}: {} of {total} ({}%) {fixture}: {verdict} [{:.2}s]",
            i + 1,
            (i + 1) * 100 / total,
            started.elapsed().as_secs_f64()
        );
    }
    assert!(read > 0, "grammar {name} read none of the fixtures");
    // Verified means: never a wrong stream, and never a member the adapter
    // could not follow. The second is the grammar's to fix (announce the
    // key before the value's rule opens, as yaml's pairs do), not the
    // list's to excuse: a grammar refused on its own documents would be
    // attempted and refused on every one of them.
    let verified = mismatches.is_empty() && unfollowed.is_empty();
    let listed = capability::incremental(name);
    let mut problems = mismatches.clone();
    problems.extend(unfollowed.iter().map(|f| {
        format!("{f}: refused for how the grammar builds its values (a container opened in a map before its member's key, or streamed and never stored)")
    }));
    assert_eq!(
        listed,
        verified,
        "capability::incremental({name:?}) is {listed}, but the grammar {} over {read} fixtures it reads{}{}",
        if verified {
            "never mismatched; add it to capability::INCREMENTAL"
        } else if mismatches.is_empty() {
            "builds its values in a way the adapter refuses (a container opened in a map before \
             its key, or streamed and never stored); remove it from capability::INCREMENTAL, or \
             fix the grammar"
        } else {
            "mismatched; remove it from capability::INCREMENTAL, or fix the adapter"
        },
        if problems.is_empty() { "" } else { ":\n  " },
        problems.join("\n  ")
    );
}

macro_rules! grammar_tests {
    ($($test:ident => $name:literal),* $(,)?) => {
        $(
            #[test]
            fn $test() {
                verify($name);
            }
        )*
    };
}

grammar_tests! {
    json_streams_incrementally_where_the_list_says_so => "json",
    jsonl_streams_incrementally_where_the_list_says_so => "jsonl",
    jsonic_streams_incrementally_where_the_list_says_so => "jsonic",
    jsonc_streams_incrementally_where_the_list_says_so => "jsonc",
    json5_streams_incrementally_where_the_list_says_so => "json5",
    yaml_streams_incrementally_where_the_list_says_so => "yaml",
    toml_streams_incrementally_where_the_list_says_so => "toml",
    ini_streams_incrementally_where_the_list_says_so => "ini",
    csv_streams_incrementally_where_the_list_says_so => "csv",
    xml_streams_incrementally_where_the_list_says_so => "xml",
    zon_streams_incrementally_where_the_list_says_so => "zon",
    markdown_streams_incrementally_where_the_list_says_so => "markdown",
    feed_streams_incrementally_where_the_list_says_so => "feed",
}

/// The grammar crates `Cargo.toml` takes by path (`tabnas-<name>`), read
/// from the manifest at test time so a grammar added there without a
/// `GRAMMARS` row fails here. A grammar is a crate that depends on the
/// engine, which its own manifest says: `tabnas-support`, the fixture
/// runner, is a path crate too and is not one.
fn manifest_grammars() -> Vec<String> {
    let mut names: Vec<String> = include_str!("../Cargo.toml")
        .lines()
        .filter_map(|line| {
            let (name, rest) = line.split_once('=')?;
            let name = name.trim().strip_prefix("tabnas-")?;
            let (_, path) = rest.split_once("path = \"")?;
            let (path, _) = path.split_once('"')?;
            let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join(path)
                .join("Cargo.toml");
            let text = fs::read_to_string(&manifest)
                .unwrap_or_else(|e| panic!("{}: {e}", manifest.display()));
            text.contains("tabnas-parser").then(|| name.to_string())
        })
        .collect();
    names.sort();
    names
}

#[test]
fn every_grammar_in_the_dev_dependencies_is_verified_here() {
    let mut suite: Vec<String> = GRAMMARS.iter().map(|g| g.name.to_string()).collect();
    suite.sort();
    assert_eq!(
        manifest_grammars(),
        suite,
        "the grammar crates Cargo.toml names and the GRAMMARS this suite runs differ"
    );
    // The list can only name grammars this suite runs.
    for name in capability::INCREMENTAL {
        assert!(
            GRAMMARS.iter().any(|g| g.name == *name),
            "capability::INCREMENTAL names {name:?}, which this suite does not run"
        );
    }
}

/// Whether a recording is a protocol-valid stream, or a prefix of one:
/// every event is accepted by a matcher, which validates the sequence.
fn well_formed(events: &[OwnedJsonEvent]) -> bool {
    let mut m = Matcher::new(&[]);
    events.iter().all(|e| m.event(e.as_event()).is_ok())
}

/// The document's root value as a router materializes it from a recording
/// under `policy`, which is how every consumer of the stream sees repeated
/// members.
fn root_value(events: &[OwnedJsonEvent], policy: Duplicates) -> Result<Datum, Fail> {
    let mut r = Router::new(
        vec![CaptureSpec::materialize("root", Selector::root())],
        &Limits::default(),
        policy,
        Metrics::new(),
        Vec::<Selected>::new(),
    )?;
    replay(events, &mut r)?;
    Ok(r.into_inner().remove(0).value.unwrap_or(Datum::Null))
}

/// A grammar by name, and a text for it.
type Case = (&'static str, fn() -> Tabnas, &'static str);

fn incremental(make: fn() -> Tabnas, text: &str) -> (Result<Flow, Fail>, Vec<OwnedJsonEvent>) {
    run(
        make,
        text,
        SourceMode::Incremental {
            prune: Prune::Never,
        },
    )
}

/// A repeated member name is the one documented place the incremental
/// stream and the walk differ: the stream carries every occurrence, the
/// walk only the engine's survivor. A router's `LastWins` makes them agree
/// for every grammar that replaces the earlier value; `Reject` sees the
/// duplicate the walk would have hidden.
#[test]
fn a_repeated_scalar_member_streams_every_occurrence_and_last_wins_agrees_with_the_walk() {
    let cases: [Case; 6] = [
        ("json", tabnas_json::make, r#"{"a":1,"a":2,"b":3}"#),
        (
            "jsonl",
            tabnas_jsonl::make,
            "{\"a\":1,\"a\":2}\n{\"a\":3,\"a\":4,\"b\":5}\n",
        ),
        ("yaml", tabnas_yaml::make, "a: 1\na: 2\nb: 3\n"),
        ("json5", tabnas_json5::make, "{a:1,a:2,b:3}"),
        ("jsonc", tabnas_jsonc::make, r#"{"a":1,"a":2,"b":3}"#),
        ("jsonic", tabnas_jsonic::make, "a:1,a:2,b:3"),
    ];
    for (name, make, text) in cases {
        let (result, events) = incremental(make, text);
        assert_eq!(result.unwrap(), Flow::Continue, "{name}");
        assert!(well_formed(&events), "{name}: {events:?}");
        let keys = events
            .iter()
            .filter(|e| matches!(e, OwnedJsonEvent::Key(k) if &**k == "a"))
            .count();
        assert!(keys >= 2, "{name}: both occurrences of a are in the stream");
        let (_, walked) = run(make, text, SourceMode::Materialize);
        assert_eq!(
            root_value(&without_lexemes(&events), Duplicates::LastWins).unwrap(),
            root_value(&walked, Duplicates::Reject).unwrap(),
            "{name}: last wins is the engine's value"
        );
        assert_eq!(
            root_value(&events, Duplicates::Reject).unwrap_err().code,
            Code::DuplicateMember,
            "{name}"
        );
    }
}

/// The grammars with `map.extend` off (json, jsonl, jsonc) replace the
/// earlier value whatever the shapes, so both are streamed and last wins.
#[test]
fn a_repeated_member_a_grammar_replaces_streams_both_values_whatever_their_shapes() {
    let grammars: [Case; 3] = [
        ("json", tabnas_json::make, ""),
        ("jsonl", tabnas_jsonl::make, ""),
        ("jsonc", tabnas_jsonc::make, ""),
    ];
    for (name, make, _) in grammars {
        for text in [
            r#"{"a":{"x":1},"a":{"y":2}}"#,
            r#"{"a":[1],"a":2}"#,
            r#"{"a":1,"a":{"y":2}}"#,
            r#"{"a":1,"a":1,"a":2}"#,
            r#"{"a":{"x":1},"a":{"x":1}}"#,
        ] {
            let (result, events) = incremental(make, text);
            assert_eq!(result.unwrap(), Flow::Continue, "{name} {text}");
            assert!(well_formed(&events), "{name} {text}: {events:?}");
            let (_, walked) = run(make, text, SourceMode::Materialize);
            assert_eq!(
                root_value(&without_lexemes(&events), Duplicates::LastWins).unwrap(),
                root_value(&walked, Duplicates::Reject).unwrap(),
                "{name} {text}"
            );
        }
    }
}

/// jsonic's `map.extend`, which yaml and json5 inherit, merges the two
/// containers of a repeated name. The first was streamed before the
/// second arrived, so the merged member cannot be, and the run fails
/// rather than emit a stream that disagrees with the walk.
#[test]
fn a_repeated_container_member_the_grammar_merges_fails_with_duplicate_member() {
    let cases: [Case; 4] = [
        ("yaml flow", tabnas_yaml::make, "a: {x: 1}\na: {y: 2}\n"),
        ("yaml block", tabnas_yaml::make, "a:\n  x: 1\na:\n  y: 2\n"),
        ("json5", tabnas_json5::make, "{a:{x:1},a:{y:2}}"),
        ("jsonic", tabnas_jsonic::make, "a:{x:1},a:{y:2}"),
    ];
    for (name, make, text) in cases {
        let (result, events) = incremental(make, text);
        let err = result
            .map(|flow| panic!("{name}: Ok({flow:?}) after {events:?}"))
            .unwrap_err();
        assert_eq!(err.code, Code::DuplicateMember, "{name}: {err}");
        assert!(
            err.message.contains("Materialize"),
            "{name}: the message says what to run instead"
        );
        assert!(well_formed(&events), "{name}: what left is a valid prefix");
        assert!(!events.contains(&OwnedJsonEvent::End), "{name}");
        let (walk, walked) = run(make, text, SourceMode::Materialize);
        walk.unwrap();
        assert_eq!(
            root_value(&walked, Duplicates::Reject).unwrap().to_string(),
            r#"{"a":{"x":1,"y":2}}"#,
            "{name}: the walk has the merged member"
        );
    }
}

/// zon refuses a repeated field itself, from the pair rule's close and
/// before jsonic's assignment, so when the engine reports that close the
/// map still holds the FIRST value. The incremental run fails exactly as
/// the walk does (`INPUT_INVALID`, the grammar's code, the same position),
/// after a protocol-valid prefix and before `End`, and the member the
/// grammar never stored is not streamed: the prefix ends with the repeated
/// `Key`, or with the second value's own container, which the adapter
/// streamed as the grammar built it. A router consumer therefore sees the
/// grammar's failure, not a protocol one.
#[test]
fn every_repeated_field_zon_fixture_fails_as_the_walk_does_after_a_protocol_valid_prefix() {
    use OwnedJsonEvent::{ArrayEnd, ArrayStart, End, Key, ObjectEnd, ObjectStart};
    let key = |k: &str| Key(k.into());
    let num = |n: f64| OwnedJsonEvent::Number {
        value: n,
        lexeme: None,
    };
    let cases: [(&str, Vec<OwnedJsonEvent>); 7] = [
        (
            "repeated-scalar.zon",
            vec![ObjectStart, key("a"), num(1.0), key("a")],
        ),
        (
            "repeated-scalar-then-struct.zon",
            vec![
                ObjectStart,
                key("a"),
                num(1.0),
                key("a"),
                ObjectStart,
                key("y"),
                num(2.0),
                ObjectEnd,
            ],
        ),
        (
            "repeated-struct-then-scalar.zon",
            vec![
                ObjectStart,
                key("a"),
                ObjectStart,
                key("x"),
                num(1.0),
                ObjectEnd,
                key("a"),
            ],
        ),
        (
            "repeated-structs.zon",
            vec![
                ObjectStart,
                key("a"),
                ObjectStart,
                key("x"),
                num(1.0),
                ObjectEnd,
                key("a"),
                ObjectStart,
                key("y"),
                num(2.0),
                ObjectEnd,
            ],
        ),
        (
            "repeated-scalar-then-tuple.zon",
            vec![
                ObjectStart,
                key("a"),
                num(1.0),
                key("a"),
                ArrayStart,
                num(7.0),
                num(8.0),
                ArrayEnd,
            ],
        ),
        (
            "repeated-after-another.zon",
            vec![
                ObjectStart,
                key("a"),
                num(1.0),
                key("b"),
                num(5.0),
                key("a"),
                ObjectStart,
                key("y"),
                num(2.0),
                ObjectEnd,
            ],
        ),
        (
            "repeated-nested.zon",
            vec![
                ObjectStart,
                key("o"),
                ObjectStart,
                key("a"),
                num(1.0),
                key("a"),
                ObjectStart,
                key("y"),
                num(2.0),
                ObjectEnd,
            ],
        ),
    ];
    for (name, prefix) in cases {
        let text = fixture(name);
        let (whole, walked) = run(tabnas_zon::make, &text, SourceMode::Materialize);
        let expected = whole.unwrap_err();
        assert_eq!(expected.code, Code::InputInvalid, "{name}: {expected}");
        assert!(
            expected.message.contains("zon_dup_field"),
            "{name}: the grammar's own guard: {expected}"
        );
        assert!(walked.is_empty(), "{name}: the walk emits nothing");

        let (result, events) = incremental(tabnas_zon::make, &text);
        let err = result
            .map(|flow| panic!("{name}: Ok({flow:?}) after {events:?}"))
            .unwrap_err();
        assert_eq!(err.code, expected.code, "{name}: {err}");
        assert_eq!(err.message, expected.message, "{name}");
        assert_eq!(
            (err.row, err.column),
            (expected.row, expected.column),
            "{name}: the grammar's position"
        );
        assert!(well_formed(&events), "{name}: {events:?}");
        assert!(!events.contains(&End), "{name}");
        assert_eq!(
            without_lexemes(&events),
            prefix,
            "{name}: the member the grammar never stored is not streamed"
        );

        let router = Router::new(
            vec![CaptureSpec::materialize("root", Selector::root())],
            &Limits::default(),
            Duplicates::LastWins,
            Metrics::new(),
            Vec::<Selected>::new(),
        )
        .unwrap();
        let (result, router) = ParserSource::new(tabnas_zon::make(), &text)
            .unverified()
            .mode(SourceMode::Incremental {
                prune: Prune::Never,
            })
            .run_owned(router);
        let err = result.unwrap_err();
        assert_eq!(
            err.code,
            Code::InputInvalid,
            "{name}: a router consumer sees the grammar's failure, not a protocol one: {err}"
        );
        assert!(router.into_inner().is_empty(), "{name}: nothing delivered");
    }
}

/// YAML resolves a `<<` merge key when the mapping closes, removing the
/// member and appending the merged ones: the members the adapter streamed
/// are no longer the map's, so the run is refused rather than completed.
#[test]
fn a_map_the_grammar_rewrites_after_streaming_is_refused() {
    let text = "base: &b\n  x: 1\nd:\n  <<: *b\n  y: 2\n";
    let (walk, walked) = run(tabnas_yaml::make, text, SourceMode::Materialize);
    walk.unwrap();
    assert_eq!(
        root_value(&walked, Duplicates::Reject).unwrap().to_string(),
        r#"{"base":{"x":1},"d":{"y":2,"x":1}}"#
    );
    let (result, events) = incremental(tabnas_yaml::make, text);
    let err = result.unwrap_err();
    assert_eq!(err.code, Code::StreamabilityUnknown, "{err}");
    assert!(err.message.contains("merge key"), "{err}");
    assert!(well_formed(&events));
    assert!(!events.contains(&OwnedJsonEvent::End));

    // An alias without a merge key copies the value and streams as the walk.
    let text = "a: &r {x: 1}\nb: *r\n";
    let (result, events) = incremental(tabnas_yaml::make, text);
    assert_eq!(result.unwrap(), Flow::Continue);
    let (_, walked) = run(tabnas_yaml::make, text, SourceMode::Materialize);
    assert_eq!(without_lexemes(&events), walked);
}

/// jsonic parses a pair inside a list and drops it when `list.pair` is
/// off (the default): `[a:{b:1}]` reads as `[]`. The pair's value is built
/// in a rule of its own, so the incremental source streamed it before the
/// grammar dropped it, and the stream contradicted the walk. The adapter
/// now refuses a container it streamed that the grammar never stored,
/// where the grammar's next step shows it: when the frame around it ends
/// here, with `STREAMABILITY_UNKNOWN` after a protocol-valid prefix and
/// before `End`; a scalar pair in a list builds no container and streams
/// as the walk does.
#[test]
fn a_container_the_grammar_streamed_and_never_stored_is_refused() {
    let text = "[a:{b:1}]";
    let (walk, walked) = run(tabnas_jsonic::make, text, SourceMode::Materialize);
    walk.unwrap();
    assert_eq!(
        root_value(&walked, Duplicates::Reject).unwrap().to_string(),
        "[]"
    );
    let (result, events) = incremental(tabnas_jsonic::make, text);
    let err = result
        .map(|flow| panic!("Ok({flow:?}) after {events:?}"))
        .unwrap_err();
    assert_eq!(err.code, Code::StreamabilityUnknown, "{err}");
    assert!(err.message.contains("never stored it"), "{err}");
    assert!(well_formed(&events), "{events:?}");
    assert!(!events.contains(&OwnedJsonEvent::End));
    assert_eq!(
        without_lexemes(&events),
        vec![
            OwnedJsonEvent::ArrayStart,
            OwnedJsonEvent::ObjectStart,
            OwnedJsonEvent::Key("b".into()),
            OwnedJsonEvent::Number {
                value: 1.0,
                lexeme: None,
            },
            OwnedJsonEvent::ObjectEnd,
        ]
    );
    // Dropped before another entry lands, and before another container
    // opens: refused there, with the same code.
    for text in ["[a:{b:1},2]", "[a:{b:1},c:{d:2}]"] {
        let (result, events) = incremental(tabnas_jsonic::make, text);
        let err = result
            .map(|flow| panic!("{text}: Ok({flow:?}) after {events:?}"))
            .unwrap_err();
        assert_eq!(err.code, Code::StreamabilityUnknown, "{text}: {err}");
        assert!(err.message.contains("never stored it"), "{text}: {err}");
        assert!(well_formed(&events), "{text}: {events:?}");
        assert!(!events.contains(&OwnedJsonEvent::End), "{text}");
    }
    for text in ["[a:1]", "[1,a:1,2]"] {
        let (result, events) = incremental(tabnas_jsonic::make, text);
        assert_eq!(result.unwrap(), Flow::Continue, "{text}");
        let (_, walked) = run(tabnas_jsonic::make, text, SourceMode::Materialize);
        assert_eq!(without_lexemes(&events), walked, "{text}");
    }
}

/// A YAML key that is itself a mapping (an explicit `?` key, YAML Test
/// Suite V9D5, Spec Example 8.19): the grammar stringifies the key and
/// stores the member when the pair closes, after the value's mapping was
/// built in a rule of its own, so the incremental source saw the value's
/// events leave before the key (tabnas/transduce#7). The adapter now
/// refuses that when the value opens, with `STREAMABILITY_UNKNOWN` after a
/// protocol-valid prefix and before `End`, and a host falls back to the
/// walk, which is right. yaml stays listed because no fixture builds a
/// member so; announcing the key before the value's rule opens is the
/// grammar's follow-up, after which this document streams as the walk.
#[test]
fn a_yaml_key_that_is_a_mapping_is_refused_before_its_value_streams() {
    let text = "- sun: yellow\n- ? earth: blue\n  : moon: white\n";
    let (walk, walked) = run(tabnas_yaml::make, text, SourceMode::Materialize);
    walk.unwrap();
    assert_eq!(
        root_value(&walked, Duplicates::Reject).unwrap().to_string(),
        r#"[{"sun":"yellow"},{"earth: blue":{"moon":"white"}}]"#
    );
    let (result, events) = incremental(tabnas_yaml::make, text);
    let err = result
        .map(|flow| panic!("Ok({flow:?}) after {events:?}"))
        .unwrap_err();
    assert_eq!(err.code, Code::StreamabilityUnknown, "{err}");
    assert!(
        err.message.contains("before announcing the member's key"),
        "{err}"
    );
    assert!(well_formed(&events), "{events:?}");
    assert!(!events.contains(&OwnedJsonEvent::End));
    assert_eq!(
        without_lexemes(&events),
        vec![
            OwnedJsonEvent::ArrayStart,
            OwnedJsonEvent::ObjectStart,
            OwnedJsonEvent::Key("sun".into()),
            OwnedJsonEvent::String("yellow".into()),
            OwnedJsonEvent::ObjectEnd,
            OwnedJsonEvent::ObjectStart,
        ],
        "the first member left whole, the second's map opened, then nothing"
    );
    // An explicit key whose value is a scalar names the member before the
    // value lands, and streams as the walk.
    let text = "? earth\n: moon\n";
    let (result, events) = incremental(tabnas_yaml::make, text);
    assert_eq!(result.unwrap(), Flow::Continue);
    let (_, walked) = run(tabnas_yaml::make, text, SourceMode::Materialize);
    assert_eq!(without_lexemes(&events), walked);
}

/// A grammar that builds a member's value in a rule of its own and names
/// the member only when the pair closes, never announcing the key (the
/// shape tabnas/transduce#7 found behind a YAML key that is a mapping):
/// `[1]` parses to `{"k":[1]}`. The value's events would leave before the
/// key, which no tree's events do, so the incremental run is refused with
/// `STREAMABILITY_UNKNOWN` when the value opens, after a protocol-valid
/// prefix and before `End`, never a completed stream the walk contradicts.
#[test]
fn a_container_opened_in_a_map_before_its_key_is_refused() {
    fn late_key() -> Tabnas {
        use std::cell::RefCell;
        use std::rc::Rc;
        use std::sync::Arc;
        use tabnas::{AltSpec, RuleSpec, Value, TIN_CS, TIN_NR, TIN_OS, TIN_ZZ};

        let mut parser = Tabnas::new();
        let mut val = RuleSpec::new("val");
        val.add_bo(|rule, _| {
            rule.node = Rc::new(RefCell::new(Value::object(Default::default())));
        });
        val.open.push(AltSpec {
            s: vec![vec![TIN_OS]],
            p: Some("list".into()),
            ..Default::default()
        });
        let mut pair = AltSpec {
            s: vec![vec![TIN_ZZ]],
            ..Default::default()
        };
        pair.add_action(|rule, _| {
            let child = rule.child_node.clone();
            if let Value::Object(members) = &mut *rule.node.borrow_mut() {
                Arc::make_mut(members).insert("k".into(), child);
            }
        });
        val.close.push(pair);
        parser.rule(val);

        let mut list = RuleSpec::new("list");
        list.add_bo(|rule, _| {
            rule.node = Rc::new(RefCell::new(Value::array(Vec::new())));
        });
        let mut item = AltSpec {
            s: vec![vec![TIN_NR]],
            ..Default::default()
        };
        item.add_action(|rule, _| {
            let value = rule
                .o0()
                .map(|token| token.val.clone())
                .unwrap_or(Value::Null);
            if let Value::Array(items) = &mut *rule.node.borrow_mut() {
                Arc::make_mut(items).push(value);
            }
        });
        list.open.push(item);
        list.close.push(AltSpec {
            s: vec![vec![TIN_CS]],
            ..Default::default()
        });
        parser.rule(list);
        parser
    }

    let (walk, walked) = run(late_key, "[1]", SourceMode::Materialize);
    walk.unwrap();
    assert_eq!(
        root_value(&walked, Duplicates::Reject).unwrap().to_string(),
        r#"{"k":[1]}"#
    );
    let (result, events) = incremental(late_key, "[1]");
    let err = result
        .map(|flow| panic!("Ok({flow:?}) after {events:?}"))
        .unwrap_err();
    assert_eq!(err.code, Code::StreamabilityUnknown, "{err}");
    assert!(
        err.message.contains("before announcing the member's key"),
        "{err}"
    );
    assert!(err.message.contains("Materialize"), "{err}");
    assert_eq!(events, vec![OwnedJsonEvent::ObjectStart]);
    assert!(well_formed(&events));
}

/// The YAML root shapes, each a fixture file. A single document (empty, a
/// comment alone, one scalar, a lone `---`) streams as the walk does: what
/// no rule event shows is `null`, which the source walks. A stream of
/// several documents is wrapped in a list by the grammar when the source
/// ends, whatever the documents' shapes, so the incremental run either
/// streams exactly the walk (`1` then `---` then `2`: no scalar left early,
/// so the finished value is walked whole) or is refused with
/// `STREAMABILITY_UNKNOWN` naming the shape, after a protocol-valid prefix
/// and before `End`: at the second document's container when there is one,
/// and otherwise when the root rule closes over a value that is not the one
/// streamed. Never a completed stream the walk contradicts.
#[test]
fn every_yaml_root_shape_fixture_streams_as_the_walk_or_is_refused_before_end() {
    for name in ["empty.yaml", "comment.yaml", "scalar.yaml", "marker.yaml"] {
        let text = fixture(name);
        let (whole, walked) = run(tabnas_yaml::make, &text, SourceMode::Materialize);
        whole.unwrap();
        let (result, events) = incremental(tabnas_yaml::make, &text);
        assert_eq!(result.unwrap(), Flow::Continue, "{name}");
        assert_eq!(without_lexemes(&events), walked, "{name}");
    }

    enum Expect {
        /// The incremental run completes with the walk's events.
        Walk,
        /// Refused after exactly these events (lexemes aside).
        Refused(Vec<OwnedJsonEvent>),
    }
    let one = |k: &str, n: f64| {
        vec![
            OwnedJsonEvent::ObjectStart,
            OwnedJsonEvent::Key(k.into()),
            OwnedJsonEvent::Number {
                value: n,
                lexeme: None,
            },
            OwnedJsonEvent::ObjectEnd,
        ]
    };
    let streams = [
        ("stream.yaml", Expect::Refused(one("a", 1.0))),
        ("stream-scalars.yaml", Expect::Walk),
        (
            "stream-sequences.yaml",
            Expect::Refused(vec![
                OwnedJsonEvent::ArrayStart,
                OwnedJsonEvent::Number {
                    value: 1.0,
                    lexeme: None,
                },
                OwnedJsonEvent::ArrayEnd,
            ]),
        ),
        ("stream-map-scalar.yaml", Expect::Refused(one("a", 1.0))),
        ("stream-scalar-map.yaml", Expect::Refused(one("b", 2.0))),
        ("stream-empty-map.yaml", Expect::Refused(one("b", 2.0))),
        ("stream-map-empty.yaml", Expect::Refused(one("a", 1.0))),
    ];
    for (name, expect) in streams {
        let text = fixture(name);
        let (whole, walked) = run(tabnas_yaml::make, &text, SourceMode::Materialize);
        whole.unwrap();
        assert_eq!(
            walked.first(),
            Some(&OwnedJsonEvent::ArrayStart),
            "{name}: the walk sees the documents wrapped in a list"
        );
        let (result, events) = incremental(tabnas_yaml::make, &text);
        match expect {
            Expect::Walk => {
                assert_eq!(result.unwrap(), Flow::Continue, "{name}");
                assert_eq!(without_lexemes(&events), walked, "{name}");
            }
            Expect::Refused(prefix) => {
                let err = result
                    .map(|flow| panic!("{name}: Ok({flow:?}) after {events:?}"))
                    .unwrap_err();
                assert_eq!(err.code, Code::StreamabilityUnknown, "{name}: {err}");
                assert!(err.message.contains("several documents"), "{name}: {err}");
                assert!(err.message.contains("Materialize"), "{name}: {err}");
                assert!(well_formed(&events), "{name}: {events:?}");
                assert!(!events.contains(&OwnedJsonEvent::End), "{name}");
                assert_eq!(
                    without_lexemes(&events),
                    prefix,
                    "{name}: the first document left whole, then nothing"
                );
            }
        }
    }
}

/// markdown is listed although it builds nodes imperatively: they land
/// whole and are walked at their insertion, so the fixtures alone do not
/// say much and richer documents are checked here.
#[test]
fn markdown_documents_stream_as_the_walk() {
    for text in [
        "# Title\n\nSome *emphasis* and a [link](http://x).\n\n- one\n- two\n  - nested\n\n```rust\nfn x() {}\n```\n\n| a | b |\n|---|---|\n| 1 | 2 |\n\n> quote\n\n1. first\n2. second\n",
        "para one\npara one continued\n\npara two\n",
        "",
        "***\n\n# A\n## B\n### C\n",
        "text with `code` and **bold** and ![img](u) end\n",
        "- a\n\n  b\n- c\n\n> - d\n> - e\n",
    ] {
        let (whole, walked) = run(tabnas_markdown::make, text, SourceMode::Materialize);
        whole.unwrap();
        let (result, events) = incremental(tabnas_markdown::make, text);
        assert_eq!(result.unwrap(), Flow::Continue, "{text:?}");
        assert_eq!(without_lexemes(&events), walked, "{text:?}");
    }
}

/// The source consults the list by the grammar's name: an unlisted grammar
/// in `Incremental` mode is refused before the parse, with nothing
/// emitted, and a listed one runs; the suite above is what may bypass it.
#[test]
fn an_unlisted_grammar_in_incremental_mode_is_refused_before_it_emits_anything() {
    let text = fs::read_to_string(fixtures_dir().join("sample.csv")).unwrap();
    let mut refused = 0;
    for grammar in GRAMMARS {
        let (result, events) = ParserSource::new((grammar.make)(), &text)
            .grammar(grammar.name)
            .mode(SourceMode::Incremental {
                prune: Prune::Never,
            })
            .run_owned(Vec::<OwnedJsonEvent>::new());
        if capability::incremental(grammar.name) {
            // Listed: the gate is open; whether csv text parses is the
            // grammar's business.
            assert_ne!(
                result.as_ref().err().map(|e| e.code),
                Some(Code::StreamabilityUnknown),
                "{}: {result:?}",
                grammar.name
            );
        } else {
            let err = result.unwrap_err();
            assert_eq!(err.code, Code::StreamabilityUnknown, "{}", grammar.name);
            assert!(err.message.contains(grammar.name), "{err}");
            assert!(
                events.is_empty(),
                "{}: nothing left the source",
                grammar.name
            );
            refused += 1;
        }
    }
    assert_eq!(refused, GRAMMARS.len() - capability::INCREMENTAL.len());
}
