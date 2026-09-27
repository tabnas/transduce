//! The differential suite behind `capability::incremental`.
//!
//! For every grammar in the dev-dependencies and every fixture that
//! grammar reads, the events `SourceMode::Incremental` produces must equal
//! the events `SourceMode::Materialize` produces (number lexemes aside:
//! the walk over a parsed value has none, so they are stripped before the
//! comparison and checked separately to spell the value they accompany).
//! A grammar is listed in `capability::INCREMENTAL` only when every one of
//! its fixtures matches, and this suite asserts BOTH directions: a listed
//! grammar that mismatches anywhere fails, and an unlisted grammar that
//! matches everywhere fails too, so the list can rot in neither.
//!
//! The fixtures are the copies under `tests/fixtures/` (aless's, plus the
//! OpenAPI YAML) and four generated documents in the spec's worked-example
//! shape: 2000 records as JSON, as JSON Lines, as CSV and as block YAML
//! (the shape whose large form mismatched under the prototype). The generated
//! ones go only to the grammars of their own family; the small ones are
//! offered to every grammar, and a grammar reads whatever parses.

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
        .mode(mode)
        .run_owned(Vec::new())
}

/// What one (grammar, fixture) pair did.
enum Outcome {
    NotRead(Code),
    Match { events: usize, lexemes: usize },
    Mismatch(String),
}

fn compare(grammar: &Grammar, text: &str) -> Outcome {
    let (whole, materialized) = run(grammar.make, text, SourceMode::Materialize);
    if let Err(fail) = whole {
        return Outcome::NotRead(fail.code);
    }
    let (result, incremental) = run(
        grammar.make,
        text,
        SourceMode::Incremental {
            prune: Prune::Never,
        },
    );
    if let Err(fail) = result {
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
    for (i, (fixture, text)) in fixtures.iter().enumerate() {
        let started = std::time::Instant::now();
        let outcome = compare(grammar, text);
        let verdict = match &outcome {
            Outcome::NotRead(code) => format!("not read ({code})"),
            Outcome::Match { events, lexemes } => {
                read += 1;
                format!("MATCH ({events} events, {lexemes} lexemes)")
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
    let matches_everywhere = mismatches.is_empty();
    let listed = capability::incremental(name);
    assert_eq!(
        listed,
        matches_everywhere,
        "capability::incremental({name:?}) is {listed}, but the grammar {} over {read} fixtures it reads{}{}",
        if matches_everywhere {
            "matched everywhere; add it to capability::INCREMENTAL"
        } else {
            "mismatched; remove it from capability::INCREMENTAL, or fix the adapter"
        },
        if mismatches.is_empty() { "" } else { ":\n  " },
        mismatches.join("\n  ")
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

#[test]
fn every_grammar_in_the_dev_dependencies_is_verified_here() {
    // The list can only name grammars this suite runs.
    for name in capability::INCREMENTAL {
        assert!(
            GRAMMARS.iter().any(|g| g.name == *name),
            "capability::INCREMENTAL names {name:?}, which this suite does not run"
        );
    }
    assert_eq!(GRAMMARS.len(), 13);
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

#[test]
fn a_repeated_member_a_grammar_refuses_is_the_grammars_error_in_both_modes() {
    let text = ".{ .a = 1, .a = 2 }";
    let (whole, _) = run(tabnas_zon::make, text, SourceMode::Materialize);
    let (result, events) = incremental(tabnas_zon::make, text);
    assert_eq!(whole.unwrap_err().code, Code::InputInvalid);
    assert_eq!(result.unwrap_err().code, Code::InputInvalid);
    assert!(well_formed(&events));
}
