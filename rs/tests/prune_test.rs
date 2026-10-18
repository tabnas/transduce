//! The differential suite behind `capability::prunable`.
//!
//! A `ParserEventSource` that prunes empties each array in the engine's
//! tree once the sink has seen a close pass on it. A pass's value is a
//! scalar or nothing, so for a grammar that never reads its arrays back
//! the passes are the same either way; a grammar whose actions or
//! conditions do read them may make other passes, or fail elsewhere, once
//! they are emptied. So for every grammar in the dev-dependencies and every
//! document the suite has for it, a run that prunes must relay the same
//! passes as one that does not, pass for pass in the fixture encoding, and
//! end the same way: both complete, or both fail with the same code,
//! position and message.
//!
//! One difference short of identity is the contract, and the suite counts
//! it apart: a token whose value is a container of the tree carries what
//! the tree holds when its pass is made, so the arrays in it may have been
//! emptied, while its kind and its maps' members are as they were. yaml's
//! lexer resolves an alias to the anchored node so (anchors-aliases.tsv),
//! and yaml's part reads such a token's kind alone, refusing an alias to a
//! sequence or a mapping as one whose members the passes do not show. A
//! part that read the elements of a token's arrays would read what pruning
//! left, so none does.
//!
//! The documents are the committed fixtures under
//! `tests/fixtures/` (offered to every grammar, which reads whatever
//! parses), the generated worked-example documents of each grammar's
//! family, and every document of the grammar's own corpus: the
//! `test/spec/*.tsv` rows of the sibling checkout this crate builds it
//! from, under the options its `make` gives (a row's own options are not
//! applied: the suite runs one parser). A grammar is listed in
//! `capability::PRUNABLE` only when no document differs, and the suite
//! asserts the list in both directions, so it can rot in neither.
//!
//! The second half measures what pruning is for: ten times the records of
//! a document leave the pruned tree holding exactly as many elements,
//! while the tree a run that does not prune hands back holds every record.

mod support;

use std::fs;
use std::path::PathBuf;

use tabnas::{Tabnas, Value};
use tabnas_transduce::{
    capability, Datum, Fail, Flow, ParserEvent, ParserEventSource, ParserSink, RulePass,
};

/// The generated documents' records in the differential: as many shapes
/// as 2,000 would hold, at a tenth of the parsing.
const RECORDS: usize = 200;

/// tabnas-chess reading a PGN database, as a host reads the `pgn` format.
fn pgn() -> Tabnas {
    tabnas_chess::make(&tabnas_chess::ChessOptions::default()).expect("the PGN grammar installs")
}

struct Grammar {
    name: &'static str,
    make: fn() -> Tabnas,
    /// Which generated documents this grammar is offered.
    generated: &'static [&'static str],
    /// The sibling checkout whose `test/spec` corpus it reads.
    repo: &'static str,
}

const GRAMMARS: &[Grammar] = &[
    Grammar {
        name: "json",
        make: tabnas_json::make,
        generated: &["records.json"],
        repo: "json",
    },
    Grammar {
        name: "jsonl",
        make: tabnas_jsonl::make,
        generated: &["records.jsonl"],
        repo: "jsonl",
    },
    Grammar {
        name: "jsonic",
        make: tabnas_jsonic::make,
        generated: &["records.json"],
        repo: "jsonic",
    },
    Grammar {
        name: "jsonc",
        make: tabnas_jsonc::make,
        generated: &["records.json"],
        repo: "jsonc",
    },
    Grammar {
        name: "json5",
        make: tabnas_json5::make,
        generated: &["records.json"],
        repo: "json5",
    },
    Grammar {
        name: "yaml",
        make: tabnas_yaml::make,
        generated: &["records.yaml"],
        repo: "yaml",
    },
    Grammar {
        name: "toml",
        make: tabnas_toml::make,
        generated: &[],
        repo: "toml",
    },
    Grammar {
        name: "ini",
        make: tabnas_ini::make,
        generated: &[],
        repo: "ini",
    },
    Grammar {
        name: "csv",
        make: tabnas_csv::make,
        generated: &["records.csv"],
        repo: "csv",
    },
    Grammar {
        name: "xml",
        make: tabnas_xml::make,
        generated: &["records.xml"],
        repo: "xml",
    },
    Grammar {
        name: "zon",
        make: tabnas_zon::make,
        generated: &[],
        repo: "zon",
    },
    Grammar {
        name: "markdown",
        make: tabnas_markdown::make,
        generated: &[],
        repo: "markdown",
    },
    Grammar {
        name: "feed",
        make: tabnas_feed::make,
        generated: &[],
        repo: "feed",
    },
    Grammar {
        name: "pgn",
        make: pgn,
        generated: &["records.pgn"],
        repo: "chess",
    },
];

fn generated(name: &str, records: usize) -> String {
    match name {
        "records.json" => support::records_json(records),
        "records.jsonl" => support::records_jsonl(records),
        "records.csv" => support::records_csv(records),
        "records.yaml" => support::records_yaml(records),
        "records.xml" => support::records_xml(records),
        "records.pgn" => support::records_pgn(records),
        other => panic!("no generator for {other}"),
    }
}

/// Every committed fixture, by file name, in a stable order.
fn committed_fixtures() -> Vec<(String, String)> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let mut out: Vec<(String, String)> = fs::read_dir(dir)
        .expect("tests/fixtures exists")
        .map(|entry| {
            let path = entry.expect("a directory entry").path();
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .expect("a UTF-8 file name")
                .to_string();
            (name, fs::read_to_string(&path).expect("a UTF-8 fixture"))
        })
        .collect();
    out.sort();
    out
}

/// Every document of the `test/spec/*.tsv` rows of the sibling checkout
/// `repo`, named by file and line.
fn own_corpus(repo: &str) -> Vec<(String, String)> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!("../../{repo}/test/spec"));
    let specs = tabnas_support::load_spec_dir(&dir, &tabnas_support::SpecOptions::default())
        .unwrap_or_else(|e| {
            panic!("{e}: clone https://github.com/tabnas/{repo} beside this repository")
        });
    specs
        .iter()
        .flat_map(|spec| {
            spec.rows.iter().map(|row| {
                (
                    format!("{}:{}", row.file, row.line),
                    row.unesc_named("input"),
                )
            })
        })
        .collect()
}

/// The passes, each in the fixture encoding, and again with the arrays in
/// its tokens' container values emptied: the passes as pruning may leave
/// them.
#[derive(Default)]
struct Passes {
    exact: Vec<String>,
    shaped: Vec<String>,
}

impl ParserSink for Passes {
    fn event(&mut self, ev: &ParserEvent) -> Result<Flow, Fail> {
        self.exact.push(ev.to_json().to_string());
        let shaped = match ev {
            ParserEvent::Open(pass) => ParserEvent::Open(shaped(pass)),
            ParserEvent::Close(pass) => ParserEvent::Close(shaped(pass)),
            ParserEvent::End => ParserEvent::End,
        };
        self.shaped.push(shaped.to_json().to_string());
        Ok(Flow::Continue)
    }
}

/// The pass with the arrays in its tokens' container values emptied.
fn shaped(pass: &RulePass) -> RulePass {
    let mut pass = pass.clone();
    for token in &mut pass.tokens {
        token.val = emptied(&token.val);
    }
    pass
}

/// A datum with every array in it emptied, its maps' members kept.
fn emptied(datum: &Datum) -> Datum {
    match datum {
        Datum::Array(_) => Datum::Array(Vec::new()),
        Datum::Object(members) => Datum::Object(
            members
                .iter()
                .map(|(name, value)| (name.clone(), emptied(value)))
                .collect(),
        ),
        scalar => scalar.clone(),
    }
}

/// One run: how it ended, its passes, and the engine's tree afterwards.
fn run(
    make: fn() -> Tabnas,
    text: &str,
    prune: bool,
) -> (Result<Flow, Fail>, Passes, Option<Value>) {
    let mut source = ParserEventSource::new(make(), text);
    if prune {
        source = source.prune().unverified();
    }
    source.run_owned_with_value(Passes::default())
}

/// How a run ended, in the terms the two runs must agree on.
fn ending(result: &Result<Flow, Fail>) -> String {
    match result {
        Ok(flow) => format!("Ok({flow:?})"),
        Err(fail) => format!(
            "{} at {:?}:{:?}: {}",
            fail.code, fail.row, fail.column, fail.message
        ),
    }
}

/// What one document did, pruned and not.
enum Outcome {
    Same,
    /// The same once the arrays in the tokens' container values are set
    /// aside: the contract's one difference.
    SameShape,
    Differs(String),
}

/// How the pruned run compares with the one that did not prune.
fn compare(make: fn() -> Tabnas, text: &str) -> Outcome {
    let (whole, kept, _) = run(make, text, false);
    let (pruned, left, _) = run(make, text, true);
    let (whole, pruned) = (ending(&whole), ending(&pruned));
    if whole != pruned {
        return Outcome::Differs(format!("ended {whole} without pruning, {pruned} pruned"));
    }
    if kept.exact == left.exact {
        return Outcome::Same;
    }
    match (0..kept.shaped.len().max(left.shaped.len()))
        .find(|&i| kept.shaped.get(i) != left.shaped.get(i))
    {
        None => Outcome::SameShape,
        Some(i) => Outcome::Differs(format!(
            "pass {i} of {} differs: {:?} without pruning, {:?} pruned",
            kept.exact.len(),
            kept.exact.get(i),
            left.exact.get(i)
        )),
    }
}

/// The documents the suite has for `grammar`.
fn documents(grammar: &Grammar) -> Vec<(String, String)> {
    let mut docs = committed_fixtures();
    for name in grammar.generated {
        docs.push((name.to_string(), generated(name, RECORDS)));
    }
    docs.extend(own_corpus(grammar.repo));
    docs
}

/// Each grammar's documents, pruned and not; the differences, by grammar.
fn differences(grammar: &Grammar) -> Vec<String> {
    let started = std::time::Instant::now();
    let docs = documents(grammar);
    let total = docs.len();
    let mut found = Vec::new();
    let mut shaped = Vec::new();
    for (i, (name, text)) in docs.iter().enumerate() {
        match compare(grammar.make, text) {
            Outcome::Same => {}
            Outcome::SameShape => shaped.push(name.as_str()),
            Outcome::Differs(why) => found.push(format!("{name}: {why}")),
        }
        if (i + 1) % 100 == 0 || i + 1 == total {
            println!(
                "{}: {} of {total} ({}%) documents, {} differing, {:.1} s",
                grammar.name,
                i + 1,
                (i + 1) * 100 / total,
                found.len(),
                started.elapsed().as_secs_f64()
            );
        }
    }
    if !shaped.is_empty() {
        println!(
            "{}: {} the same once the arrays in their tokens' containers are set aside: {}",
            grammar.name,
            shaped.len(),
            shaped.join(", ")
        );
    }
    found
}

/// The list in both directions, for one grammar: listed, and no document
/// makes it differ; or not listed, and some document does.
fn prunes_as_the_list_says(name: &str) {
    let grammar = GRAMMARS
        .iter()
        .find(|g| g.name == name)
        .expect("the grammar is in the suite");
    let found = differences(grammar);
    match (capability::prunable(name), found.is_empty()) {
        (true, false) => panic!(
            "{name} is in capability::PRUNABLE, but {} of its documents differ:\n    {}",
            found.len(),
            found.join("\n    ")
        ),
        (false, true) => {
            panic!("{name} is not in capability::PRUNABLE, but none of its documents differs")
        }
        _ => {}
    }
}

/// A test per grammar, so they run side by side.
macro_rules! prunes_as_the_list_says {
    ($($test:ident: $name:literal,)*) => {
        $(
            #[test]
            fn $test() {
                prunes_as_the_list_says($name);
            }
        )*

        /// Every grammar the suite runs has its test, and every listed
        /// grammar is one the suite runs.
        #[test]
        fn every_grammar_is_held_to_the_list() {
            let tested = [$($name),*];
            for grammar in GRAMMARS {
                assert!(tested.contains(&grammar.name), "{} has no test", grammar.name);
            }
            for name in capability::PRUNABLE {
                assert!(
                    GRAMMARS.iter().any(|g| g.name == *name),
                    "{name} is listed, and this suite never runs it"
                );
            }
        }
    };
}

prunes_as_the_list_says! {
    json_prunes_as_the_list_says: "json",
    jsonl_prunes_as_the_list_says: "jsonl",
    jsonic_prunes_as_the_list_says: "jsonic",
    jsonc_prunes_as_the_list_says: "jsonc",
    json5_prunes_as_the_list_says: "json5",
    yaml_prunes_as_the_list_says: "yaml",
    toml_prunes_as_the_list_says: "toml",
    ini_prunes_as_the_list_says: "ini",
    csv_prunes_as_the_list_says: "csv",
    xml_prunes_as_the_list_says: "xml",
    zon_prunes_as_the_list_says: "zon",
    markdown_prunes_as_the_list_says: "markdown",
    feed_prunes_as_the_list_says: "feed",
    pgn_prunes_as_the_list_says: "pgn",
}

/// Every element of every array in `value`, and every member of every map.
fn elements(value: &Value) -> usize {
    match value {
        Value::Array(items) => items.len() + items.iter().map(elements).sum::<usize>(),
        Value::ListRef(list) => list.value.len() + list.value.iter().map(elements).sum::<usize>(),
        Value::Object(map) => map.len() + map.values().map(elements).sum::<usize>(),
        Value::MapRef(map) => map.value.len() + map.value.values().map(elements).sum::<usize>(),
        _ => 0,
    }
}

/// What the tree holds after a run of `records` records, pruned or not.
fn left(grammar: &str, doc: &str, records: usize, prune: bool) -> usize {
    let make = GRAMMARS
        .iter()
        .find(|g| g.name == grammar)
        .expect("the grammar is in the suite")
        .make;
    let (result, _, value) = run(make, &generated(doc, records), prune);
    assert_eq!(
        result.unwrap(),
        Flow::Continue,
        "{grammar} over {records} records"
    );
    elements(&value.expect("a parse that completed returns a value"))
}

/// Ten times the records leave the pruned tree as it was, for one grammar
/// the hosts read records with, while the tree a run that does not prune
/// hands back grows with them.
fn leaves_the_pruned_tree_as_it_was(grammar: &str, doc: &str) {
    let (few, many) = (
        left(grammar, doc, 200, true),
        left(grammar, doc, 2000, true),
    );
    let (whole_few, whole_many) = (
        left(grammar, doc, 200, false),
        left(grammar, doc, 2000, false),
    );
    println!(
        "retention: {grammar}: pruned {few} then {many} elements, not pruned {whole_few} then \
         {whole_many}"
    );
    assert_eq!(
        few, many,
        "{grammar}: the pruned tree held {few} elements after 200 records and {many} after 2000"
    );
    assert!(
        whole_many >= whole_few + 1800,
        "{grammar}: the tree held {whole_few} then {whole_many} elements without pruning: is \
         the document {doc} records?"
    );
}

/// A test per grammar, so they run side by side.
macro_rules! leaves_the_pruned_tree_as_it_was {
    ($($test:ident: $name:literal $doc:literal,)*) => {
        $(
            #[test]
            fn $test() {
                leaves_the_pruned_tree_as_it_was($name, $doc);
            }
        )*
    };
}

leaves_the_pruned_tree_as_it_was! {
    ten_times_the_json_records_leave_the_pruned_tree_as_it_was: "json" "records.json",
    ten_times_the_jsonc_records_leave_the_pruned_tree_as_it_was: "jsonc" "records.json",
    ten_times_the_json5_records_leave_the_pruned_tree_as_it_was: "json5" "records.json",
    ten_times_the_jsonic_records_leave_the_pruned_tree_as_it_was: "jsonic" "records.json",
    ten_times_the_jsonl_records_leave_the_pruned_tree_as_it_was: "jsonl" "records.jsonl",
    ten_times_the_yaml_records_leave_the_pruned_tree_as_it_was: "yaml" "records.yaml",
    ten_times_the_xml_records_leave_the_pruned_tree_as_it_was: "xml" "records.xml",
    ten_times_the_csv_records_leave_the_pruned_tree_as_it_was: "csv" "records.csv",
    ten_times_the_pgn_games_leave_the_pruned_tree_as_it_was: "pgn" "records.pgn",
}
