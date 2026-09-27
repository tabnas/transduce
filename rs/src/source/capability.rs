//! Which grammars the incremental source is verified for.
//!
//! Incremental streaming is a per-grammar VERIFIED capability, never an
//! assumption: `tests/incremental_test.rs` runs every fixture of every
//! grammar in the dev-dependencies through both `SourceMode::Incremental`
//! and `SourceMode::Materialize` and compares the recordings. A grammar is
//! listed here only when no fixture produced a completed stream that
//! disagrees with the walk, and the test asserts the list in both
//! directions, so it can rot in neither. A grammar not listed still works
//! through `Materialize` and `ValueSource`; it just retains the whole
//! value, and `ParserSource` refuses to run it incrementally.
//!
//! What "verified" promises, precisely: for a listed grammar an incremental
//! run either streams exactly what the walk would (number lexemes aside),
//! or, where the document repeats a member name, streams every occurrence
//! so that a `LastWins` router builds the walk's value, or FAILS before
//! `End` with a documented code and never a wrong stream. The refusals
//! are the shapes the rule events cannot follow: a container that wraps a
//! value already streamed as the root (a YAML stream of several documents,
//! whatever the documents' shapes, caught at the second document's
//! container when it has one and otherwise when the root rule closes over
//! the wrapping list; a jsonic top-level implicit list whose first element
//! is a container), a map the grammar rewrote after it was streamed (a
//! YAML merge key), both `STREAMABILITY_UNKNOWN`; and a repeated member
//! whose containers the grammar merged (jsonic's `map.extend`, on for
//! yaml, json5 and jsonic), `DUPLICATE_MEMBER`. `rule_events.rs`
//! documents each.
//!
//! markdown is listed although it builds its nodes imperatively: each
//! node is inserted whole and walked at its insertion, so the events are
//! the walk's, only later than a container-by-container stream. The other
//! imperative grammars (toml, ini, csv, xml, feed) build their values in
//! ways the rule events do not show: the adapter emits well-formed streams
//! with the wrong shape (csv's header and raw rows as extra elements) or
//! malformed ones, and a completed wrong stream is exactly what the list
//! exists to prevent, so they are not listed.

/// The grammars whose incremental events never contradict the whole-value
/// walk on any fixture, by the name their crate uses (`tabnas-<name>`).
pub const INCREMENTAL: &[&str] = &[
    "json", "json5", "jsonc", "jsonic", "jsonl", "markdown", "yaml", "zon",
];

/// Whether `grammar` may be run with `SourceMode::Incremental`.
pub fn incremental(grammar: &str) -> bool {
    INCREMENTAL.contains(&grammar)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_list_answers_by_grammar_name() {
        assert!(incremental("json"));
        assert!(incremental("yaml"));
        assert!(incremental("jsonic"));
        assert!(!incremental("csv"));
        assert!(!incremental("Json"), "names are the crates', lowercase");
        assert!(!incremental(""));
        for name in INCREMENTAL {
            assert!(incremental(name));
        }
    }
}
