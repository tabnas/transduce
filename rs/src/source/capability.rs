//! Which grammars the incremental source is verified for.
//!
//! Incremental streaming is a per-grammar VERIFIED capability, never an
//! assumption: `tests/incremental_test.rs` runs every fixture of every
//! grammar in the dev-dependencies through both `SourceMode::Incremental`
//! and `SourceMode::Materialize` and compares the recordings. A grammar is
//! listed here only when every fixture matched, and the test asserts the
//! list in both directions, so it can rot in neither. A grammar not listed
//! still works through `Materialize` and `ValueSource`; it just retains
//! the whole value.
//!
//! Why jsonic is absent although the prototype matched it on its own
//! fixtures: the suite offers every small fixture to every grammar, and
//! jsonic reads CSV, INI and JSON Lines texts as top-level IMPLICIT lists.
//! One whose first element is a scalar (`1,2,3`) streams; one whose first
//! element is a container (`{a:1}\n{b:2}`, `[1]\n[2]`) cannot, because the
//! grammar wraps a value that has already been streamed as the root, and
//! the incremental source refuses it with `STREAMABILITY_UNKNOWN`. Since
//! that is a valid jsonic document, jsonic is not verified. The prototype's
//! prediction was for its own fixtures only; the brief's rule (every
//! fixture the grammar reads) decides here.

/// The grammars whose incremental events equal the whole-value walk on
/// every fixture, by the name their crate uses (`tabnas-<name>`).
pub const INCREMENTAL: &[&str] = &["json", "json5", "jsonc", "jsonl", "yaml", "zon"];

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
        assert!(!incremental("csv"));
        assert!(!incremental("jsonic"));
        assert!(!incremental(""));
        for name in INCREMENTAL {
            assert!(incremental(name));
        }
    }
}
