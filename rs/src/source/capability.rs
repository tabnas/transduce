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

/// The grammars whose incremental events equal the whole-value walk on
/// every fixture, by the name their crate uses (`tabnas-<name>`).
pub const INCREMENTAL: &[&str] = &["json", "json5", "jsonc", "jsonic", "jsonl", "zon"];

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
        assert!(!incremental("csv"));
        assert!(!incremental(""));
        for name in INCREMENTAL {
            assert!(incremental(name));
        }
    }
}
