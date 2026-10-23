//! Which grammars a [`crate::ParserEventSource`] may prune behind its
//! stream ([`PRUNABLE`]): a verified capability, never an assumption.
//! `tests/prune_test.rs` runs every document of every grammar in the
//! dev-dependencies pruned and not, compares the passes and the outcomes,
//! and asserts the list in both directions, so it can rot in neither.
//!
//! The incremental list, the grammars whose rule events the inference in
//! `rule_events.rs` turned into a document's events, is gone with the
//! inference (alchemy's streaming plan, step 13): every format on it has
//! a transducer part now, or reads whole as markdown declares, and a part
//! makes the events.

/// The grammars whose passes a [`crate::ParserEventSource`] relays alike
/// whether it prunes or not, on every document `tests/prune_test.rs` reads
/// them, by the name of the format each reads: its crate's
/// (`tabnas-<name>`), but `pgn` for tabnas-chess, the name hosts ask by.
/// The suite asserts the list in both directions.
pub const PRUNABLE: &[&str] = &[
    "csv", "feed", "ini", "json", "json5", "jsonc", "jsonic", "jsonl", "markdown", "pgn", "xml",
    "yaml", "zon",
];

/// Whether `grammar`'s arrays may be emptied behind a
/// [`crate::ParserEventSource`]'s stream ([`crate::ParserEventSource::prune`]).
pub fn prunable(grammar: &str) -> bool {
    PRUNABLE.contains(&grammar)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_prunable_list_answers_by_grammar_name() {
        assert!(prunable("json"));
        assert!(prunable("yaml"));
        assert!(prunable("xml"));
        assert!(prunable("pgn"));
        assert!(
            !prunable("toml"),
            "its message for a key defined twice quotes the value"
        );
        assert!(!prunable("chess"), "the format's name, not the crate's");
        assert!(!prunable(""));
        for name in PRUNABLE {
            assert!(prunable(name));
        }
    }
}
