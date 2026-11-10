//! Which grammars a [`crate::ParserEventSource`] may prune behind its
//! stream ([`PRUNABLE`]), and which of them have their maps emptied as
//! well as their arrays ([`PRUNES_MAPS`]): verified capabilities, never
//! assumptions.
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
    "csv", "feed", "ini", "json", "json5", "jsonc", "jsonic", "jsonl", "markdown", "pgn", "toml",
    "xml", "yaml", "zon",
];

/// Whether `grammar`'s arrays may be emptied behind a
/// [`crate::ParserEventSource`]'s stream ([`crate::ParserEventSource::prune`]).
pub fn prunable(grammar: &str) -> bool {
    PRUNABLE.contains(&grammar)
}

/// The grammars whose maps a pruning [`crate::ParserEventSource`] empties
/// too, at a close pass whose rule's node is the map, as it empties
/// arrays: their passes are the same with their maps emptied, on every
/// document `tests/prune_test.rs` reads them, and the suite asserts the
/// list in both directions. Every one is in [`PRUNABLE`]. The grammars it
/// leaves out read their maps back during the parse: yaml, toml, xml, zon
/// and feed find a key or a table defined twice, or a name to merge or
/// resolve, by looking it up in the tree they are building, so with their
/// maps emptied they pass documents they refuse, or refuse documents they
/// pass.
pub const PRUNES_MAPS: &[&str] = &[
    "csv", "ini", "json", "json5", "jsonc", "jsonic", "jsonl", "markdown", "pgn",
];

/// Whether `grammar`'s maps are emptied behind a pruning
/// [`crate::ParserEventSource`]'s stream as well as its arrays.
pub fn prunes_maps(grammar: &str) -> bool {
    PRUNES_MAPS.contains(&grammar)
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
            prunable("toml"),
            "its message for a key defined twice names no value, since tabnas-toml 0.5.20"
        );
        assert!(!prunable("css"), "the suite does not run it");
        assert!(!prunable("chess"), "the format's name, not the crate's");
        assert!(!prunable(""));
        for name in PRUNABLE {
            assert!(prunable(name));
        }
    }

    #[test]
    fn every_grammar_whose_maps_are_emptied_is_prunable() {
        for name in PRUNES_MAPS {
            assert!(prunes_maps(name));
            assert!(
                prunable(name),
                "{name} empties its maps, and is not prunable"
            );
        }
        assert!(prunes_maps("ini"));
        assert!(
            !prunes_maps("toml"),
            "it finds a key defined twice in its tree"
        );
        assert!(!prunes_maps(""));
    }
}
