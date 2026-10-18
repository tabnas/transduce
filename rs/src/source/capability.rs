//! Which grammars the incremental source is verified for, and which a
//! [`crate::ParserEventSource`] may prune behind its stream ([`PRUNABLE`],
//! at the end).
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
//! YAML merge key), and a container the grammar opens inside a map before
//! the member's key (as toml and ini open a section's container, as xml
//! opened an element's until its rules built each element in document
//! order, and as yaml opened the value of the first member of a mapping in
//! a sequence entry until tabnas/yaml#107), all `STREAMABILITY_UNKNOWN`;
//! a repeated member whose containers the grammar merged (jsonic's
//! `map.extend`, on for yaml, json5 and jsonic), `DUPLICATE_MEMBER`; and a
//! container the adapter streamed as it was built that the grammar then
//! never stored in the one around it (jsonic drops a pair inside a list
//! when `list.pair` is off), refused where the next entry lands, the next
//! container opens or the frame around it ends, `STREAMABILITY_UNKNOWN`.
//! Those last two are nets, not licences: a listed grammar trips neither
//! on a fixture, since a grammar that builds its values so does it for
//! every document, and the suite counts them against the grammar rather
//! than for it. It counts one more thing against a grammar: no document it
//! reads streaming to the end. A grammar refused on every document it
//! reads is refused for how it builds, whichever shape the refusal names
//! (feed, below). A document the
//! grammar itself refuses fails the incremental run with the grammar's
//! `INPUT_INVALID`, at the same position, after a protocol-valid prefix
//! (zon's repeated fields: the member the grammar never stored is not
//! streamed), and the suite checks that for every fixture a grammar does
//! not read. `rule_events.rs` documents each.
//!
//! markdown is listed although it builds its nodes imperatively: each
//! node is inserted whole and walked at its insertion, so the events are
//! the walk's, only later than a container-by-container stream. xml is
//! listed since its rules build each element in document order, in a cell
//! of its own made when the start tag is read, members in their order,
//! `children` named before its list opens and the list in a cell of its
//! own (tabnas/xml, "The order the rules build an element in"); a
//! complete container that lands whole, an element's attributes, is walked
//! at its insertion as markdown's nodes are. pgn (tabnas-chess) is listed
//! since the plugin builds each game, and each variation inside it, on a
//! stack in the parse's context, so the database is the only container in
//! the parse tree and a game is stored in it only once complete: each game
//! is walked at its insertion, a game at a time, as markdown's nodes are.
//! That is tabnas-chess 0.1.17 and later. Earlier releases attached a
//! game's annotations, variations and result after the adapter had sent
//! them, and this crate does not depend on tabnas-chess, so a host that
//! streams PGN takes 0.1.17 or later itself.
//! The other imperative grammars
//! (toml, ini, csv, feed) build their values in ways the rule events do not
//! show: csv builds a record per row that it does not store as the stream
//! saw it, toml and ini open a section's container before its key, and
//! feed, which builds on xml's events, replaces the root element with the
//! feed it makes of the tree when the root closes, so each of its documents
//! streams the element tree and is then refused as a rewritten root. A
//! completed wrong stream is exactly what the list exists to prevent, and a
//! grammar refused on its own documents would be attempted and refused on
//! every one, so they are not listed.

/// The grammars whose incremental events never contradict the whole-value
/// walk on any fixture, by the name of the format each reads: its crate's
/// (`tabnas-<name>`), but `pgn` for tabnas-chess, the name hosts ask by.
pub const INCREMENTAL: &[&str] = &[
    "json", "json5", "jsonc", "jsonic", "jsonl", "markdown", "pgn", "xml", "yaml", "zon",
];

/// Whether `grammar` may be run with `SourceMode::Incremental`.
pub fn incremental(grammar: &str) -> bool {
    INCREMENTAL.contains(&grammar)
}

/// The grammars whose passes a [`crate::ParserEventSource`] relays alike
/// whether it prunes or not, on every document `tests/prune_test.rs` reads
/// them, by the name of the format each reads, as [`INCREMENTAL`] names
/// them. The suite asserts the list in both directions.
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
    fn the_list_answers_by_grammar_name() {
        assert!(incremental("json"));
        assert!(incremental("yaml"));
        assert!(incremental("jsonic"));
        assert!(incremental("xml"));
        assert!(incremental("pgn"));
        assert!(!incremental("chess"), "the format's name, not the crate's");
        assert!(!incremental("csv"));
        assert!(!incremental("feed"));
        assert!(!incremental("Json"), "names are the crates', lowercase");
        assert!(!incremental(""));
        for name in INCREMENTAL {
            assert!(incremental(name));
        }
    }

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
