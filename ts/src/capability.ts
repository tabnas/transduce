/* Copyright (c) 2026 tabnas, MIT License */

// Which grammars a `ParserEventSource` may prune behind its stream, and which
// of them have their maps emptied as well as their arrays, in THIS runtime
// (rs/src/source/capability.rs): verified capabilities, never assumptions. `test/prune.test.ts` runs every document of every grammar in
// the devDependencies pruned and not, compares the passes and the outcomes,
// and asserts the list in both directions, so it can rot in neither. The
// TypeScript grammars build their values through TypeScript rule passes,
// whose shapes need not be the Rust ones, so this list is earned here and
// not copied from the Rust crate.
//
// The incremental list, the grammars whose rule events the inference in
// `rule-events.ts` turned into a document's events, is gone with the
// inference (alchemy's streaming plan, step 13, in Rust first and then in
// this port): every format on it has a transducer part now, or reads whole
// as markdown declares, and a part makes the events.

// The grammars whose passes a `ParserEventSource` relays alike whether it
// prunes or not, on every document `test/prune.test.ts` reads them, by the
// name of the format each reads: its package's (`@tabnas/<name>`), but
// `pgn` for @tabnas/chess, the name hosts ask by. The suite asserts the
// list in both directions. `toml` joined it with @tabnas/toml 0.5.20, whose
// message for a key defined twice names no value, and css and the grammar
// notations abnf, ebnf and gbnf once the suite ran them.
export const PRUNABLE: readonly string[] = Object.freeze([
  'abnf',
  'css',
  'csv',
  'ebnf',
  'feed',
  'gbnf',
  'ini',
  'json',
  'json5',
  'jsonc',
  'jsonic',
  'jsonl',
  'markdown',
  'pgn',
  'toml',
  'xml',
  'yaml',
  'zon',
])

// Whether a `ParserEventSource` may empty `grammar`'s arrays behind its
// stream (`ParserEventSource.prune`).
export function isPrunable(grammar: string): boolean {
  return PRUNABLE.includes(grammar)
}

// The grammars whose maps a pruning `ParserEventSource` empties too, at a
// close pass whose rule's node is the map, as it empties arrays: their
// passes are the same with their maps emptied, on every document
// `test/prune.test.ts` reads them, and the suite asserts the list in both
// directions. Every one is in `PRUNABLE`. The grammars it leaves out read
// their maps back during the parse: yaml, toml, xml, zon and feed find a key
// or a table defined twice, or a name to merge or resolve, by looking it up
// in the tree they are building, so with their maps emptied they pass
// documents they refuse, or refuse documents they pass; abnf, ebnf and gbnf
// append a finished rule to a list member of a map, which emptying the map
// deletes, and their TypeScript ports then fail where their Rust and Go ports
// go on (DIVERGENCE.md 7). css appended so too until tabnas/css 0.5.20, whose
// TypeScript makes the list again as its Go port does.
export const PRUNES_MAPS: readonly string[] = Object.freeze([
  'css',
  'csv',
  'ini',
  'json',
  'json5',
  'jsonc',
  'jsonic',
  'jsonl',
  'markdown',
  'pgn',
])

// Whether `grammar`'s maps are emptied behind a pruning `ParserEventSource`'s
// stream as well as its arrays.
export function prunesMaps(grammar: string): boolean {
  return PRUNES_MAPS.includes(grammar)
}

// The grammars a pruning `ParserEventSource` bounds the engine's rule
// history for (`STREAM_RULE_HISTORY`): every document `test/prune.test.ts`
// reads them parses alike with the history bounded and without, the same
// passes, the same ending and the same value, and the suite asserts the
// list in both directions. A grammar off it keeps the engine's default, an
// unbounded history, because what it reads back through the rule links
// reaches further than the bound keeps.
export const BOUNDS_HISTORY: readonly string[] = Object.freeze([
  'abnf',
  'css',
  'csv',
  'ebnf',
  'feed',
  'gbnf',
  'ini',
  'json',
  'json5',
  'jsonc',
  'jsonic',
  'jsonl',
  'markdown',
  'pgn',
  'toml',
  'xml',
  'yaml',
  'zon',
])

// Whether a pruning `ParserEventSource` bounds the engine's rule history for
// `grammar`.
export function boundsHistory(grammar: string): boolean {
  return BOUNDS_HISTORY.includes(grammar)
}

export const capability = Object.freeze({
  PRUNABLE,
  PRUNES_MAPS,
  BOUNDS_HISTORY,
  prunable: isPrunable,
  prunesMaps,
  boundsHistory,
})
