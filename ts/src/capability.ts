/* Copyright (c) 2026 tabnas, MIT License */

// Which grammars a `ParserEventSource` may prune behind its stream, in THIS
// runtime (rs/src/source/capability.rs): a verified capability, never an
// assumption. `test/prune.test.ts` runs every document of every grammar in
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
// message for a key defined twice names no value.
export const PRUNABLE: readonly string[] = Object.freeze([
  'csv',
  'feed',
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

export const capability = Object.freeze({
  PRUNABLE,
  prunable: isPrunable,
})
