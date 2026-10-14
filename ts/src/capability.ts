/* Copyright (c) 2026 tabnas, MIT License */

// Which grammars the incremental source is verified for, in THIS runtime.
//
// Incremental streaming is a per-grammar VERIFIED capability, never an
// assumption, and it is earned per runtime: the TypeScript grammars build
// their values through TypeScript rule events, whose shapes need not be the
// Rust ones, so this list is not copied from the Rust crate.
// `test/incremental.test.ts` runs every fixture under rs/tests/fixtures
// (and the generated documents) through both the incremental and the
// materialized path for every grammar in the devDependencies, and asserts
// this list in both directions: a listed grammar that ever completes a
// stream the walk contradicts, or that the adapter refuses for how it
// builds its values, fails the suite, and so does an unlisted grammar that
// never does. A grammar not listed still works through materialize mode
// and `ValueSource`; it just retains the whole value, and `ParserSource`
// refuses to run it incrementally.
//
// What "verified" promises is the Rust runtime's promise (docs/reference.md,
// "The verified grammars"): an incremental run either streams exactly what
// the walk would (number lexemes aside), or streams every occurrence of a
// repeated member name so that a `last_wins` router builds the walk's
// value, or fails before `end` with a documented code, never with a wrong
// stream.

// Measured: the TypeScript grammars earn the same nine as the Rust ones.
// `xml` builds each element in document order, in a node of its own made
// when the start tag is read, with `children` named before its list opens
// and the list in a node of its own. `toml` and `ini` open a section's
// container before its key, `csv` builds a record the stream does not see,
// and `feed` replaces xml's root element with the feed it makes of the tree
// when the root closes, so the adapter refuses each on its own samples.

// The grammars whose incremental events never contradict the whole-value
// walk on any fixture, by the name of the format each reads: its package's
// (`@tabnas/<name>`), but `pgn` for @tabnas/chess, the name hosts ask by.
// @tabnas/chess builds each game on a stack in the parse's context, so
// the database is the only container in the parse tree and a game is
// stored in it only once complete: each game is walked at its insertion.
// That is @tabnas/chess 0.1.17 and later; earlier releases attached a
// game's parts after the adapter had sent them, and this package does not
// depend on @tabnas/chess, so a host that streams PGN takes 0.1.17 or later.
export const INCREMENTAL: readonly string[] = Object.freeze([
  'json',
  'json5',
  'jsonc',
  'jsonic',
  'jsonl',
  'markdown',
  'pgn',
  'xml',
  'yaml',
  'zon',
])

// Whether `grammar` may be run in incremental mode.
export function isIncremental(grammar: string): boolean {
  return INCREMENTAL.includes(grammar)
}

export const capability = Object.freeze({
  INCREMENTAL,
  incremental: isIncremental,
})
