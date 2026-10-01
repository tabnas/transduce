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

// Measured: the TypeScript grammars earn the same eight as the Rust ones;
// `toml`, `ini`, `csv`, `xml` and `feed` open a section's, a record's or an
// element's container before its key, or build a record the stream does not
// see, and the adapter refuses each on its own samples.

// The grammars whose incremental events never contradict the whole-value
// walk on any fixture, by their package's name (`@tabnas/<name>`).
export const INCREMENTAL: readonly string[] = Object.freeze([
  'json',
  'json5',
  'jsonc',
  'jsonic',
  'jsonl',
  'markdown',
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
