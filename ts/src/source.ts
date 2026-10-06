/* Copyright (c) 2026 tabnas, MIT License */

// Sources: where `JsonEvents/1` come from.
//
// - `ValueSource` walks a parsed engine value. Always correct, retains the
//   whole value: the fallback for every grammar.
// - `ParserSource` drives a tabnas parse of one text and, in incremental
//   mode, turns its rule events into source events as they happen, for the
//   grammars the differential suite has verified (`capability`); in
//   materialize mode it parses and walks.
// - `LinesSource` reads JSON Lines or CSV a record (or a chunk of records)
//   at a time, bounding memory whatever the input's size.
//
// Every source emits through `Guarded`, which enforces the source limits,
// polls the abort flag and counts the source metrics.

import {
  AbortFlag,
  Ev,
  Fail,
  Flow,
  Selector,
  Sink,
  engineCode,
  engineDetail,
  engineKeys,
  engineScalar,
  isEngineMap,
} from '@tabnas/alchemy/shared'

// Which arrays the incremental source empties as it streams them: none
// (`never`), the array whose elements the selector names (`under`: a
// trailing `[*]` names the elements; without one the selector names the
// array), or every array (`all_arrays`). Pruning alters the value the
// engine returns, which the incremental source discards; it is never
// applied in materialize mode.
export type Prune =
  | { type: 'never' }
  | { type: 'under'; selector: Selector }
  | { type: 'all_arrays' }

export const Prune = Object.freeze({
  never(): Prune {
    return { type: 'never' }
  },
  under(selector: Selector): Prune {
    return { type: 'under', selector }
  },
  allArrays(): Prune {
    return { type: 'all_arrays' }
  },
})

// How `ParserSource` produces its events: parse the whole text, then walk
// the value (`materialize`, sound for every grammar), or emit from the
// engine's rule events as the parse proceeds (`incremental`, sound for the
// grammars `capability.incremental` lists).
export type SourceMode = { type: 'materialize' } | { type: 'incremental'; prune: Prune }

export const SourceMode = Object.freeze({
  materialize(): SourceMode {
    return { type: 'materialize' }
  },
  incremental(prune: Prune = Prune.never()): SourceMode {
    return { type: 'incremental', prune }
  },
})

// Something that can drive a sink with one document's events. `run`
// returns `'stop'` when the sink stopped it (the document was not
// validated past that point) and throws a `Fail` on failure.
export interface Source {
  run(sink: Sink): Flow
}

// The engine's cancel code: what a cancelled parse reports, whether the
// canceller is this crate's abort or a guard of the grammar's own.
const CANCEL = 'cancel'

// Map an engine error to a failure. A cancel while the caller's flag is set
// is `ABORTED`. A cancel otherwise is a guard the GRAMMAR installed, so
// the message says so instead of "parse cancelled", which would read as
// the caller's doing; it stays `INPUT_INVALID`, because the document is
// what the grammar refused. Any other error is the input's, with the
// engine's code and position. A thrown value that is not an engine error
// (a grammar's plugin threw) is the input's too, without a position.
export function engineFailure(error: unknown, abort: AbortFlag): Fail {
  if (error instanceof Fail) return error
  if (CANCEL !== engineCode(error)) return Fail.fromTabnas(error)
  if (abort.isAborted()) return Fail.aborted()
  const fail = Fail.fromTabnas(error)
  fail.message =
    `the grammar stopped the parse with a guard of its own (${engineCode(error)}: ` +
    `${engineDetail(error)}); a grammar may refuse nesting or size below this crate's Limits`
  return fail
}

// Emit one engine value's events (without `end`). `undefined` is `null`,
// as the engine serializes it; an object's members are in insertion order.
export function walkValue(value: unknown, sink: Sink): Flow {
  if (Array.isArray(value)) {
    if ('stop' === sink.event(Ev.arrayStart)) return 'stop'
    for (const item of value) {
      if ('stop' === walkValue(item, sink)) return 'stop'
    }
    return sink.event(Ev.arrayEnd)
  }
  const scalar = engineScalar(value)
  if (undefined !== scalar) {
    switch (scalar.type) {
      case 'null':
        return sink.event(Ev.null)
      case 'bool':
        return sink.event(Ev.bool(scalar.value))
      case 'number':
        return sink.event(Ev.number(scalar.value))
      case 'string':
        return sink.event(Ev.string(scalar.value))
    }
  }
  if ('stop' === sink.event(Ev.objectStart)) return 'stop'
  // Neither an array nor a scalar: a map, which `isEngineMap` confirms.
  if (isEngineMap(value)) {
    for (const k of engineKeys(value)) {
      if ('stop' === sink.event(Ev.key(k))) return 'stop'
      if ('stop' === walkValue(value[k], sink)) return 'stop'
    }
  }
  return sink.event(Ev.objectEnd)
}

// Emit a parsed engine value as events, ending with `end`. Applies no
// limits: wrap the sink in `Guarded` for those.
export class ValueSource implements Source {
  readonly value: unknown

  constructor(value: unknown) {
    this.value = value
  }

  run(sink: Sink): Flow {
    if ('stop' === walkValue(this.value, sink)) return 'stop'
    return sink.event(Ev.end)
  }
}
