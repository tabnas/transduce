/* Copyright (c) 2026 tabnas, MIT License */

// `ParserSource`: `JsonEvents/1` from a tabnas parse of one text
// (rs/src/source/parser.rs).
//
// It parses, then walks the value: always sound, for every grammar, and it
// retains the whole value. A format whose package ships a transducer part
// streams through the part instead, over `ParserEventSource`; the
// rule-event inference that streamed the grammars the differential suite
// had verified, before the parts, was retired with alchemy's streaming
// plan's step 13, in Rust first and then in this port.
//
// The parser is the source's: the source sets its parse budget (the engine
// has no way to take it back), so a parser is given to one source and not
// reused afterwards, and a source runs once (a second run is
// `STREAM_REUSED`).
//
// Failure mapping, in this order: a sink failure is thrown as it was; a
// sink that stopped is `'stop'`; a parse cancelled through the caller's
// `AbortFlag` is `ABORTED`; any other engine error is `INPUT_INVALID`
// with the engine's code and position; a grammar's own guard cancelling
// the parse is `INPUT_INVALID` too, with a message that names the
// grammar's guard.

import { AbortFlag, Ev, Fail, Flow, Limits, Metrics, Sink } from '@tabnas/alchemy/shared'
import type { Tabnas } from '@tabnas/parser'

import { Guarded } from './guard'
import { Source, engineFailure, prepare, walkValue } from './source'

// A tabnas parser applied to one text, as a source.
export class ParserSource implements Source {
  private parser: Tabnas
  private text: string
  private sourceLimits: Limits = Limits.default()
  private abortFlag: AbortFlag = new AbortFlag()
  private sourceMetrics: Metrics = new Metrics()
  private used = false

  // A source with default limits, its own abort flag and fresh metrics;
  // the builder methods change each.
  constructor(parser: Tabnas, text: string) {
    this.parser = parser
    this.text = text
  }

  limits(limits: Limits): this {
    this.sourceLimits = limits
    return this
  }

  abort(abort: AbortFlag): this {
    this.abortFlag = abort
    return this
  }

  metrics(metrics: Metrics): this {
    this.sourceMetrics = metrics
    return this
  }

  // Parse, then walk, into `sink`.
  run(sink: Sink): Flow {
    return this.runWithValue(sink).flow
  }

  // `run`, also handing back the grammar's value, when the parse returned
  // one.
  runWithValue(sink: Sink): { flow: Flow; value: unknown } {
    if (this.used) {
      throw new Fail(
        'STREAM_REUSED',
        "a ParserSource runs once: its parser carries the run's budget; " +
          'make a new source, with a new parser, for another run',
      )
    }
    this.used = true
    const guarded = new Guarded(sink, this.sourceLimits, this.abortFlag, this.sourceMetrics)
    try {
      return materialize(this.parser, this.text, this.abortFlag, guarded)
    } finally {
      guarded.flush()
    }
  }
}

// Parse, then walk; the grammar's value comes back beside the outcome.
function materialize<S extends Sink>(
  parser: Tabnas,
  text: string,
  abort: AbortFlag,
  guarded: Guarded<S>,
): { flow: Flow; value: unknown } {
  prepare(parser, () => !abort.isAborted())
  let value: unknown
  try {
    value = parser.parse(text)
  } catch (err) {
    throw engineFailure(err, abort)
  }
  let flow = walkValue(value, guarded)
  if ('continue' === flow) flow = guarded.event(Ev.end)
  return { flow, value }
}
