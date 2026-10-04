/* Copyright (c) 2026 tabnas, MIT License */

// `ParserSource`: `JsonEvents/1` from a tabnas parse of one text.
//
// Two modes. `materialize` parses, then walks the value: always sound, and
// retains the whole value. `incremental` installs the rule-event adapter
// so events leave the parse as containers open and entries land, and
// optionally prunes streamed array elements from the engine's tree. It is
// sound for the grammars `capability.incremental` lists, which the
// differential suite verifies in this runtime, and for no other: an
// imperative grammar's rule events give a well-formed stream of the wrong
// shape or a malformed one, and the run would still return. So the
// incremental path is gated on the list, by the grammar's name, which the
// source cannot learn from the `Tabnas` and so must be told (`grammar`);
// without a listed name it fails with `STREAMABILITY_UNKNOWN` before the
// parse, having emitted nothing. `unverified()` lifts the gate for the
// differential suite that maintains the list.
//
// The parser is the source's: the source subscribes to its rule events and
// sets its parse budget (the engine has no way to take either back), so a
// parser is given to one source and not reused afterwards, and a source
// runs once (a second run is `STREAM_REUSED`).
//
// Failure mapping, in this order: a sink failure is thrown as it was; a
// sink that stopped is `'stop'`; a parse cancelled through the caller's
// `AbortFlag` is `ABORTED`; any other engine error is `INPUT_INVALID`
// with the engine's code and position; a grammar's own guard cancelling
// the parse is `INPUT_INVALID` too, with a message that names the
// grammar's guard. An incremental parse that returned without one rule
// event the adapter could turn into a value (YAML's empty document is
// `null`) has its value walked instead.

import { AbortFlag, Ev, Fail, Flow, Limits, Metrics, Sink } from '@tabnas/alchemy/shared'

import { isIncremental } from './capability'
import { Guarded } from './guard'
import { Adapter, notStreamable, prepare } from './rule-events'
import { Prune, Source, SourceMode, engineFailure, walkValue } from './source'

// A tabnas parser applied to one text, as a source.
export class ParserSource implements Source {
  private parser: any
  private text: string
  private sourceMode: SourceMode = SourceMode.materialize()
  private sourceLimits: Limits = Limits.default()
  private abortFlag: AbortFlag = new AbortFlag()
  private sourceMetrics: Metrics = new Metrics()
  private grammarName: string | null = null
  private isUnverified = false
  private used = false

  // A source in materialize mode with default limits, its own abort flag
  // and fresh metrics; the builder methods change each.
  constructor(parser: any, text: string) {
    this.parser = parser
    this.text = text
  }

  mode(mode: SourceMode): this {
    this.sourceMode = mode
    return this
  }

  // The grammar the parser implements, by its package's name (`json` for
  // `@tabnas/json`). Incremental mode runs only for a name
  // `capability.incremental` lists.
  grammar(name: string): this {
    this.grammarName = name
    return this
  }

  // Run incremental mode whatever the verified list says. This exists for
  // the differential suite that maintains the list and for nothing else.
  unverified(): this {
    this.isUnverified = true
    return this
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

  // Why the incremental path may not run, when it may not.
  private gate(): Fail | null {
    if (this.isUnverified) return null
    const name = this.grammarName
    if (null !== name && isIncremental(name)) return null
    if (null !== name) {
      return new Fail(
        'STREAMABILITY_UNKNOWN',
        `grammar ${JSON.stringify(name)} is not in capability.incremental: the differential ` +
          'suite has not verified that its rule events stream as the walk does; run it with ' +
          'SourceMode.materialize',
      )
    }
    return new Fail(
      'STREAMABILITY_UNKNOWN',
      "SourceMode.incremental needs the grammar's name (ParserSource.grammar) to check " +
        'capability.incremental; without one, run SourceMode.materialize',
    )
  }

  // Run in the configured mode.
  run(sink: Sink): Flow {
    return this.runWithValue(sink).flow
  }

  // `run`, also handing back the value the engine returned. In materialize
  // mode that is the grammar's value. In incremental mode it is the
  // engine's tree AFTER pruning, which is neither the grammar's value nor
  // the run's result (the events are): it exists so a test can measure
  // what pruning left in the tree, and nothing else should read it.
  runWithValue(sink: Sink): { flow: Flow; value: unknown } {
    if (this.used) {
      throw new Fail(
        'STREAM_REUSED',
        'a ParserSource runs once: its parser carries the run\'s subscriber and budget; ' +
          'make a new source, with a new parser, for another run',
      )
    }
    this.used = true
    const mode = this.sourceMode
    if ('materialize' === mode.type) {
      const guarded = new Guarded(sink, this.sourceLimits, this.abortFlag, this.sourceMetrics)
      try {
        return materialize(this.parser, this.text, this.abortFlag, guarded)
      } finally {
        guarded.flush()
      }
    }
    const refused = this.gate()
    if (null !== refused) throw refused
    return incremental(
      this.parser,
      this.text,
      this.sourceLimits,
      this.abortFlag,
      this.sourceMetrics,
      mode.prune,
      sink,
    )
  }
}

// Parse, then walk; the grammar's value comes back beside the outcome.
function materialize<S extends Sink>(
  parser: any,
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

function incremental<S extends Sink>(
  parser: any,
  text: string,
  limits: Limits,
  abort: AbortFlag,
  metrics: Metrics,
  prune: Prune,
  sink: S,
): { flow: Flow; value: unknown } {
  const stop = new AbortFlag()
  const adapter = new Adapter(sink, limits, abort, metrics, prune, stop)
  prepare(
    parser,
    () => !abort.isAborted() && !stop.isAborted(),
    (rule, done) => adapter.onDone(rule, done),
  )
  let parsed: { ok: true; value: unknown } | { ok: false; error: unknown }
  try {
    parsed = { ok: true, value: parser.parse(text) }
  } catch (error) {
    parsed = { ok: false, error }
  }
  let outcome: { flow: Flow } | { error: unknown } = { flow: 'continue' }
  try {
    if ('running' === adapter.status.type) {
      if (parsed.ok && adapter.complete()) {
        outcome = { flow: adapter.send(Ev.end) }
      } else if (parsed.ok && adapter.idle()) {
        let flow = adapter.walkWhole(parsed.value)
        if ('continue' === flow) flow = adapter.send(Ev.end)
        outcome = { flow }
      } else if (parsed.ok) {
        outcome = { error: notStreamable() }
      } else {
        outcome = { error: engineFailure(parsed.error, abort) }
      }
    }
  } catch (error) {
    outcome = { error }
  } finally {
    adapter.sink.flush()
  }
  const value = parsed.ok ? parsed.value : undefined
  const status = adapter.status
  if ('failed' === status.type) throw status.error
  if ('stopped' === status.type) return { flow: 'stop', value }
  if ('error' in outcome) throw outcome.error
  return { flow: outcome.flow, value }
}
