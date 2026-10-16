/* Copyright (c) 2026 tabnas, MIT License */

// `ParserEventSource`: `ParserEvents/1` from a tabnas parse of one text,
// the input a transducer part reads (rs/src/source/parser_events.rs).
//
// The source installs one `ruleDone` subscriber, which relays each rule
// pass the engine completes as a `ParserEvent`, on the thread that parses,
// into a `ParserSink`: an `open` when a rule's open pass has run, a `close`
// when its close pass has. It knows no grammar. When the engine has
// returned the parse's value it sends one `end`, so a document is complete
// only once it validated, as every source's is.
//
// What each pass carries is alchemy's `RulePass`:
//
// - the rule's `name`, its depth `d` and instance `i` (the engine's, which
//   counts from 1 here and from 0 in Go and Rust: DIVERGENCE.md 6), the
//   alternate that matched (`alt`: `p`, `r`, `b`, and its groups `g`,
//   sorted, since they are tags whose order the runtimes' engines keep
//   differently), whether recovery forced the close, and its named counters
//   `n`, sorted by name;
// - the `tokens` the pass consumed: those its alternate matched (the
//   engine's `o[:oN]` or `c[:cN]`), less the `b` it gave back from their
//   end, which are the next rule's to consume, and none when `b` is more
//   than it recorded, since a close alternate can back up a token it did
//   not record. A token is its name, its value, its source text and its
//   1-based row and column; a token with no value (the end of the source,
//   a fixed token such as `[`, which Rust's engine gives its text:
//   DIVERGENCE.md 7) has `null`, and a number carries its source text as its lexeme when
//   that text is a JSON number spelling the value (RFC 8259), the rule the
//   incremental source keeps lexemes by, so a renderer can write it as it
//   stands;
// - on a close pass, the rule's node when that is a scalar (`value`), a
//   number with the lexeme of the rule's first open token as above;
//   nothing on an open pass, nor for a container.
//
// A pass made while a probe is open is speculative: the grammar parses
// ahead and rewinds past it (tabnas-bnf's probe, the engine's own for a
// serialized grammar's `pd_` keys), so it is never the document's. The
// engine flags it (`RuleDone.probe`), and the source drops it before any
// sink sees it.
//
// The source holds its limits as it builds each pass, before the sink sees
// it: a token's value or source text, or a pass's value, longer than
// `max_scalar_bytes` is refused with that limit's name, as a JSON-events
// source refuses a scalar. `max_depth` and `max_key_bytes` hold where a
// part's outputs become `JsonEvents/1`, on the events it writes. The abort
// flag is polled per pass, and `Metrics.events` counts the events
// delivered.
//
// Failure mapping is `ParserSource`'s: a sink failure is thrown as it was;
// a sink that stopped is `'stop'`; a parse cancelled through the caller's
// `AbortFlag` is `ABORTED`; any other engine error is `INPUT_INVALID` with
// the engine's code and position. A source runs once, as `ParserSource`
// does: its parser carries the run's subscriber and budget.

import {
  AbortFlag,
  Datum,
  Fail,
  Limits,
  Metrics,
  engineScalar,
  fromTabnas,
  isJsonNumber,
  utf8Bytes,
} from '@tabnas/alchemy/shared'
import type { Flow, ParserEvent, ParserSink, PassAlt, PassToken, RulePass } from '@tabnas/alchemy/shared'
import type { Rule, RuleDone, Tabnas } from '@tabnas/parser'

import { prepare } from './rule-events'
import { engineFailure } from './source'

// A tabnas parser applied to one text, as a source of its rule passes.
export class ParserEventSource {
  private parser: Tabnas
  private text: string
  private sourceLimits: Limits = Limits.default()
  private abortFlag: AbortFlag = new AbortFlag()
  private sourceMetrics: Metrics = new Metrics()
  private used = false

  // A source with default limits, its own abort flag and fresh metrics; the
  // builder methods change each.
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

  // Parse, relaying each pass into `sink`, then `end`.
  run(sink: ParserSink): Flow {
    if (this.used) {
      throw new Fail(
        'STREAM_REUSED',
        "a ParserEventSource runs once: its parser carries the run's subscriber and budget; " +
          'make a new source, with a new parser, for another run',
      )
    }
    this.used = true
    const abort = this.abortFlag
    const stop = new AbortFlag()
    const relay = new Relay(sink, this.sourceLimits.max_scalar_bytes, abort, stop)
    prepare(
      this.parser,
      () => !abort.isAborted() && !stop.isAborted(),
      (rule, done) => relay.onDone(rule, done),
    )
    let parsed: { ok: true } | { ok: false; error: unknown }
    try {
      this.parser.parse(this.text)
      parsed = { ok: true }
    } catch (error) {
      parsed = { ok: false, error }
    }
    try {
      const status = relay.status
      if ('failed' === status.type) throw status.error
      if ('stopped' === status.type) return 'stop'
      if (!parsed.ok) throw engineFailure(parsed.error, abort)
      return relay.send({ type: 'end' })
    } finally {
      this.sourceMetrics.events += relay.events
    }
  }
}

type Status = { type: 'running' } | { type: 'stopped' } | { type: 'failed'; error: unknown }

// The subscriber's state: the sink and how the run stands.
class Relay {
  status: Status = { type: 'running' }
  events = 0

  constructor(
    readonly sink: ParserSink,
    readonly maxScalarBytes: number,
    readonly abort: AbortFlag,
    // Set to make the parse guard cancel the parse.
    readonly stop: AbortFlag,
  ) {}

  send(ev: ParserEvent): Flow {
    this.events++
    return this.sink.event(ev)
  }

  private fail(error: unknown): void {
    this.status = { type: 'failed', error }
    this.stop.abort()
  }

  // One pass the engine completed.
  onDone(rule: Rule, done: RuleDone): void {
    if (true === done.probe || 'running' !== this.status.type) return
    if (this.abort.isAborted()) {
      this.fail(Fail.aborted())
      return
    }
    const open = 'o' === done.state
    let ev: ParserEvent
    try {
      const pass = rulePass(rule, done, open, this.maxScalarBytes)
      ev = open ? { type: 'open', pass } : { type: 'close', pass }
    } catch (error) {
      this.fail(error)
      return
    }
    try {
      if ('stop' === this.send(ev)) {
        this.status = { type: 'stopped' }
        this.stop.abort()
      }
    } catch (error) {
      this.fail(error)
    }
  }
}

// The pass as alchemy's `RulePass`, its sizes held to `max_scalar_bytes`.
function rulePass(rule: Rule, done: RuleDone, open: boolean, max: number): RulePass {
  const r: any = rule
  const recorded: any[] = open ? r.o.slice(0, r.oN) : r.c.slice(0, r.cN)
  const b = null == done.alt ? 0 : done.alt.b
  const kept = recorded.slice(0, Math.max(0, recorded.length - b))
  const tokens: PassToken[] = kept.map((t: any) => {
    const src: string = 'string' === typeof t.src ? t.src : String(t.src ?? '')
    held(utf8Bytes(src), "a token's source text", max)
    const val = scalarOf(t.val, src)
    if ('string' === val.type) held(utf8Bytes(val.value), "a token's value", max)
    return { name: t.name, val, src, row: t.rI, col: t.cI }
  })
  let value: Datum | undefined
  if (!open && undefined !== r.node && undefined !== engineScalar(r.node)) {
    const first = r.o[0]
    value = scalarOf(r.node, 0 < r.oN && 'string' === typeof first?.src ? first.src : undefined)
    if ('string' === value.type) held(utf8Bytes(value.value), "a rule's value", max)
  }
  const n = Object.entries((r.n ?? {}) as Record<string, number>)
    .map(([k, v]) => [k, Math.trunc(v)] as const)
    .sort(([a], [b]) => byCodeUnit(a, b))
  const alt: PassAlt | undefined =
    null == done.alt
      ? undefined
      : { p: done.alt.p, r: done.alt.r, b: done.alt.b, g: [...done.alt.g].sort(byCodeUnit) }
  return {
    name: rule.name,
    d: rule.d,
    i: rule.i,
    ...(undefined === alt ? {} : { alt }),
    tokens,
    forced: true === done.forced,
    n,
    ...(undefined === value ? {} : { value }),
  }
}

// Strings in code-unit order, as Rust's `sort` orders ASCII names.
function byCodeUnit(a: string, b: string): number {
  return a < b ? -1 : a > b ? 1 : 0
}

// An engine value as a pass carries it: `undefined` as null, and a number
// with `lexeme` when that text is a JSON number spelling it.
export function scalarOf(value: unknown, lexeme: string | undefined): Datum {
  const scalar = engineScalar(value)
  if (undefined !== scalar && 'number' === scalar.type) {
    const keep = undefined !== lexeme && isJsonNumber(lexeme) && Number(lexeme) === scalar.value
    return Datum.number(scalar.value, keep ? lexeme : undefined)
  }
  return fromTabnas(value)
}

// `max_scalar_bytes` held on one text a pass carries.
function held(bytes: number, what: string, max: number): void {
  if (bytes > max) {
    throw Fail.limit('max_scalar_bytes', max, `${what} of ${bytes} bytes is larger than ${max}`)
  }
}
