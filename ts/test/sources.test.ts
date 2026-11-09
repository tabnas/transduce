/* Copyright (c) 2026 tabnas, MIT License */

// The sources: `ParserSource`, which parses and walks, `ValueSource` and
// `Guarded`. The ports of the Rust unit tests in rs/src/source/, including
// the ones DIVERGENCE.md lists as API shapes a TSV row cannot carry (the
// abort flag, a sink that stops or fails part-way, the metrics). A source
// read from a reader is ./reader.test.ts's, and pruning ./prune.test.ts's.

import { describe, it } from 'node:test'
import assert from 'node:assert'

import { make as makeJson } from '@tabnas/json'

import {
  AbortFlag,
  Ev,
  EventRecorder,
  Fail,
  Flow,
  FnSink,
  Guarded,
  JsonEvent,
  Limits,
  Metrics,
  ParserSource,
  ValueSource,
  eventEquals,
} from '../dist/transduce'

const DOC = '{"a":[1,2.50,"x",{"b":null}],"c":{},"d":[],"e":1e21,"f":true}'

function record(src: string): { flow?: Flow; fail?: Fail; events: JsonEvent[] } {
  const rec = new EventRecorder()
  try {
    const flow = new ParserSource(makeJson(), src).run(rec)
    return { flow, events: rec.events }
  } catch (fail) {
    return { fail: fail as Fail, events: rec.events }
  }
}

function failOf(fn: () => unknown): Fail {
  try {
    fn()
  } catch (e) {
    if (e instanceof Fail) return e
    throw e
  }
  throw new Error('expected a failure')
}

function same(a: JsonEvent[], b: JsonEvent[]): boolean {
  return a.length === b.length && a.every((e, i) => eventEquals(e, b[i]))
}

function walked(value: unknown): JsonEvent[] {
  const rec = new EventRecorder()
  new ValueSource(value).run(rec)
  return rec.events
}

describe('ParserSource', () => {
  it('a root scalar is one event, then end', () => {
    assert.ok(same(record(' 42 ').events, [Ev.number(42), Ev.end]))
    assert.ok(same(record('"s"').events, [Ev.string('s'), Ev.end]))
  })

  it('the walk carries no number lexemes', () => {
    const { flow, events } = record(DOC)
    assert.equal(flow, 'continue')
    assert.deepEqual(
      events.flatMap((e) => ('number' === e.type ? [e.value] : [])),
      [1, 2.5, 1e21],
    )
    assert.ok(events.every((e) => 'number' !== e.type || null === e.lexeme))
  })

  it('a stop from the sink stops the parse', () => {
    let seen = 0
    const sink = new FnSink(() => (++seen === 3 ? 'stop' : 'continue'))
    assert.equal(new ParserSource(makeJson(), DOC).run(sink), 'stop')
    assert.equal(seen, 3)
  })

  it('a sink failure comes back unchanged', () => {
    const sink = new FnSink((ev) => {
      if ('key' === ev.type && 'c' === ev.key) throw Fail.output('disk full').atPath('.c')
    })
    const f = failOf(() => new ParserSource(makeJson(), DOC).run(sink))
    assert.equal(f.code, 'OUTPUT_FAILED')
    assert.equal(f.path, '.c')
  })

  it('an aborted flag cancels the parse as ABORTED', () => {
    const abort = new AbortFlag()
    abort.abort()
    const f = failOf(() => new ParserSource(makeJson(), DOC).abort(abort).run(new EventRecorder()))
    assert.equal(f.code, 'ABORTED')
  })

  it('an abort raised part-way through the walk ends it as ABORTED', () => {
    const abort = new AbortFlag()
    let seen = 0
    const sink = new FnSink(() => {
      if (++seen === 2) abort.abort()
    })
    const f = failOf(() => new ParserSource(makeJson(), DOC).abort(abort).run(sink))
    assert.equal(f.code, 'ABORTED')
  })

  it('a parse error is invalid input with its position', () => {
    const { fail } = record('{"a": 1,\n "b": }')
    assert.equal(fail?.code, 'INPUT_INVALID')
    assert.ok((fail?.message ?? '').startsWith('unexpected'), String(fail))
    assert.deepEqual([fail?.row, fail?.col], [2, 7])
  })

  // A guard the grammar installed (the parse budget) cancelling the parse is
  // INPUT_INVALID naming the grammar's guard, not a cancellation the caller
  // asked for. @tabnas/json installs no depth guard in TypeScript (the Rust
  // tabnas-json refuses nesting past 128; DIVERGENCE.md), so the guard here
  // is one a test installs as a grammar would.
  it("a grammar's own guard is invalid input that names the grammar", () => {
    const parser: any = makeJson()
    parser.options({
      parse: { budget: { checkEveryN: 1, onCheck: (ctx: any) => (ctx.rs?.length ?? ctx.rsI ?? 0) <= 20 && ctx.kI < 40 } },
    })
    const rec = new EventRecorder()
    const src = '['.repeat(200) + '1' + ']'.repeat(200)
    const f = failOf(() => new ParserSource(parser, src).run(rec))
    assert.equal(f.code, 'INPUT_INVALID', String(f))
    assert.ok(f.message.startsWith('the grammar stopped the parse'), f.message)
    assert.ok(f.message.includes('cancel'), f.message)
  })

  // Without a grammar guard, deep JSON reaches this package's own limit.
  it('deep JSON reaches max_depth', () => {
    const src = '['.repeat(300) + '1' + ']'.repeat(300)
    const f = record(src).fail
    assert.equal(f?.code, 'RESOURCE_LIMIT_EXCEEDED', String(f))
    assert.equal(f?.limit?.name, 'max_depth')
  })

  it('source limits apply by name', () => {
    const run = (src: string, limits: Partial<Limits>) =>
      failOf(() => new ParserSource(makeJson(), src).limits(Limits.with(limits)).run(new EventRecorder())).limit
        ?.name
    assert.equal(run('{"ab":1}', { max_key_bytes: 1 }), 'max_key_bytes')
    assert.equal(run('[[[1]]]', { max_depth: 2 }), 'max_depth')
    assert.equal(run('["abc"]', { max_scalar_bytes: 2 }), 'max_scalar_bytes')
    assert.equal(run('["日本"]', { max_scalar_bytes: 5 }), 'max_scalar_bytes')
    new ParserSource(makeJson(), '["日本"]').limits(Limits.with({ max_scalar_bytes: 6 })).run(new EventRecorder())
  })

  it('metrics count the source events', () => {
    const metrics = new Metrics()
    const rec = new EventRecorder()
    new ParserSource(makeJson(), DOC).metrics(metrics).run(rec)
    assert.equal(metrics.events, rec.events.length)
    assert.equal(metrics.keys, 6)
    assert.equal(metrics.scalars, 6)
  })

  it('a source runs once', () => {
    const source = new ParserSource(makeJson(), '[1]')
    source.run(new EventRecorder())
    assert.equal(failOf(() => source.run(new EventRecorder())).code, 'STREAM_REUSED')
  })

  it('integer-like keys keep their source order', () => {
    const src = '{"b":1,"2":2,"a":{"10":3,"1":4}}'
    const keys = record(src).events.flatMap((e) => ('key' === e.type ? [e.key] : []))
    assert.deepEqual(keys, ['b', '2', 'a', '10', '1'])
  })
})

describe('ValueSource and Guarded', () => {
  it('a value walks in document order', () => {
    const events = walked(makeJson().parse('{"a":[1,"x"],"b":null}'))
    assert.ok(
      same(events, [
        Ev.objectStart, Ev.key('a'), Ev.arrayStart, Ev.number(1), Ev.string('x'), Ev.arrayEnd,
        Ev.key('b'), Ev.null, Ev.objectEnd, Ev.end,
      ]),
    )
  })

  it('counts are flushed at end and on flush', () => {
    const metrics = new Metrics()
    const g = new Guarded(new EventRecorder(), Limits.default(), new AbortFlag(), metrics)
    for (const ev of [Ev.objectStart, Ev.key('a'), Ev.null, Ev.key('b'), Ev.string('x'), Ev.objectEnd]) g.event(ev)
    assert.equal(metrics.events, 0)
    g.event(Ev.end)
    assert.deepEqual([metrics.events, metrics.keys, metrics.scalars], [7, 2, 2])
    g.event(Ev.null)
    g.flush()
    assert.equal(metrics.events, 8)
  })

  it('each source limit fails by name, and an aborted flag stops the next event', () => {
    const guarded = (limits: Partial<Limits>, abort = new AbortFlag()) =>
      new Guarded(new EventRecorder(), Limits.with(limits), abort, new Metrics())
    let g = guarded({ max_depth: 1 })
    g.event(Ev.arrayStart)
    assert.equal(failOf(() => g.event(Ev.arrayStart)).limit?.name, 'max_depth')
    g = guarded({ max_key_bytes: 2 })
    g.event(Ev.objectStart)
    assert.equal(failOf(() => g.event(Ev.key('abc'))).limit?.name, 'max_key_bytes')
    assert.equal(failOf(() => guarded({ max_scalar_bytes: 2 }).event(Ev.string('abc'))).limit?.name, 'max_scalar_bytes')
    assert.equal(failOf(() => guarded({ max_scalar_bytes: 2 }).event(Ev.number(1.5, '1.500'))).limit?.name, 'max_scalar_bytes')
    const abort = new AbortFlag()
    g = guarded({}, abort)
    g.event(Ev.arrayStart)
    abort.abort()
    assert.equal(failOf(() => g.event(Ev.null)).code, 'ABORTED')
  })
})
