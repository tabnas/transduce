/* Copyright (c) 2026 tabnas, MIT License */

// The sources: `ParserSource` in both modes, `LinesSource` on both paths
// and through its push writer, `Guarded`, and pruning. The ports of the
// Rust unit tests in rs/src/source/, including the ones DIVERGENCE.md
// lists as API shapes a TSV row cannot carry (input that is not UTF-8, a
// reader that hands out one byte at a time or never ends, the abort flag,
// a sink that stops or fails part-way, the metrics).

import { describe, it } from 'node:test'
import assert from 'node:assert'

import { make as makeJson } from '@tabnas/json'
import { make as makeJsonl } from '@tabnas/jsonl'
import { make as makeCsv } from '@tabnas/csv'

import {
  AbortFlag,
  Datum,
  Ev,
  EventRecorder,
  Fail,
  Flow,
  FnSink,
  Guarded,
  JsonEvent,
  LineFormat,
  Limits,
  LinesSource,
  Metrics,
  ParserSource,
  Prune,
  Selector,
  SourceMode,
  ValueSource,
  eventEquals,
  isScalar,
  toText,
} from '../dist/transduce'

const DOC = '{"a":[1,2.50,"x",{"b":null}],"c":{},"d":[],"e":1e21,"f":true}'

function record(mode: SourceMode, src: string): { flow?: Flow; fail?: Fail; events: JsonEvent[] } {
  const rec = new EventRecorder()
  try {
    const flow = new ParserSource(makeJson(), src).grammar('json').mode(mode).run(rec)
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

function withoutLexemes(events: JsonEvent[]): JsonEvent[] {
  return events.map((e) => ('number' === e.type ? Ev.number(e.value) : e))
}

function same(a: JsonEvent[], b: JsonEvent[]): boolean {
  return a.length === b.length && a.every((e, i) => eventEquals(e, b[i]))
}

function walked(value: unknown): JsonEvent[] {
  const rec = new EventRecorder()
  new ValueSource(value).run(rec)
  return rec.events
}

const incremental = () => SourceMode.incremental()
const BOTH = () => [SourceMode.materialize(), incremental()]

describe('ParserSource', () => {
  it('incremental mode needs a verified grammar name and emits nothing without one', () => {
    let rec = new EventRecorder()
    let f = failOf(() => new ParserSource(makeJson(), DOC).mode(incremental()).run(rec))
    assert.equal(f.code, 'STREAMABILITY_UNKNOWN')
    assert.ok(f.message.includes('ParserSource.grammar'), f.message)
    assert.equal(rec.events.length, 0)

    rec = new EventRecorder()
    f = failOf(() => new ParserSource(makeCsv(), 'a,b\n1,2\n').grammar('csv').mode(incremental()).run(rec))
    assert.equal(f.code, 'STREAMABILITY_UNKNOWN')
    assert.ok(f.message.includes('"csv"'), f.message)
    assert.equal(rec.events.length, 0, 'refused before the parse')

    // Materialize needs no name, and the unverified switch lifts the gate:
    // csv then runs, and the adapter refuses it as it meets how the grammar
    // builds its records, after a prefix and before end.
    rec = new EventRecorder()
    new ParserSource(makeCsv(), 'a,b\n1,2\n').run(rec)
    assert.equal(rec.events[rec.events.length - 1].type, 'end')
    rec = new EventRecorder()
    f = failOf(() => new ParserSource(makeCsv(), 'a,b\n1,2\n').unverified().mode(incremental()).run(rec))
    assert.equal(f.code, 'STREAMABILITY_UNKNOWN')
    assert.ok(f.message.includes('the incremental source cannot follow a grammar that builds'), f.message)
    assert.ok(0 < rec.events.length, 'refused during the parse, not before it')
    assert.ok(!rec.events.some((e) => 'end' === e.type))
  })

  it('incremental events equal the walk and carry lexemes', () => {
    const inc = record(incremental(), DOC)
    const mat = record(SourceMode.materialize(), DOC)
    assert.equal(inc.flow, 'continue')
    assert.equal(mat.flow, 'continue')
    assert.ok(same(withoutLexemes(inc.events), mat.events))
    assert.deepEqual(
      inc.events.flatMap((e) => ('number' === e.type ? [e.lexeme] : [])),
      ['1', '2.50', '1e21'],
    )
    assert.ok(mat.events.every((e) => 'number' !== e.type || null === e.lexeme))
  })

  it('a root scalar is one event, then end', () => {
    assert.ok(same(record(incremental(), ' 42 ').events, [Ev.number(42, '42'), Ev.end]))
    assert.ok(same(record(incremental(), '"s"').events, [Ev.string('s'), Ev.end]))
  })

  it('a stop from the sink stops the parse', () => {
    for (const mode of BOTH()) {
      let seen = 0
      const sink = new FnSink(() => (++seen === 3 ? 'stop' : 'continue'))
      assert.equal(new ParserSource(makeJson(), DOC).grammar('json').mode(mode).run(sink), 'stop')
      assert.equal(seen, 3)
    }
  })

  it('a sink failure comes back unchanged', () => {
    for (const mode of BOTH()) {
      const sink = new FnSink((ev) => {
        if ('key' === ev.type && 'c' === ev.key) throw Fail.output('disk full').atPath('.c')
      })
      const f = failOf(() => new ParserSource(makeJson(), DOC).grammar('json').mode(mode).run(sink))
      assert.equal(f.code, 'OUTPUT_FAILED')
      assert.equal(f.path, '.c')
    }
  })

  it('an aborted flag cancels the parse as ABORTED', () => {
    for (const mode of BOTH()) {
      const abort = new AbortFlag()
      abort.abort()
      const f = failOf(() =>
        new ParserSource(makeJson(), DOC).grammar('json').mode(mode).abort(abort).run(new EventRecorder()),
      )
      assert.equal(f.code, 'ABORTED')
    }
  })

  it('an abort raised part-way through the parse cancels it as ABORTED', () => {
    for (const mode of BOTH()) {
      const abort = new AbortFlag()
      let seen = 0
      const sink = new FnSink(() => {
        if (++seen === 2) abort.abort()
      })
      const f = failOf(() =>
        new ParserSource(makeJson(), DOC).grammar('json').mode(mode).abort(abort).run(sink),
      )
      assert.equal(f.code, 'ABORTED')
    }
  })

  it('a parse error is invalid input with its position', () => {
    for (const mode of BOTH()) {
      const { fail } = record(mode, '{"a": 1,\n "b": }')
      assert.equal(fail?.code, 'INPUT_INVALID')
      assert.ok((fail?.message ?? "").startsWith('unexpected'), String(fail))
      assert.deepEqual([fail?.row, fail?.col], [2, 7])
    }
  })

  // A guard the grammar installed (the parse budget) cancelling the parse is
  // INPUT_INVALID naming the grammar's guard, not a cancellation the caller
  // asked for. @tabnas/json installs no depth guard in TypeScript (the Rust
  // tabnas-json refuses nesting past 128; DIVERGENCE.md), so the guard here
  // is one a test installs as a grammar would.
  it("a grammar's own guard is invalid input that names the grammar", () => {
    for (const mode of BOTH()) {
      const parser: any = makeJson()
      parser.options({
        parse: { budget: { checkEveryN: 1, onCheck: (ctx: any) => (ctx.rs?.length ?? ctx.rsI ?? 0) <= 20 && ctx.kI < 40 } },
      })
      const rec = new EventRecorder()
      const src = '['.repeat(200) + '1' + ']'.repeat(200)
      const f = failOf(() => new ParserSource(parser, src).grammar('json').mode(mode).run(rec))
      assert.equal(f.code, 'INPUT_INVALID', String(f))
      assert.ok(f.message.startsWith('the grammar stopped the parse'), f.message)
      assert.ok(f.message.includes('cancel'), f.message)
    }
  })

  // Without a grammar guard, deep JSON reaches this package's own limit.
  it('deep JSON reaches max_depth in both modes', () => {
    const src = '['.repeat(300) + '1' + ']'.repeat(300)
    for (const mode of BOTH()) {
      const f = record(mode, src).fail
      assert.equal(f?.code, 'RESOURCE_LIMIT_EXCEEDED', String(f))
      assert.equal(f?.limit?.name, 'max_depth')
    }
  })

  it('source limits apply in both modes by name', () => {
    for (const mode of BOTH()) {
      const run = (src: string, limits: Partial<Limits>) =>
        failOf(() =>
          new ParserSource(makeJson(), src).grammar('json').mode(mode).limits(Limits.with(limits)).run(new EventRecorder()),
        ).limit?.name
      assert.equal(run('{"ab":1}', { max_key_bytes: 1 }), 'max_key_bytes')
      assert.equal(run('[[[1]]]', { max_depth: 2 }), 'max_depth')
      assert.equal(run('["abc"]', { max_scalar_bytes: 2 }), 'max_scalar_bytes')
      assert.equal(run('["日本"]', { max_scalar_bytes: 5 }), 'max_scalar_bytes')
      new ParserSource(makeJson(), '["日本"]').grammar('json').mode(mode).limits(Limits.with({ max_scalar_bytes: 6 })).run(new EventRecorder())
    }
  })

  it('metrics count the source events', () => {
    for (const mode of BOTH()) {
      const metrics = new Metrics()
      const rec = new EventRecorder()
      new ParserSource(makeJson(), DOC).grammar('json').mode(mode).metrics(metrics).run(rec)
      assert.equal(metrics.events, rec.events.length)
      assert.equal(metrics.keys, 6)
      assert.equal(metrics.scalars, 6)
    }
  })

  it('pruning leaves the events untouched and empties the streamed arrays in the engine value only', () => {
    const plain = record(incremental(), DOC).events
    for (const prune of [Prune.allArrays(), Prune.under(Selector.root().property('a').eachIndex()), Prune.under(Selector.root().property('a'))]) {
      assert.ok(same(record(SourceMode.incremental(prune), DOC).events, plain))
    }
    const src = '{"rows":[{"a":1},{"a":2}],"keep":[1,2,3],"n":{"rows":[[1],[2]]}}'
    const tree = (prune: Prune) => {
      const { value } = new ParserSource(makeJson(), src)
        .grammar('json')
        .mode(SourceMode.incremental(prune))
        .runWithValue(new EventRecorder())
      return toText(Datum.fromTabnas(value))
    }
    assert.equal(tree(Prune.never()), src)
    assert.equal(tree(Prune.under(Selector.root().property('rows').eachIndex())), '{"rows":[],"keep":[1,2,3],"n":{"rows":[[1],[2]]}}')
    assert.equal(tree(Prune.allArrays()), '{"rows":[],"keep":[],"n":{"rows":[]}}')
    assert.equal(tree(Prune.under(Selector.root().property('n').property('rows'))), '{"rows":[{"a":1},{"a":2}],"keep":[1,2,3],"n":{"rows":[]}}')
  })

  it('a source runs once', () => {
    const source = new ParserSource(makeJson(), '[1]').grammar('json')
    source.run(new EventRecorder())
    assert.equal(failOf(() => source.run(new EventRecorder())).code, 'STREAM_REUSED')
    const l = new LinesSource('[1]\n', LineFormat.jsonl())
    l.run(new EventRecorder())
    assert.equal(failOf(() => l.runIncremental(new EventRecorder())).code, 'STREAM_REUSED')
  })

  it('integer-like keys keep their source order in both modes', () => {
    const src = '{"b":1,"2":2,"a":{"10":3,"1":4}}'
    for (const mode of BOTH()) {
      const keys = record(mode, src).events.flatMap((e) => ('key' === e.type ? [e.key] : []))
      assert.deepEqual(keys, ['b', '2', 'a', '10', '1'])
    }
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

// The input in pieces of at most `step` bytes, so every chunk boundary the
// line reader could meet is met.
function* trickle(text: string, step: number): Generator<Uint8Array> {
  const bytes = Buffer.from(text, 'utf8')
  for (let i = 0; i < bytes.length; i += step) yield bytes.subarray(i, i + step)
}

const JSONL = '{"a":1.50,"b":[true,null]}\r\n\n  \n{"a":2,"b":"x"}\n[3]\n"s"'
const CSV = 'id,note,n\r\n1,"multi\r\nline, with ""quotes""",2.50\r\n3,plain,4\r\n\r\n5,"a",6'

function lines(input: any, format: LineFormat, configure?: (s: LinesSource) => void) {
  const source = new LinesSource(input, format)
  configure?.(source)
  return source
}

describe('LinesSource', () => {
  it('JSON Lines matches the whole-input parse at every chunk boundary, on both paths', () => {
    const want = walked(makeJsonl().parse(JSONL))
    for (let step = 1; step <= Buffer.byteLength(JSONL); step++) {
      const rec = new EventRecorder()
      lines(trickle(JSONL, step), LineFormat.jsonl()).run(rec)
      assert.ok(same(rec.events, want), `walk, step ${step}`)
      const inc = new EventRecorder()
      assert.equal(lines(trickle(JSONL, step), LineFormat.jsonl()).runIncremental(inc), 'continue')
      assert.ok(same(withoutLexemes(inc.events), want), `incremental, step ${step}`)
      assert.ok(inc.events.some((e) => eventEquals(e, Ev.number(1.5, '1.50'))), 'the incremental path keeps lexemes')
    }
  })

  it('the push writer is the iterable path', () => {
    const want = walked(makeJsonl().parse(JSONL))
    const rec = new EventRecorder()
    const writer = new LinesSource(null, LineFormat.jsonl()).writer(rec)
    for (const chunk of trickle(JSONL, 5)) assert.equal(writer.write(chunk), 'continue')
    assert.equal(writer.end(), 'continue')
    assert.ok(same(rec.events, want))
    // Text chunks may split a multibyte character only as bytes; as text
    // they arrive whole.
    const text = new EventRecorder()
    const w = new LinesSource(null, LineFormat.jsonl()).writer(text, true)
    w.write('{"é":"日')
    w.write('本"}\n')
    w.end()
    assert.ok(same(text.events, [Ev.arrayStart, Ev.objectStart, Ev.key('é'), Ev.string('日本'), Ev.objectEnd, Ev.arrayEnd, Ev.end]))
    assert.throws(() => w.write('x'), (e: any) => 'PROTOCOL_ORDER_ERROR' === e.code)
  })

  it('a bad JSON Lines line names its line number', () => {
    const text = '{"a":1}\n\n{"a": }\n{"a":2}\n'
    let f = failOf(() => lines(text, LineFormat.jsonl()).run(new EventRecorder()))
    assert.equal(f.code, 'INPUT_INVALID')
    assert.deepEqual([f.row, f.col], [3, 7])
    f = failOf(() => lines(text, LineFormat.jsonl()).runIncremental(new EventRecorder()))
    assert.equal(f.code, 'INPUT_INVALID')
    assert.equal(f.row, 3)
  })

  it('empty input is an empty array for both formats', () => {
    for (const format of [LineFormat.jsonl(), LineFormat.csv()]) {
      const rec = new EventRecorder()
      lines('', format).run(rec)
      assert.ok(same(rec.events, [Ev.arrayStart, Ev.arrayEnd, Ev.end]))
      const blank = new EventRecorder()
      lines('\n\n', format).runIncremental(blank)
      assert.equal(blank.events.length, 3)
    }
  })

  it('CSV matches the whole-input parse at every chunk size and chunk boundary', () => {
    const want = walked(makeCsv().parse(CSV))
    assert.equal(want.filter((e) => 'object_start' === e.type).length, 3)
    for (let chunk = 0; chunk <= Buffer.byteLength(CSV) + 1; chunk++) {
      const rec = new EventRecorder()
      lines(CSV, LineFormat.csv(), (s) => s.chunkBytes(chunk)).run(rec)
      assert.ok(same(rec.events, want), `chunk ${chunk}`)
    }
    for (let step = 1; step <= Buffer.byteLength(CSV); step++) {
      const rec = new EventRecorder()
      lines(trickle(CSV, step), LineFormat.csv(), (s) => s.chunkBytes(7)).runIncremental(rec)
      assert.ok(same(rec.events, want), `step ${step}`)
    }
  })

  it("CSV without a header yields the grammar's records", () => {
    const text = '1,2\n3,"4\n5"\n'
    for (const object of [true, false]) {
      const want = walked(makeCsv({ header: false, object }).parse(text))
      for (let chunk = 0; chunk <= text.length; chunk++) {
        const rec = new EventRecorder()
        lines(text, LineFormat.csv(false, { object }), (s) => s.chunkBytes(chunk)).run(rec)
        assert.ok(same(rec.events, want), `object ${object}, chunk ${chunk}`)
      }
    }
  })

  it('a header-only file is an empty table, and a bad record names its file line', () => {
    const rec = new EventRecorder()
    lines('a,b\n', LineFormat.csv()).run(rec)
    assert.equal(rec.events.length, 3)
    const text = 'a,b\n1,2\n3,"x\n4,5\n'
    let f = failOf(() => lines(text, LineFormat.csv(), (s) => s.chunkBytes(0)).run(new EventRecorder()))
    assert.equal(f.code, 'INPUT_INVALID')
    assert.equal(f.row, 3, String(f))
    f = failOf(() => lines(text, LineFormat.csv()).run(new EventRecorder()))
    assert.equal(f.row, 3, String(f))
  })

  it('an unterminated line is refused at the limit, not after being read whole', () => {
    const limits = Limits.with({ max_record_bytes: 8 })
    for (const format of [LineFormat.jsonl(), LineFormat.csv()]) {
      let pulled = 0
      function* endless(): Generator<Uint8Array> {
        const block = Buffer.alloc(4096, 'a')
        for (;;) {
          pulled += block.length
          yield block
        }
      }
      const f = failOf(() => lines(endless(), format, (s) => s.limits(limits)).run(new EventRecorder()))
      assert.equal(f.limit?.name, 'max_record_bytes')
      assert.equal(f.row, 1)
      assert.ok(pulled <= 8 + 4096, `${format.type}: ${pulled} bytes were pulled from an endless line`)
    }
  })

  it('an oversized record names max_record_bytes at its line, the line ending counting', () => {
    const limits = Limits.with({ max_record_bytes: 8 })
    let f = failOf(() => lines('{"a":1}\n{"a":123456}\n', LineFormat.jsonl(), (s) => s.limits(limits)).run(new EventRecorder()))
    assert.equal(f.limit?.name, 'max_record_bytes')
    assert.equal(f.row, 2)
    f = failOf(() => lines('a\n"long\nquoted\nfield"\n', LineFormat.csv(), (s) => s.limits(limits)).run(new EventRecorder()))
    assert.equal(f.limit?.name, 'max_record_bytes')
    assert.equal(f.row, 2)
    // Seven bytes and a newline pass; with `\r\n` the same record fails.
    lines('"abcde"\n', LineFormat.jsonl(), (s) => s.limits(limits)).run(new EventRecorder())
    f = failOf(() => lines('"abcde"\r\n', LineFormat.jsonl(), (s) => s.limits(limits)).run(new EventRecorder()))
    assert.equal(f.limit?.name, 'max_record_bytes')
  })

  it('a stop and an abort end the run on both paths', () => {
    const text = '{"a":1}\n{"a":2}\n{"a":3}\n'
    const stopper = () => {
      let n = 0
      return new FnSink(() => (++n === 4 ? 'stop' : 'continue'))
    }
    assert.equal(lines(text, LineFormat.jsonl()).run(stopper()), 'stop')
    assert.equal(lines(text, LineFormat.jsonl()).runIncremental(stopper()), 'stop')
    const abort = new AbortFlag()
    abort.abort()
    assert.equal(failOf(() => lines(text, LineFormat.jsonl(), (s) => s.abort(abort)).run(new EventRecorder())).code, 'ABORTED')
    assert.equal(failOf(() => lines(text, LineFormat.jsonl(), (s) => s.abort(abort)).runIncremental(new EventRecorder())).code, 'ABORTED')
    // A writer that stopped stays stopped.
    const w = new LinesSource(null, LineFormat.jsonl()).writer(stopper())
    assert.equal(w.write(text), 'stop')
    assert.equal(w.write(text), 'stop')
    assert.equal(w.end(), 'stop')
  })

  it('input that is not UTF-8 is invalid input at its line and column', () => {
    const bytes = Buffer.concat([Buffer.from('{"a":1}\n{"a":"'), Buffer.from([0xff]), Buffer.from('"}\n')])
    for (const path of ['run', 'runIncremental'] as const) {
      const f = failOf(() => lines(bytes, LineFormat.jsonl())[path](new EventRecorder()))
      assert.equal(f.code, 'INPUT_INVALID')
      assert.deepEqual([f.row, f.col], [2, 7])
    }
    const csv = Buffer.concat([Buffer.from('a\nx'), Buffer.from([0xc3]), Buffer.from('\n')])
    const f = failOf(() => lines(csv, LineFormat.csv()).run(new EventRecorder()))
    assert.deepEqual([f.code, f.row, f.col], ['INPUT_INVALID', 2, 2])
  })

  it('metrics count every line on both paths', () => {
    for (const path of ['run', 'runIncremental'] as const) {
      const metrics = new Metrics()
      const rec = new EventRecorder()
      lines(JSONL, LineFormat.jsonl(), (s) => s.metrics(metrics))[path](rec)
      assert.equal(metrics.events, rec.events.length, path)
      assert.equal(metrics.keys, rec.events.filter((e) => 'key' === e.type).length)
      assert.equal(metrics.scalars, rec.events.filter(isScalar).length)
    }
  })
})
