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

  it('an abort names the row the record or chunk it was reading starts on', () => {
    // An abort lands between two of the engine's steps, where it has no
    // position of its own, so the run names the row the record (JSON Lines)
    // or chunk (CSV) it was reading starts on, and no column.
    const aborter = (abort: AbortFlag, after: number) => {
      let n = 0
      return new FnSink(() => {
        if (++n === after) abort.abort()
        return 'continue'
      })
    }
    // Raised with the second record's first event, which the incremental
    // path emits during that record's parse and the walk while it walks the
    // value: the run stops in that record, which starts on row 3. Already
    // raised: the run's first event fails, before any record is read, and
    // names no row.
    const text = '{"a":1}\n\n{"a":2}\n{"a":3}\n'
    for (const [after, row] of [[6, 3], [0, undefined]] as [number, number | undefined][]) {
      for (const path of ['run', 'runIncremental'] as const) {
        const abort = new AbortFlag()
        if (0 === after) abort.abort()
        const f = failOf(() => lines(text, LineFormat.jsonl(), (s) => s.abort(abort))[path](aborter(abort, after)))
        assert.deepEqual([f.code, f.row, f.col], ['ABORTED', row, undefined], `${path}, after ${after}: ${f}`)
      }
    }
    // CSV: the first row of the chunk, with a chunk for each record, and
    // with one chunk for the whole text.
    for (const [chunk, row] of [[0, 3], [256 * 1024, 1]]) {
      const abort = new AbortFlag()
      const f = failOf(() =>
        lines('a\n1\n2\n3\n', LineFormat.csv(), (s) => s.abort(abort).chunkBytes(chunk)).run(aborter(abort, 5)),
      )
      assert.deepEqual([f.code, f.row, f.col], ['ABORTED', row, undefined], `chunk ${chunk}: ${f}`)
    }
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

// A reading of one text: its events, or its failure as the engine's code,
// row and column.
type Reading = { events: JsonEvent[] } | { fail: [string, number | undefined, number | undefined] }

// The whole parse's reading, which the line source must reproduce.
function wholeReading(parse: () => unknown): Reading {
  try {
    return { events: walked(parse()) }
  } catch (e: any) {
    return { fail: [e.code, e.lineNumber, e.columnNumber] }
  }
}

// The line source's reading. Its failure carries the engine's code at the
// head of its message.
function lineReading(run: (rec: EventRecorder) => Flow): Reading {
  const rec = new EventRecorder()
  try {
    assert.equal(run(rec), 'continue')
    return { events: rec.events }
  } catch (f: any) {
    assert.ok(f instanceof Fail, String(f))
    assert.equal(f.code, 'INPUT_INVALID', String(f))
    return { fail: [f.message.split(':')[0], f.row, f.col] }
  }
}

function sameReading(a: Reading, b: Reading): boolean {
  if ('events' in a && 'events' in b) return same(a.events, b.events)
  if ('fail' in a && 'fail' in b) return a.fail.every((x, i) => x === b.fail[i])
  return false
}

function show(r: Reading): string {
  return JSON.stringify('events' in r ? r.events : r.fail)
}

function objects(r: Reading): number {
  assert.ok('events' in r, show(r))
  return r.events.filter((e) => 'object_start' === e.type).length
}

// Holds the CSV line source to the whole parse of `text` through the same
// grammar options, at chunk sizes from a chunk per record (0) to the whole
// text and, on the incremental path, at a few bytes a write too, and
// returns that reading.
function csvStreamsAsWhole(text: string, options: Record<string, any> = {}): Reading {
  const header = options.header ?? true
  const want = wholeReading(() => makeCsv({ ...options, header }).parse(text))
  const format = LineFormat.csv(header, options)
  const n = Buffer.byteLength(text)
  for (let chunk = 0; chunk <= n + 1; chunk++) {
    if (!(chunk < 4 || 0 === (chunk & (chunk - 1)) || chunk >= n)) continue
    const got = lineReading((rec) => lines(text, format, (s) => s.chunkBytes(chunk)).run(rec))
    assert.ok(sameReading(got, want), `${JSON.stringify(text)}, chunk ${chunk}: ${show(got)}, not ${show(want)}`)
  }
  for (const step of [1, 3]) {
    const got = lineReading((rec) =>
      lines(trickle(text, step), format, (s) => s.chunkBytes(0)).runIncremental(rec),
    )
    assert.ok(sameReading(got, want), `${JSON.stringify(text)}, step ${step}: ${show(got)}, not ${show(want)}`)
  }
  return want
}

// Holds the JSON Lines source to the whole parse of `text` by the JSON
// Lines grammar, on both paths, whole and a few bytes a write, and returns
// that reading.
function jsonlStreamsAsWhole(text: string): Reading {
  const want = wholeReading(() => makeJsonl().parse(text))
  for (const step of [0, 1, 3]) {
    const input = () => (0 === step ? text : trickle(text, step))
    const walk = lineReading((rec) => lines(input(), LineFormat.jsonl()).run(rec))
    assert.ok(sameReading(walk, want), `walk, ${JSON.stringify(text)}, step ${step}: ${show(walk)}`)
    const inc = lineReading((rec) => lines(input(), LineFormat.jsonl()).runIncremental(rec))
    const bare = 'events' in inc ? { events: withoutLexemes(inc.events) } : inc
    assert.ok(sameReading(bare, want), `incremental, ${JSON.stringify(text)}, step ${step}: ${show(inc)}`)
  }
  return want
}

// tabnas-csv's vendored corpus file
// `papa-misplaced-quotes-in-data-twice-not-as-opening-quotes.csv`, byte for
// byte. A quote inside a field is that field's text, not the start of a
// quoted field, so the grammar reads two lines, the header and a record.
const MISPLACED_QUOTES = 'A,B",C\nD,E",F'

describe('LinesSource: records end where the grammar ends them', () => {
  it('quotes inside a field are read as the grammar reads them', () => {
    assert.equal(objects(csvStreamsAsWhole(MISPLACED_QUOTES)), 1)
    // The corpus's own options: no header, each record an array.
    const corpus = { header: false, object: false }
    csvStreamsAsWhole(MISPLACED_QUOTES, corpus)
    const more = `${MISPLACED_QUOTES}\nG,H,I\nJ"K,L,"M\nN"\n`
    assert.equal(objects(csvStreamsAsWhole(more)), 3)
    csvStreamsAsWhole(more, corpus)
  })

  it('an input ending inside a quoted field fails as the grammar does', () => {
    // In the header, which a header-only chunk never parsed, and in a
    // record, with and without a newline after it.
    for (const text of ['a,"b\n', 'a,"b', 'a,"b\nc,d\n', '"\n', 'a\n"x\n', 'a\n1\n"x']) {
      const want = csvStreamsAsWhole(text)
      assert.ok('fail' in want && 'unterminated_string' === want.fail[0], `${JSON.stringify(text)}: ${show(want)}`)
    }
  })

  it('a last record without a newline is read', () => {
    for (const text of ['a,b\n1,2\n3,4', 'a,b\n1,2\n3,"x\ny"', 'a,b\r\n1,2\r\n3,4']) {
      assert.equal(objects(csvStreamsAsWhole(text)), 2, JSON.stringify(text))
    }
  })

  it('the header is the first record the grammar reads', () => {
    const empty = { record: { empty: true } }
    const relaxed = { strict: false }
    const comment = { comment: true }
    const cases: [string, Record<string, any>, number][] = [
      // A blank line before it is none of it...
      ['\n\na,b\n1,2\n3,4\n', {}, 2],
      ['\r\na,b\r\n1,2\r\n3,4\r\n', {}, 2],
      // ...unless a blank line is a record, when it is the header.
      ['\na,b\n\n1,2\n', empty, 3],
      // A line of spaces is the header in strict mode, blank otherwise.
      ['  \na,b\n1,2\n3,4\n', {}, 3],
      ['  \na,b\n1,2\n3,4\n', relaxed, 2],
      // A comment is no record, and a quote inside one no quote.
      ['# x "\na,b\n1,2\n3,4\n', comment, 2],
      ['// x "\na,b\n1,2\n3,4\n', relaxed, 2],
      ['a,b # c "\n1,2\n3,4\n', comment, 2],
    ]
    for (const [text, options, records] of cases) {
      assert.equal(objects(csvStreamsAsWhole(text, options)), records, JSON.stringify(text))
    }
  })

  it('a lone carriage return ends a CSV record', () => {
    for (const text of ['a,b\r1,2\r3,4\r', 'a,b\r1,2\n3,4\n5,6\n']) {
      assert.ok(2 <= objects(csvStreamsAsWhole(text)), JSON.stringify(text))
    }
  })

  it('a field spans lines only where the grammar reads one that does', () => {
    const quote = { string: { quote: "'" } }
    const tildes = { field: { separation: '~~' } }
    const comment = { comment: true }
    const cases: [string, Record<string, any>][] = [
      // The engine's own strings: a backtick spans lines, and an escaped
      // newline continues a single-quoted one, which a `"` inside does not
      // end.
      ['a,b\n`x\ny`,z\n1,2\n', {}],
      ["a,b\n'x\\\ny',z\n1,2\n", {}],
      ['a,b\n\'x,"y\',z\n"p\nq",r\n', {}],
      // A quote after a space, or after a separator of the grammar's.
      ['a,b\nx, "p\nq"\n1,2\n', {}],
      ['a~~b\nx~~"p\nq"\n1~~2\n', tildes],
      // The configured quote, and a `"` that is then the engine's, which a
      // line refuses.
      ["a,b\n'x\ny',z\n1,2\n", quote],
      ['a,b\n"x,y\n1,2\n', quote],
      // A block comment over lines.
      ['a,b\n/* x\ny */1,2\n3,4\n', comment],
    ]
    for (const [text, options] of cases) csvStreamsAsWhole(text, options)
  })

  it('configured record separators end records', () => {
    const semicolons = { record: { separators: ';' } }
    assert.equal(objects(csvStreamsAsWhole('a,b;1,x;3,4;5,6', semicolons)), 3)
    // A newline is then no line ending, and never a place to cut. (This
    // grammar refuses it in a field, where the Rust one reads it as text.)
    csvStreamsAsWhole('a,b;1,x\ny;3,4\nz;5,6', semicolons)
  })

  it('a JSON Lines line is blank only when the grammar reads it so', () => {
    // Space and tab are the grammar's blanks; a form feed, a vertical tab,
    // a no-break space or a line separator is not, and the grammar refuses
    // the line rather than skipping it.
    for (const blank of ['\f', '\v', ' ', '\u0085', ' ', '　']) {
      const want = jsonlStreamsAsWhole(`{"a":1}\n${blank}\n{"b":2}\n`)
      assert.ok('fail' in want && 2 === want.fail[1], `${JSON.stringify(blank)} is refused on its line: ${show(want)}`)
    }
    assert.equal(objects(jsonlStreamsAsWhole('{"a":1}\n \t\n\r\n{"b":2}\n   ')), 2)
  })

  it('a lone carriage return ends a JSON Lines record', () => {
    assert.equal(objects(jsonlStreamsAsWhole('{"a":1}\r{"b":2}\r\r \r{"c":3}\n')), 3)
    // A value cut by one is incomplete, as the grammar reads it, and the
    // position of a failure after one is the grammar's.
    for (const text of ['{"a":\r1}\n', '{"a":1}\r{"b": }\n']) {
      assert.ok('fail' in jsonlStreamsAsWhole(text), JSON.stringify(text))
    }
    // Inside a string it is the string's (refused there, as a control
    // character), not a record's end.
    assert.ok('fail' in jsonlStreamsAsWhole('{"a":"x\ry"}\n'))
  })

  it('records one line holds are bounded and cut one by one', () => {
    // Many records and no `\n`: records ended by a lone `\r`, by a
    // configured separator, and JSON Lines records ended by `\r`. Each
    // record is under the limit and the line far over it, so the limit is
    // the record's, and a chunk closes after a record rather than after the
    // line.
    const limits = Limits.with({ max_record_bytes: 8 })
    const crs = `a,b\r${'1,2\r'.repeat(200)}`
    const semicolons = { record: { separators: ';' } }
    const separated = `a,b;${'1,x;'.repeat(200)}`
    for (const [text, options] of [[crs, {}], [separated, semicolons]] as [string, Record<string, any>][]) {
      const want = wholeReading(() => makeCsv(options).parse(text))
      assert.equal(objects(want), 200)
      for (const chunk of [0, 64, 256 * 1024]) {
        const got = lineReading((rec) =>
          lines(text, LineFormat.csv(true, options), (s) => s.limits(limits).chunkBytes(chunk)).run(rec),
        )
        assert.ok(sameReading(got, want), `${JSON.stringify(text.slice(0, 12))}, chunk ${chunk}: ${show(got).slice(0, 200)}`)
      }
      // The first half of the line in, records are already out: chunks
      // close inside it.
      const rec = new EventRecorder()
      const writer = new LinesSource(null, LineFormat.csv(true, options))
        .limits(limits)
        .chunkBytes(64)
        .writer(rec)
      writer.write(text.slice(0, text.length / 2))
      assert.ok(rec.events.filter((e) => 'object_start' === e.type).length > 50, `${rec.events.length} events`)
      writer.write(text.slice(text.length / 2))
      writer.end()
      assert.ok(same(rec.events, (want as { events: JsonEvent[] }).events))
    }
    const records = '{"a":1}\r'.repeat(200)
    const want = wholeReading(() => makeJsonl().parse(records))
    assert.equal(objects(want), 200)
    assert.ok(sameReading(lineReading((rec) => lines(records, LineFormat.jsonl(), (s) => s.limits(limits)).run(rec)), want))
    const inc = lineReading((rec) => lines(records, LineFormat.jsonl(), (s) => s.limits(limits)).runIncremental(rec))
    assert.ok(sameReading('events' in inc ? { events: withoutLexemes(inc.events) } : inc, want))
    // A record over the limit still fails, at the row it starts on, which is
    // counted as the engine counts rows: by `\n`, or by the configured
    // separator.
    for (const [text, options, row] of [
      ['a,b\r1,2\r123456789,x\r', {}, 1],
      ['a,b;1,x;123456789,x;', semicolons, 3],
    ] as [string, Record<string, any>, number][]) {
      const f = failOf(() => lines(text, LineFormat.csv(true, options), (s) => s.limits(limits)).run(new EventRecorder()))
      assert.equal(f.limit?.name, 'max_record_bytes')
      assert.equal(f.row, row, `${JSON.stringify(text)}: ${f}`)
    }
  })

  it('a line token of two characters is never cut', () => {
    // Under `record.empty` a run of line characters ends at a repeated one,
    // so `\r\n`, and `\n\r` as much, is one line token. A chunk cut between
    // its two characters would start with a line token, a record of its own
    // to a chunk without a header; and where a write ends between the two,
    // the piece waits for the next.
    for (const header of [false, true]) {
      const options = { record: { empty: true }, header }
      for (const text of ['a,b\r\n1,2\r\n\r\n3,4\r\n', 'a,b\n\r1,2\n\r\n\r3,4', 'a\r\n\r\r\nb\r\n']) {
        csvStreamsAsWhole(text, options)
      }
    }
  })

  it("a line character inside a JSON Lines string is the string's", () => {
    // The grammar refuses a string at a raw line character, and the record
    // is read whole up to the line character after it, so the failure is
    // the grammar's: a `\n` in a string ends a record no more than a `\r`
    // does.
    for (const text of ['{"a":"x\ny"}\n{"b":1}\n', '["\\\n"]\n', '{"a":"x\r\ny"}\r\n']) {
      assert.ok('fail' in jsonlStreamsAsWhole(text), JSON.stringify(text))
    }
  })

  it('a line character inside a fixed token ends no piece', () => {
    // The lexer reads a fixed token before a line, so a field separator
    // that holds a line character owns it, and a piece does not end there.
    // Where a fixed token holds one, a run of line characters is one token
    // to the lexer, and a piece takes the run whole, so that no chunk starts
    // inside it.
    const starts = { field: { separation: '\n~' } }
    const inside = { field: { separation: '~\n~' } }
    for (const [text, options] of [
      ['a\n~b\nx\n~y\n', starts],
      ['a\n~b\n\n~c\nx\n~y\n', starts],
      ['a\n~b\r\n~c\nx\n~y', starts],
      ['a~\n~b\nx~\n~y\n', inside],
    ] as [string, Record<string, any>][]) {
      csvStreamsAsWhole(text, options)
    }
  })

  it('a separator of several bytes is found across writes', () => {
    // A configured separator outside ASCII is matched on its whole UTF-8
    // form, written a byte at a time too; under `record.empty` a token of
    // two of them is followed across writes, and a character that only
    // starts like one is left to the next record.
    csvStreamsAsWhole('a,b␞1,é␞3,4␞', { record: { separators: '␞' } })
    csvStreamsAsWhole('a,b␞¶£,é␞£,2¶␞3,4', { record: { separators: '␞¶', empty: true }, header: false })
    // Outside the basic plane too, which the engine keeps as two UTF-16
    // halves, each a line and a row character: a failure after one is
    // placed as the grammar places it.
    const astral = { record: { separators: '😀' } }
    assert.equal(objects(csvStreamsAsWhole('a,b😀1,é😀3,4😀', astral)), 2)
    assert.ok('fail' in csvStreamsAsWhole('a,b😀1,2😀3,"x', astral))
    // Each record is under the limit and the line far over it.
    const text = `a,b😀${'1,2😀'.repeat(100)}`
    const want = wholeReading(() => makeCsv(astral).parse(text))
    const got = lineReading((rec) =>
      lines(text, LineFormat.csv(true, astral), (s) => s.limits(Limits.with({ max_record_bytes: 8 })).chunkBytes(64)).run(rec),
    )
    assert.ok(sameReading(got, want), show(got).slice(0, 200))
  })
})
