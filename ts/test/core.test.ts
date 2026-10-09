/* Copyright (c) 2026 tabnas, MIT License */

// The unit tests of the protocol, value, selector, matcher, router, table
// and scan modules: the ports of the Rust unit tests that pin behaviour the
// shared fixtures do not reach (API shapes, failure messages and paths,
// metrics, byte accounting).

import { describe, it } from 'node:test'
import assert from 'node:assert'

import {
  CaptureSpec,
  Cell,
  Code,
  CountSink,
  Datum,
  DatumBuilder,
  Duplicates,
  Ev,
  EventRecorder,
  Fail,
  FnSink,
  JsonEvent,
  Limits,
  Matcher,
  Metrics,
  NODE_BYTES,
  Path,
  RouteSink,
  Router,
  ScanEmit,
  Schema,
  Selected,
  SelectedRecorder,
  Selector,
  Table,
  TableBinding,
  TableFromJson,
  Transition,
  TreeContract,
  boundColumn,
  columnFromMeta,
  eventEquals,
  isEnd,
  isScalar,
  isStart,
  jsonString,
  numberText,
  replay,
  toText,
  utf8Bytes,
  walkDatum,
} from '../dist/transduce'

const root = () => Selector.root()

// A JSON text's events, walked from a datum (numbers with their shortest
// text as lexeme), ending with `end`.
function doc(json: string): JsonEvent[] {
  const rec = new EventRecorder()
  walkDatum(Datum.fromJSON(JSON.parse(json)), rec)
  rec.event(Ev.end)
  return rec.events
}

function codeOf(fn: () => unknown): string {
  try {
    fn()
  } catch (e: any) {
    return e.code
  }
  return 'NO_FAILURE'
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

// The spec's worked example: metadata first, then records whose member
// order differs, one number without a lexeme.
function workedExample(): JsonEvent[] {
  const k = Ev.key
  const s = Ev.string
  const n = Ev.number
  return [
    Ev.objectStart, k('response'), Ev.objectStart, k('metadata'), Ev.objectStart,
    k('fields'), Ev.arrayStart,
    Ev.objectStart, k('title'), s('Identifier'), k('path'), Ev.arrayStart, s('id'), Ev.arrayEnd, Ev.objectEnd,
    Ev.objectStart, k('title'), s('Full name'), k('path'), Ev.arrayStart, s('person'), s('name'), Ev.arrayEnd, Ev.objectEnd,
    Ev.objectStart, k('title'), s('Balance'), k('path'), Ev.arrayStart, s('account'), s('balance'), Ev.arrayEnd, Ev.objectEnd,
    Ev.arrayEnd, Ev.objectEnd,
    k('payload'), Ev.objectStart, k('deep'), Ev.objectStart, k('records'), Ev.arrayStart,
    Ev.objectStart, k('id'), n(123, '123'), k('person'), Ev.objectStart, k('name'), s('Alice'), Ev.objectEnd,
    k('account'), Ev.objectStart, k('balance'), n(50.25, '50.25'), Ev.objectEnd, Ev.objectEnd,
    Ev.objectStart, k('account'), Ev.objectStart, k('balance'), n(72), Ev.objectEnd,
    k('id'), n(456, '456'), k('person'), Ev.objectStart, k('name'), s('Bob'), Ev.objectEnd, Ev.objectEnd,
    Ev.arrayEnd, Ev.objectEnd, Ev.objectEnd, Ev.objectEnd, Ev.objectEnd, Ev.end,
  ]
}

const metadataSelector = () => root().property('response').property('metadata').property('fields')
const recordsSelector = () =>
  root().property('response').property('payload').property('deep').property('records').eachIndex()

describe('event', () => {
  it('classifies', () => {
    assert.ok(isStart(Ev.arrayStart))
    assert.ok(isEnd(Ev.objectEnd))
    assert.ok(isScalar(Ev.null))
    assert.ok(!isScalar(Ev.key('k')))
    assert.ok(!isScalar(Ev.end))
  })

  it('compares numbers by value and lexeme, -0 apart from 0', () => {
    assert.ok(eventEquals(Ev.number(1, '1.0'), Ev.number(1, '1.0')))
    assert.ok(!eventEquals(Ev.number(1, '1.0'), Ev.number(1)))
    assert.ok(!eventEquals(Ev.number(-0), Ev.number(0)))
  })
})

describe('error', () => {
  it('codes are stable names, all of them in Code.ALL', () => {
    assert.equal(Code.ALL.length, 15)
    for (const code of Code.ALL) {
      assert.equal(Code.parse(code), code)
      assert.match(code, /^[A-Z_]+$/)
    }
    assert.equal(Code.parse('nope'), undefined)
    assert.deepEqual(Code.ALL.slice(0, 3), ['DSL_PARSE_ERROR', 'DSL_TYPE_ERROR', 'STREAM_REUSED'])
    assert.equal(Code.ALL[14], 'ABORTED')
  })

  it('a position names its file when it has one', () => {
    assert.equal(
      new Fail('DSL_TYPE_ERROR', 'arity: one argument').at(2, 3).toString(),
      'DSL_TYPE_ERROR: arity: one argument (2:3)',
    )
    assert.equal(
      new Fail('DSL_TYPE_ERROR', 'arity: one argument').at(2, 3).inFile('render.alc').toString(),
      'DSL_TYPE_ERROR: arity: one argument (render.alc:2:3)',
    )
    assert.equal(
      new Fail('DSL_PARSE_ERROR', 'bad_def: a name').inFile('lift.alc').toString(),
      'DSL_PARSE_ERROR: bad_def: a name (in lift.alc)',
    )
  })

  it('has the JSON shape hosts print', () => {
    const f = Fail.limit('max_record_bytes', 64, 'a row of 65 bytes').atPath('.rows[3]').committed()
    const j: any = f.toJSON()
    assert.equal(j.code, 'RESOURCE_LIMIT_EXCEEDED')
    assert.deepEqual(j.limit, { name: 'max_record_bytes', value: 64 })
    assert.equal(j.path, '.rows[3]')
    assert.equal(j.output, 'partial')
    assert.equal(j.file, undefined)
    assert.equal(
      f.toString(),
      'RESOURCE_LIMIT_EXCEEDED: a row of 65 bytes at .rows[3] [max_record_bytes = 64]',
    )
    const filed: any = new Fail('DSL_TYPE_ERROR', 'x').at(2, 3).inFile('render.alc').toJSON()
    assert.equal(filed.file, 'render.alc')
    assert.deepEqual([filed.row, filed.col], [2, 3])
    assert.equal(new Fail('ABORTED', 'x').toJSON().output, 'none')
  })
})

describe('limits', () => {
  it('has the documented defaults', () => {
    assert.deepEqual(Limits.default(), {
      max_depth: 256,
      max_key_bytes: 65536,
      max_scalar_bytes: 16777216,
      max_metadata_bytes: 16777216,
      max_columns: 10000,
      max_record_bytes: 67108864,
      max_capture_bytes: 67108864,
      max_output_bytes: null,
    })
  })

  it('counts UTF-8 bytes', () => {
    assert.equal(utf8Bytes(''), 0)
    assert.equal(utf8Bytes('abc'), 3)
    assert.equal(utf8Bytes('é'), 2)
    assert.equal(utf8Bytes('日'), 3)
    assert.equal(utf8Bytes('😀'), 4)
  })

  it('capture tracks the high water', () => {
    const m = new Metrics()
    m.capture(10)
    m.capture(20)
    m.release(10)
    m.capture(5)
    assert.equal(m.captured_bytes, 25)
    assert.equal(m.captured_bytes_high, 30)
    assert.equal(m.retained_bytes_high, 30)
    assert.equal(m.toJSON().captured_bytes_high, 30)
  })
})

describe('json text', () => {
  it('escapes strings as RFC 8259 requires', () => {
    assert.equal(jsonString('a"b\\c\n\u0001\u007fé'), '"a\\"b\\\\c\\n\\u0001\u007fé"')
    assert.equal(jsonString('\b\f\r\t\u001f'), '"\\b\\f\\r\\t\\u001f"')
    assert.equal(JSON.parse(jsonString('a"b\\c\n\u0001')), 'a"b\\c\n\u0001')
  })

  it('writes a number without a lexeme as the Rust runtime does', () => {
    assert.equal(numberText(72), '72')
    assert.equal(numberText(50.25), '50.25')
    assert.equal(numberText(0.1), '0.1')
    assert.equal(numberText(-0), '-0')
    assert.equal(numberText(0), '0')
    assert.equal(numberText(1e21), '1000000000000000000000')
    assert.equal(numberText(1e-7), '0.0000001')
    assert.equal(numberText(-1.5e-3), '-0.0015')
    assert.equal(numberText(1.2345678901234568e29), '123456789012345680000000000000')
    assert.equal(numberText(NaN), 'null')
  })
})

describe('selector', () => {
  it('displays as jq', () => {
    const s = root().property('response').property('odd key').index(3).eachIndex().eachMember()
    assert.equal(s.toString(), '.response."odd key"[3][*][]')
    assert.equal(root().toString(), '.')
    assert.equal(new Path(['a', 0, 'b-c']).toString(), '.a[0]."b-c"')
    assert.equal(Path.root().toString(), '.')
    assert.equal(new Path(['é', 'a_1', '1a', '', 'q"', '_', 't\tb']).toString(), '."é".a_1."1a"."".' + '"q\\""._."t\\tb"')
  })

  it('from segments is single', () => {
    const s = Selector.fromSegments(['account', 'balance'])
    assert.equal(s.toString(), '.account.balance')
    assert.ok(!s.isMulti())
    assert.ok(root().eachIndex().isMulti())
    assert.equal(Selector.fromSegments(['a', 0]).toString(), '.a[0]')
  })

  it('overlaps', () => {
    const rows = root().property('records').eachIndex()
    const meta = root().property('metadata')
    const inner = root().property('records').index(2).property('x')
    assert.ok(!rows.mayOverlap(meta))
    assert.ok(rows.mayOverlap(inner))
    assert.ok(inner.mayOverlap(rows))
    assert.ok(rows.mayOverlap(rows))
    assert.ok(root().mayOverlap(meta))
    assert.ok(root().eachMember().mayOverlap(meta))
    assert.ok(!root().eachIndex().mayOverlap(meta))
  })

  it('composes and stays immutable', () => {
    const a = root().property('a')
    assert.equal(a.compose(root().eachIndex()).toString(), '.a[*]')
    assert.equal(a.toString(), '.a')
  })
})

describe('datum', () => {
  function build(events: JsonEvent[], limit: number, policy: Duplicates = 'reject'): Datum {
    const b = new DatumBuilder(limit, 'max_capture_bytes', policy)
    for (const ev of events) b.event(ev)
    assert.ok(b.finished())
    return b.take() as Datum
  }

  it('walks and builds round trip', () => {
    const d = Datum.fromJSON({ a: [1, 'x', null, true], b: { c: 2.5 } })
    const rec = new EventRecorder()
    walkDatum(d, rec)
    const back = build(rec.events, Number.MAX_SAFE_INTEGER)
    assert.deepEqual(back, d)
    assert.equal(toText(back), '{"a":[1,"x",null,true],"b":{"c":2.5}}')
  })

  it('keeps lexemes and member order', () => {
    assert.equal(toText(Datum.number(1.2345678901234568e29, '123456789012345678901234567890')), '123456789012345678901234567890')
    assert.equal(toText(Datum.number(72)), '72')
    const ordered = Datum.object([['2', Datum.number(1)], ['1', Datum.number(2)]])
    assert.equal(toText(ordered), '{"2":1,"1":2}')
  })

  it('measures in UTF-8 bytes plus a node each', () => {
    const d = Datum.fromJSON(['abcd', 'ef'])
    assert.equal(Datum.byteSize(d), NODE_BYTES * 3 + 6)
    assert.equal(Datum.byteSize(Datum.string('日本')), NODE_BYTES + 6)
    assert.equal(Datum.byteSize(Datum.number(1)), NODE_BYTES + 8)
    const rec = new EventRecorder()
    walkDatum(d, rec)
    const f = failOf(() => build(rec.events, NODE_BYTES * 2 + 5))
    assert.equal(f.code, 'RESOURCE_LIMIT_EXCEEDED')
    assert.equal(f.limit?.name, 'max_capture_bytes')
  })

  it('gets and takes paths', () => {
    const d = Datum.fromJSON({ a: [{ b: 'x' }], c: 2 })
    assert.deepEqual(Datum.getPath(d, ['a', 0, 'b']), Datum.string('x'))
    assert.equal(Datum.getPath(d, ['z']), undefined)
    assert.equal(Datum.getPath(d, [0]), undefined)
    assert.deepEqual(Datum.getPath(d, []), d)
    assert.deepEqual(Datum.takePath(d, ['a', 0, 'b']), Datum.string('x'))
    assert.equal(toText(d), '{"a":[{"b":null}],"c":2}')
    assert.equal(Datum.takePath(d, ['z']), undefined)
  })

  it('follows the duplicates policy', () => {
    const events = [Ev.objectStart, Ev.key('a'), Ev.bool(true), Ev.key('a'), Ev.bool(false), Ev.objectEnd]
    assert.equal(codeOf(() => build(events, 1e9, 'reject')), 'DUPLICATE_MEMBER')
    assert.equal(toText(build(events, 1e9, 'last_wins')), '{"a":false}')
    assert.equal(toText(build(events, 1e9, 'first_wins')), '{"a":true}')
  })

  it('an unfinished builder keeps its charge when probed', () => {
    const b = new DatumBuilder(1e9, 'max_capture_bytes', 'reject')
    b.event(Ev.arrayStart)
    b.event(Ev.string('abcd'))
    const held = b.bytes()
    assert.equal(b.take(), undefined)
    assert.equal(b.bytes(), held)
    b.event(Ev.arrayEnd)
    assert.notEqual(b.take(), undefined)
    assert.equal(b.bytes(), 0)
  })

  it('a repeated member does not grow the charge', () => {
    for (const policy of ['first_wins', 'last_wins'] as Duplicates[]) {
      const b = new DatumBuilder(1e9, 'max_capture_bytes', policy)
      b.event(Ev.objectStart)
      b.event(Ev.key('k'))
      b.event(Ev.string('first'))
      const once = b.bytes()
      for (let i = 0; i < 100; i++) {
        b.event(Ev.key('k'))
        b.event(Ev.string('again'))
      }
      assert.equal(b.bytes(), once, policy)
      b.event(Ev.objectEnd)
      assert.equal(Datum.byteSize(b.take() as Datum), once, policy)
    }
  })

  it('a repeated member still trips the limit while both are held', () => {
    const b = new DatumBuilder(NODE_BYTES * 2 + 1 + 5 + 3, 'max_capture_bytes', 'last_wins')
    b.event(Ev.objectStart)
    b.event(Ev.key('k'))
    b.event(Ev.string('first'))
    b.event(Ev.key('k'))
    assert.equal(codeOf(() => b.event(Ev.string('second'))), 'RESOURCE_LIMIT_EXCEEDED')
  })

  it('refuses a malformed sequence', () => {
    const fresh = () => new DatumBuilder(1e9, 'max_capture_bytes', 'reject')
    assert.equal(codeOf(() => fresh().event(Ev.objectEnd)), 'PROTOCOL_ORDER_ERROR')
    const b = fresh()
    b.event(Ev.objectStart)
    assert.equal(codeOf(() => b.event(Ev.null)), 'PROTOCOL_ORDER_ERROR')
    const c = fresh()
    c.event(Ev.arrayStart)
    assert.equal(codeOf(() => c.event(Ev.end)), 'PROTOCOL_ORDER_ERROR')
  })

  it('reads engine values, with dates as their source text', () => {
    const d = Datum.fromTabnas({ a: [1, 2], b: 'x', c: null, d: undefined, e: new String('t') })
    assert.equal(toText(d), '{"a":[1,2],"b":"x","c":null,"d":null,"e":"t"}')
  })
})

describe('sink', () => {
  it('a recorder records and replays', () => {
    const rec = new EventRecorder()
    for (const ev of [Ev.arrayStart, Ev.bool(true), Ev.arrayEnd, Ev.end]) rec.event(ev)
    const count = new CountSink()
    assert.equal(replay(rec.events, count), 'continue')
    assert.equal(count.events, 4)
  })

  it('stop ends a replay', () => {
    let seen = 0
    const stopper = new FnSink(() => (++seen === 2 ? 'stop' : 'continue'))
    assert.equal(replay([Ev.arrayStart, Ev.null, Ev.arrayEnd, Ev.end], stopper), 'stop')
    assert.equal(seen, 2)
  })

  it("a tree's events pass the tree contract unchanged", () => {
    const text = '{"a":{"b":1,"a":2},"b":[{"a":3},{"a":4},[],{}],"c":[],"d":{}}'
    const guard = new TreeContract(new EventRecorder())
    assert.equal(replay(doc(text), guard), 'continue')
    assert.ok(guard.inner.events.every((e, i) => eventEquals(e, doc(text)[i])))
    for (const rootText of ['1', '"x"', 'null', '[]', '[1,[2,[3]]]']) {
      const g = new TreeContract(new EventRecorder())
      replay(doc(rootText), g)
      assert.equal(g.inner.events.length, doc(rootText).length, rootText)
    }
  })

  it('a repeated key in one object is a duplicate member at its path', () => {
    const k = Ev.key
    const stream = [Ev.objectStart, k('a'), Ev.null, k('b'), Ev.arrayStart, Ev.null, Ev.objectStart, k('x'), Ev.null, k('x')]
    const guard = new TreeContract(new EventRecorder())
    const f = failOf(() => replay(stream, guard))
    assert.equal(f.code, 'DUPLICATE_MEMBER')
    assert.ok(f.message.startsWith('member "x"'), f.message)
    assert.equal(f.path, '.b[1].x')
    assert.equal(guard.inner.events.length, stream.length - 1)
    const g = new TreeContract(new EventRecorder())
    assert.equal(failOf(() => replay([Ev.objectStart, k('a b'), Ev.null, k('a b')], g)).path, '."a b"')
  })

  it("events no tree has are refused as not a tree's", () => {
    const k = Ev.key
    const cases: [JsonEvent[], string, string][] = [
      [[Ev.objectStart, Ev.null], 'a value where a key is due', '.'],
      [[Ev.objectStart, k('a'), Ev.objectStart, Ev.arrayStart], 'a value where a key is due', '.a'],
      [[Ev.objectStart, k('a'), k('b')], 'a key where a value is due', '.a'],
      [[Ev.arrayStart, k('a')], 'a key outside an object', '[0]'],
      [[k('a')], 'a key outside an object', '.'],
      [[Ev.objectStart, k('a'), Ev.objectEnd], "an object's end where a value is due", '.a'],
      [[Ev.arrayStart, Ev.null, Ev.objectEnd], "an object's end where none is due", '[1]'],
      [[Ev.objectEnd], "an object's end where none is due", '.'],
      [[Ev.objectStart, Ev.arrayEnd], "an array's end where none is due", '.'],
      [[Ev.null, Ev.null], 'a second root value', '.'],
      [[Ev.arrayStart, Ev.arrayEnd, Ev.objectStart], 'a second root value', '.'],
      [[Ev.arrayStart, Ev.end], 'its end inside an open container', '[0]'],
    ]
    for (const [bad, why, path] of cases) {
      const guard = new TreeContract(new EventRecorder())
      const f = failOf(() => replay(bad, guard))
      assert.equal(f.code, 'STREAMABILITY_UNKNOWN', why)
      assert.ok(f.message.includes(why), f.message)
      assert.equal(f.path, path, why)
      assert.equal(guard.inner.events.length, bad.length - 1, why)
    }
  })
})

describe('matcher', () => {
  function run(selectors: Selector[], events: JsonEvent[]): [string, number[]][] {
    const m = new Matcher(selectors)
    const out: [string, number[]][] = []
    for (const ev of events) {
      const hit = m.event(ev)
      if (hit.begins > 0) out.push([m.path(hit.depth).toString(), m.begins().slice()])
    }
    assert.ok(m.ended())
    return out
  }

  it('a property chain names one value', () => {
    assert.deepEqual(run([root().property('a').property('b')], doc('{"a":{"b":[1,2],"c":3},"b":{"b":4}}')), [['.a.b', [0]]])
  })

  it('each index names every element with its index', () => {
    assert.deepEqual(run([root().property('xs').eachIndex()], doc('{"xs":[10,{"y":1},[2]],"ys":[9]}')), [
      ['.xs[0]', [0]],
      ['.xs[1]', [0]],
      ['.xs[2]', [0]],
    ])
  })

  it('each member names every member value', () => {
    assert.deepEqual(run([root().eachMember()], doc('{"a":1,"odd key":{"z":2},"c":[3]}')), [
      ['.a', [0]],
      ['."odd key"', [0]],
      ['.c', [0]],
    ])
  })

  it('an index names one element; the root names the root', () => {
    assert.deepEqual(run([root().index(1)], doc('[[0,1],[2,3],[4,5]]')), [['[1]', [0]]])
    assert.deepEqual(run([root()], doc('{"a":[1]}')), [['.', [0]]])
    assert.deepEqual(run([root()], doc('42')), [['.', [0]]])
  })

  it('selectors sharing a prefix match independently, in id order', () => {
    assert.deepEqual(
      run(
        [root().property('response').property('metadata'), root().property('response').property('records').eachIndex()],
        doc('{"response":{"metadata":[1],"records":[{"id":1},{"id":2}]},"records":[0]}'),
      ),
      [['.response.metadata', [0]], ['.response.records[0]', [1]], ['.response.records[1]', [1]]],
    )
    assert.deepEqual(run([root().property('a'), root().eachMember()], doc('{"a":1,"b":2}')), [
      ['.a', [0, 1]],
      ['.b', [1]],
    ])
  })

  it('nests, keeps kinds apart and reuses levels cleanly', () => {
    assert.deepEqual(run([root().property('missing').eachIndex(), root().index(5)], doc('{"a":[1,2,3]}')), [])
    assert.deepEqual(run([root().eachIndex().eachIndex()], doc('[[1,2],[],[[3]]]')), [
      ['[0][0]', [0]],
      ['[0][1]', [0]],
      ['[2][0]', [0]],
    ])
    assert.deepEqual(run([root().index(0), root().property('0')], doc('{"0":[7]}')), [['."0"', [1]]])
    assert.deepEqual(run([root().eachIndex().property('k')], doc('[{"k":1,"other":2},{"other":3},{"k":4}]')), [
      ['[0].k', [0]],
      ['[2].k', [0]],
    ])
  })

  it('a close reports the path of the container that closed, and depth counts enclosing', () => {
    const m = new Matcher([])
    const closes: string[] = []
    for (const ev of doc('{"a":[{"b":1},{"c":[]}]}')) {
      const hit = m.event(ev)
      if ('close' === hit.kind) closes.push(m.path(hit.depth).toString())
    }
    assert.deepEqual(closes, ['.a[0]', '.a[1].c', '.a[1]', '.a', '.'])
    const d = new Matcher([])
    assert.deepEqual(
      doc('[1,[2]]').map((ev) => {
        const h = d.event(ev)
        return [h.kind, h.depth]
      }),
      [['start', 0], ['scalar', 1], ['start', 1], ['scalar', 2], ['close', 1], ['close', 0], ['end', 0]],
    )
  })

  it('malformed streams are protocol errors', () => {
    const one = Ev.number(1)
    const k = Ev.key
    const cases: JsonEvent[][] = [
      [k('a')],
      [Ev.objectStart, one],
      [Ev.objectStart, k('a'), k('b')],
      [Ev.objectStart, k('a'), Ev.objectEnd],
      [Ev.arrayStart, Ev.objectEnd],
      [Ev.objectStart, Ev.arrayEnd],
      [Ev.arrayEnd],
      [one, one],
      [Ev.end],
      [Ev.arrayStart, Ev.end],
      [one, Ev.end, Ev.end],
      [Ev.arrayStart, k('a')],
    ]
    for (const c of cases) {
      const m = new Matcher([root()])
      assert.equal(codeOf(() => c.forEach((ev) => m.event(ev))), 'PROTOCOL_ORDER_ERROR')
    }
  })
})

describe('router', () => {
  const router = (specs: CaptureSpec[], limits = Limits.default(), policy: Duplicates = 'reject') =>
    new Router(specs, limits, policy, new Metrics(), new SelectedRecorder())

  it('delivers the worked example metadata then each record in order', () => {
    const r = router([CaptureSpec.materialize('meta', metadataSelector()), CaptureSpec.materialize('row', recordsSelector())])
    assert.equal(replay(workedExample(), r), 'continue')
    assert.ok(r.ended())
    const got = r.downstream.selections.map((s) => [s.id, s.tag, s.path.toString(), toText(s.value as Datum)])
    assert.deepEqual(got, [
      [0, 'meta', '.response.metadata.fields',
        '[{"title":"Identifier","path":["id"]},{"title":"Full name","path":["person","name"]},{"title":"Balance","path":["account","balance"]}]'],
      [1, 'row', '.response.payload.deep.records[0]', '{"id":123,"person":{"name":"Alice"},"account":{"balance":50.25}}'],
      [1, 'row', '.response.payload.deep.records[1]', '{"account":{"balance":72},"id":456,"person":{"name":"Bob"}}'],
    ])
  })

  it('refuses overlapping materializations at construction, both ways', () => {
    const rows = root().property('records').eachIndex()
    const inner = root().property('records').index(2).property('x')
    for (const [a, b] of [[rows, inner], [inner, rows]]) {
      assert.equal(codeOf(() => router([CaptureSpec.materialize('a', a), CaptureSpec.materialize('b', b)])), 'CAPTURE_OVERLAP_UNSUPPORTED')
    }
    assert.equal(codeOf(() => router([CaptureSpec.observe('a', rows), CaptureSpec.materialize('b', root())])), 'CAPTURE_OVERLAP_UNSUPPORTED')
    router([CaptureSpec.observe('a', rows), CaptureSpec.observe('b', root())])
    router([CaptureSpec.materialize('a', rows), CaptureSpec.materialize('b', root().property('meta'))])
  })

  it('names the limit and the path of a capture over its budget', () => {
    const r = router([CaptureSpec.materialize('row', recordsSelector())], Limits.with({ max_capture_bytes: 40 }))
    const f = failOf(() => replay(workedExample(), r))
    assert.equal(f.code, 'RESOURCE_LIMIT_EXCEEDED')
    assert.deepEqual(f.limit, { name: 'max_capture_bytes', value: 40 })
    assert.equal(f.path, '.response.payload.deep.records[0]')
    const m = router([CaptureSpec.materialize('meta', metadataSelector()).withBudget(10, 'max_metadata_bytes')])
    assert.equal(failOf(() => replay(workedExample(), m)).limit?.name, 'max_metadata_bytes')
  })

  it('names max_depth at the container that passes it', () => {
    const r = router([], Limits.with({ max_depth: 3 }))
    replay([Ev.arrayStart, Ev.arrayStart, Ev.arrayStart], r)
    const f = failOf(() => r.event(Ev.arrayStart))
    assert.equal(f.limit?.name, 'max_depth')
    assert.equal(f.path, '[0][0][0]')
  })

  it('delivers observed paths at the value end, innermost first', () => {
    const r = router([
      CaptureSpec.observe('row', recordsSelector()),
      CaptureSpec.observe('records', root().property('response').property('payload').property('deep').property('records')),
      CaptureSpec.observe('title', metadataSelector().eachIndex().property('title')),
    ])
    replay(workedExample(), r)
    assert.deepEqual(
      r.downstream.selections.map((s) => [s.tag, s.path.toString(), s.value]),
      [
        ['title', '.response.metadata.fields[0].title', null],
        ['title', '.response.metadata.fields[1].title', null],
        ['title', '.response.metadata.fields[2].title', null],
        ['row', '.response.payload.deep.records[0]', null],
        ['row', '.response.payload.deep.records[1]', null],
        ['records', '.response.payload.deep.records', null],
      ],
    )
  })

  it('a stop from downstream stops the router, and end comes exactly once', () => {
    let selected = 0
    const takeOne: RouteSink = {
      selected: () => (selected++, 'stop'),
      end: () => {
        throw new Error('end must not follow a stop')
      },
    }
    const r = new Router([CaptureSpec.materialize('row', recordsSelector())], Limits.default(), 'reject', new Metrics(), takeOne)
    assert.equal(replay(workedExample(), r), 'stop')
    assert.equal(selected, 1)
    assert.ok(!r.ended())
    let ends = 0
    const e = new Router([], Limits.default(), 'reject', new Metrics(), {
      selected: () => {
        throw new Error('nothing is selected')
      },
      end: () => (ends++, 'continue'),
    })
    replay(workedExample(), e)
    assert.equal(ends, 1)
    assert.equal(codeOf(() => e.event(Ev.end)), 'PROTOCOL_ORDER_ERROR')
    assert.equal(ends, 1)
  })

  it('a began hook refuses a value before anything is retained, at its path', () => {
    const r = new Router([CaptureSpec.materialize('v', root().property('a'))], Limits.default(), 'reject', new Metrics(), {
      began: () => {
        throw new Fail('INPUT_ORDER_VIOLATION', 'not yet')
      },
      selected: () => 'continue',
      end: () => 'continue',
    })
    const f = failOf(() => replay(doc('{"a":[1]}'), r))
    assert.equal(f.code, 'INPUT_ORDER_VIOLATION')
    assert.equal(f.path, '.a')
  })

  it('accounts for captures and leaves the source counts alone', () => {
    const metrics = new Metrics()
    const out = new SelectedRecorder()
    const r = new Router([CaptureSpec.materialize('row', recordsSelector())], Limits.default(), 'reject', metrics, out)
    replay(workedExample(), r)
    assert.deepEqual([metrics.events, metrics.keys, metrics.scalars, metrics.captured_bytes], [0, 0, 0, 0])
    const biggest = Math.max(...out.selections.map((s) => Datum.byteSize(s.value as Datum)))
    assert.equal(metrics.captured_bytes_high, biggest)
  })

  it('a scalar capture is delivered whole, with its lexeme', () => {
    const r = router([CaptureSpec.materialize('v', root().property('v'))])
    replay([Ev.objectStart, Ev.key('v'), Ev.number(1.5, '1.50'), Ev.objectEnd, Ev.end], r)
    assert.deepEqual(r.downstream.selections[0].value, Datum.number(1.5, '1.50'))
  })
})

describe('table', () => {
  const fromMetadata = (): TableBinding => ({
    schema: Schema.fromMetadata(metadataSelector(), columnFromMeta),
    rows: recordsSelector(),
  })
  const run = (binding: TableBinding, events: JsonEvent[], limits = Limits.default()) => {
    const t = new TableFromJson(binding, limits, 'reject', new Metrics(), new Table())
    replay(events, t)
    return t.sink
  }
  const labels = (t: Table) => t.columns.map((c) => c.label)
  const rows = (t: Table) => t.rows.map((r) => r.map(Cell.toText))

  it('column from meta reads title and path', () => {
    const c = columnFromMeta(Datum.fromJSON({ title: 'Balance', path: ['account', 'balance'] }))
    assert.equal(c.label, 'Balance')
    assert.deepEqual(c.source, ['account', 'balance'])
    assert.deepEqual(columnFromMeta(Datum.fromJSON({ title: 'First', path: ['tags', 0] })).source, ['tags', 0])
    assert.equal(codeOf(() => columnFromMeta(Datum.fromJSON({ title: 'x', path: ['a', -1] }))), 'INPUT_INVALID')
    assert.equal(codeOf(() => columnFromMeta(Datum.fromJSON({ path: ['a'] }))), 'INPUT_INVALID')
  })

  it('cells print as JSON', () => {
    assert.equal(Cell.toText(Cell.fromDatum(Datum.fromJSON(50.25))), '50.25')
    assert.equal(Cell.toText(Cell.fromDatum(Datum.fromJSON([1, 2]))), '"[1,2]"')
    assert.equal(Cell.toText(Cell.missing), 'missing')
    assert.deepEqual(Cell.fromDatum(Datum.null), Cell.null)
  })

  it('the worked example yields the spec table with lexemes kept', () => {
    const t = run(fromMetadata(), workedExample())
    assert.deepEqual(labels(t), ['Identifier', 'Full name', 'Balance'])
    assert.deepEqual(rows(t), [
      ['123', '"Alice"', '50.25'],
      ['456', '"Bob"', '72'],
    ])
    assert.deepEqual(t.rows[0][2], Cell.number(50.25, '50.25'))
    assert.deepEqual(t.rows[1][2], Cell.number(72))
    assert.ok(t.ended)
  })

  it('a row before the metadata is refused at its start', () => {
    const f = failOf(() =>
      run(fromMetadata(), doc('{"response":{"payload":{"deep":{"records":[{"id":1}]}},"metadata":{"fields":[{"title":"Id","path":["id"]}]}}}')),
    )
    assert.equal(f.code, 'INPUT_ORDER_VIOLATION')
    assert.equal(f.path, '.response.payload.deep.records[0]')
  })

  it('metadata selected twice is an order violation', () => {
    const f = failOf(() =>
      run(
        {
          schema: Schema.fromMetadata(root().eachIndex().property('response').property('metadata').property('fields')),
          rows: root().property('rows').eachIndex(),
        },
        doc('[{"response":{"metadata":{"fields":[]}}},{"response":{"metadata":{"fields":[]}}}]'),
      ),
    )
    assert.equal(f.code, 'INPUT_ORDER_VIOLATION')
    assert.equal(f.path, '[1].response.metadata.fields')
  })

  it('missing and explicit null are told apart by policy', () => {
    const events = doc('{"rows":[{"a":null},{}]}')
    const binding = (missing: 'missing' | 'null' | 'error'): TableBinding => ({
      schema: Schema.static([boundColumn('A', ['a'], missing)]),
      rows: root().property('rows').eachIndex(),
    })
    assert.deepEqual(run(binding('missing'), events).rows, [[Cell.null], [Cell.missing]])
    assert.deepEqual(run(binding('null'), events).rows, [[Cell.null], [Cell.null]])
    const f = failOf(() => run(binding('error'), events))
    assert.equal(f.code, 'MISSING_VALUE')
    assert.equal(f.path, '.rows[1].a')
  })

  it('zero rows is an empty table with its schema; missing metadata is invalid input', () => {
    const t = run(fromMetadata(), doc('{"response":{"metadata":{"fields":[{"title":"Id","path":["id"]}]},"payload":{"deep":{"records":[]}}}}'))
    assert.deepEqual(labels(t), ['Id'])
    assert.ok(0 === t.rows.length && t.ended)
    const s = run({ schema: Schema.static([boundColumn('x', ['x'])]), rows: root().eachIndex() }, doc('[]'))
    assert.deepEqual(labels(s), ['x'])
    const i = run({ schema: Schema.infer(), rows: root().eachIndex() }, doc('[]'))
    assert.ok(0 === i.columns.length && i.ended)
    const f = failOf(() => run(fromMetadata(), doc('{"other":1}')))
    assert.equal(f.code, 'INPUT_INVALID')
    assert.equal(f.path, '.response.metadata.fields')
  })

  it("infer takes the first row's members in its order", () => {
    const t = run({ schema: Schema.infer(), rows: root().eachIndex() }, doc('[{"b":1,"a":"x"},{"a":"y","c":true},{"b":3}]'))
    assert.deepEqual(labels(t), ['b', 'a'])
    assert.deepEqual(rows(t), [['1', '"x"'], ['missing', '"y"'], ['3', 'missing']])
  })

  it('infer labels an array row by position', () => {
    const infer = (): TableBinding => ({ schema: Schema.infer(), rows: root().eachIndex() })
    const t = run(infer(), doc('[[1,"x"],["y",true,3],[2]]'))
    assert.deepEqual(labels(t), ['0', '1'])
    assert.deepEqual(rows(t), [['1', '"x"'], ['"y"', 'true'], ['2', 'missing']])
    // An empty array row is a table of no columns, as no rows is.
    const e = run(infer(), doc('[[],[1]]'))
    assert.deepEqual(labels(e), [])
    assert.deepEqual(rows(e), [[], []])
    assert.ok(e.ended)
  })

  it('infer gives a scalar row one value column', () => {
    const t = run({ schema: Schema.infer(), rows: root().eachIndex() }, doc('[1,"s",true,null]'))
    assert.deepEqual(labels(t), ['value'])
    assert.deepEqual(rows(t), [['1'], ['"s"'], ['true'], ['null']])
  })

  // A later row of another kind than the first projects through the first
  // row's paths: a key path on an array or a scalar, and an index path on an
  // object or a scalar, miss, so the cell is missing under the column's
  // policy; the empty path of a `value` column finds every row, a container
  // as its compact JSON text.
  it("infer projects a later row of another kind through the first row's paths", () => {
    const infer = (): TableBinding => ({ schema: Schema.infer(), rows: root().eachIndex() })
    const o = run(infer(), doc('[{"a":1},[2],3]'))
    assert.deepEqual(labels(o), ['a'])
    assert.deepEqual(rows(o), [['1'], ['missing'], ['missing']])
    const a = run(infer(), doc('[[1],{"0":2},3]'))
    assert.deepEqual(labels(a), ['0'])
    assert.deepEqual(rows(a), [['1'], ['missing'], ['missing']])
    const s = run(infer(), doc('[1,{"a":2},[3]]'))
    assert.deepEqual(labels(s), ['value'])
    assert.deepEqual(rows(s), [['1'], ['"{\\"a\\":2}"'], ['"[3]"']])
  })

  it('projects nested and overlapping paths whatever the member order', () => {
    const t = run(
      {
        schema: Schema.static([
          boundColumn('P', ['p']),
          boundColumn('N', ['p', 'n']),
          boundColumn('Id', ['id']),
          boundColumn('Tag', ['tags', 1]),
        ]),
        rows: root().property('rows').eachIndex(),
      },
      doc('{"rows":[{"p":{"n":"a"},"id":1,"tags":["t0","t1"]},{"tags":["u0"],"id":2,"p":{"n":"b"}}]}'),
    )
    assert.deepEqual(rows(t), [
      ['"{\\"n\\":\\"a\\"}"', '"a"', '1', '"t1"'],
      ['"{\\"n\\":\\"b\\"}"', '"b"', '2', 'missing'],
    ])
  })

  it('enforces max_columns, max_metadata_bytes and max_record_bytes by name', () => {
    assert.equal(failOf(() => run(fromMetadata(), workedExample(), Limits.with({ max_columns: 2 }))).limit?.name, 'max_columns')
    assert.equal(failOf(() => run(fromMetadata(), workedExample(), Limits.with({ max_metadata_bytes: 64 }))).limit?.name, 'max_metadata_bytes')
    const r = failOf(() => run(fromMetadata(), workedExample(), Limits.with({ max_record_bytes: 48 })))
    assert.equal(r.limit?.name, 'max_record_bytes')
    assert.equal(r.path, '.response.payload.deep.records[0]')
    const infer = (): TableBinding => ({ schema: Schema.infer(), rows: root().eachIndex() })
    assert.equal(failOf(() => run(infer(), doc('[{"a":1,"b":2}]'), Limits.with({ max_columns: 1 }))).limit?.name, 'max_columns')
    // Two one-byte names take 16 + 2 * (16 + 1) = 50 bytes.
    const m = failOf(() => run(infer(), doc('[{"a":1,"b":2}]'), Limits.with({ max_metadata_bytes: 49 })))
    assert.equal(m.limit?.name, 'max_metadata_bytes')
    assert.equal(m.path, '[0]')
    assert.deepEqual(labels(run(infer(), doc('[{"a":1,"b":2}]'), Limits.with({ max_metadata_bytes: 50 }))), ['a', 'b'])
    // Positional labels and the `value` label are measured the same way:
    // "0" and "1" take 50 bytes too, and "value" 16 + 16 + 5 = 37.
    const p = failOf(() => run(infer(), doc('[[1,2]]'), Limits.with({ max_metadata_bytes: 49 })))
    assert.equal(p.limit?.name, 'max_metadata_bytes')
    assert.equal(p.path, '[0]')
    assert.deepEqual(labels(run(infer(), doc('[[1,2]]'), Limits.with({ max_metadata_bytes: 50 }))), ['0', '1'])
    assert.equal(failOf(() => run(infer(), doc('[1]'), Limits.with({ max_metadata_bytes: 36 }))).limit?.name, 'max_metadata_bytes')
    assert.deepEqual(labels(run(infer(), doc('[1]'), Limits.with({ max_metadata_bytes: 37 }))), ['value'])
    assert.equal(failOf(() => run(infer(), doc('[[1,2]]'), Limits.with({ max_columns: 1 }))).limit?.name, 'max_columns')
  })

  it('a bad descriptor names its position', () => {
    const f = failOf(() =>
      run(fromMetadata(), doc('{"response":{"metadata":{"fields":[{"title":"Id","path":["id"]},{"path":["x"]}]},"payload":{"deep":{"records":[]}}}}')),
    )
    assert.equal(f.code, 'INPUT_INVALID')
    assert.equal(f.path, '.response.metadata.fields[1]')
  })

  it('end arrives only with the document end, rows count, and a stop stops the run', () => {
    const metrics = new Metrics()
    const t = new TableFromJson(fromMetadata(), Limits.default(), 'reject', metrics, new Table())
    const events = workedExample()
    replay(events.slice(0, -1), t)
    assert.ok(!t.ended() && !t.sink.ended)
    assert.equal(t.sink.rows.length, 2)
    t.event(Ev.end)
    assert.ok(t.ended() && t.sink.ended)
    assert.equal(metrics.rows, 2)
    const stopAtSchema = {
      tableEvent: (ev: any) => {
        if ('schema' === ev.type) return 'stop' as const
        throw new Error('nothing follows a stop')
      },
    }
    const s = new TableFromJson(fromMetadata(), Limits.default(), 'reject', new Metrics(), stopAtSchema)
    assert.equal(replay(workedExample(), s), 'stop')
  })

  it('overlapping metadata and rows are refused', () => {
    assert.equal(
      codeOf(
        () =>
          new TableFromJson(
            { schema: Schema.fromMetadata(root().property('a')), rows: root().property('a').eachIndex() },
            Limits.default(),
            'reject',
            new Metrics(),
            new Table(),
          ),
      ),
      'CAPTURE_OVERLAP_UNSUPPORTED',
    )
  })
})

describe('scan-emit', () => {
  it('runs a sum with a total at the end, and finishes once', () => {
    const seen: string[] = []
    const scan = new ScanEmit<number, number, string>(
      0,
      (sum, x) => Transition.emit(sum + x, `+${x}`),
      (sum) => [`=${sum}`],
      (s) => (seen.push(s), 'continue'),
    )
    scan.item(1)
    scan.item(2)
    scan.finish()
    assert.equal(codeOf(() => scan.finish()), 'PROTOCOL_ORDER_ERROR')
    assert.equal(codeOf(() => scan.item(3)), 'PROTOCOL_ORDER_ERROR')
    assert.deepEqual(seen, ['+1', '+2', '=3'])
  })

  it('propagates a stop', () => {
    const scan = new ScanEmit<null, number, number>(
      null,
      (_s, x) => Transition.emit(null, x),
      () => [],
      (x) => (2 === x ? 'stop' : 'continue'),
    )
    assert.equal(scan.item(1), 'continue')
    assert.equal(scan.item(2), 'stop')
  })
})

// The selected type is used for its shape in the router tests.
void ({} as Selected)
