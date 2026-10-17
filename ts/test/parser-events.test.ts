/* Copyright (c) 2026 tabnas, MIT License */

// `ParserEventSource` and the native `TransduceTree`: the ports of the Rust
// unit tests in rs/src/source/parser_events.rs and rs/src/transduce_tree.rs
// that a TSV row cannot carry (a sink that stops or fails, the abort flag,
// the metrics, a pass built by hand).

import { describe, it } from 'node:test'
import assert from 'node:assert'

import { make as makeJson } from '@tabnas/json'

import {
  AbortFlag,
  Datum,
  Ev,
  EventRecorder,
  Fail,
  Flow,
  Limits,
  Metrics,
  ParserEvent,
  ParserEventSource,
  ParserSink,
  ParserSource,
  Prune,
  RulePass,
  SourceMode,
  TransduceTree,
  TreeSpec,
  parserEventToJSON,
  treeTextRefusal,
} from '../dist/transduce'

class Seen implements ParserSink {
  events: ParserEvent[] = []
  event(ev: ParserEvent): Flow {
    this.events.push(ev)
    return 'continue'
  }
}

function passes(src: string): ParserEvent[] {
  const seen = new Seen()
  new ParserEventSource(makeJson(), src).run(seen)
  return seen.events
}

function thrown(f: () => unknown): Fail {
  try {
    f()
  } catch (err) {
    if (err instanceof Fail) return err
    throw err
  }
  throw new Error('no failure')
}

const JSON_SPEC: TreeSpec = {
  rules: new Map<string, any>([
    ['map', { container: 'object' }],
    ['list', { container: 'array' }],
    ['pair', { key: { token: 0 } }],
    ['val', { scalar: { token: 0 } }],
  ]),
}

describe('ParserEventSource', () => {
  // The open pass consumed the number, whose source text is its lexeme; the
  // close consumed the end of the source, which has no value, and carries
  // the node's value. The groups are sorted.
  it("a root scalar is its val rule's two passes, then end", () => {
    const seen = passes('2.50')
    assert.deepStrictEqual(seen.map(parserEventToJSON), [
      ['open', { name: 'val', d: 0, i: 1, alt: { p: '', r: '', b: 0, g: ['json', 'val'] }, tokens: [['#NR', 2.5, '2.50', 1, 1]], forced: false, n: {} }],
      ['close', { name: 'val', d: 0, i: 1, alt: { p: '', r: '', b: 0, g: ['end', 'json'] }, tokens: [['#ZZ', null, '', 1, 5]], forced: false, n: {}, value: 2.5 }],
      ['end'],
    ])
    const open = seen[0] as any
    assert.deepStrictEqual(open.pass.tokens[0].val, Datum.number(2.5, '2.50'))
  })

  // What this runtime's engine reports differently (DIVERGENCE.md 6 and 7):
  // the root rule is instance 1 (Go's and Rust's is 0), and a fixed token
  // has no value, as in Go (Rust's has its text).
  it("the engine's own instance numbers and fixed-token values", () => {
    const seen = passes('[1]') as any[]
    assert.equal(seen[0].pass.i, 1)
    assert.equal(seen[1].pass.tokens[0].name, '#OS')
    assert.deepStrictEqual(seen[1].pass.tokens[0].val, Datum.null)
  })

  it('a parse error is INPUT_INVALID after the passes before it', () => {
    const seen = new Seen()
    const err = thrown(() => new ParserEventSource(makeJson(), '[1,\n ]').run(seen))
    assert.equal(err.code, 'INPUT_INVALID')
    assert.equal(err.row, 2)
    assert.ok(0 < seen.events.length)
    assert.ok(!seen.events.some((e) => 'end' === e.type))
  })

  it('a huge token is refused by name before the sink sees it', () => {
    const seen = new Seen()
    const limits = { ...Limits.default(), max_scalar_bytes: 3 }
    const err = thrown(() => new ParserEventSource(makeJson(), '["abcd"]').limits(limits).run(seen))
    assert.equal(err.code, 'RESOURCE_LIMIT_EXCEEDED')
    assert.equal(err.limit?.name, 'max_scalar_bytes')
    assert.ok(!seen.events.some((e) => JSON.stringify(parserEventToJSON(e)).includes('abcd')))
  })

  it('a stop stops the parse, and a failure comes back', () => {
    let n = 0
    const flow = new ParserEventSource(makeJson(), '[1,2,3]').run({
      event: () => (2 === ++n ? 'stop' : 'continue'),
    })
    assert.equal(flow, 'stop')
    assert.equal(n, 2)
    const err = thrown(() =>
      new ParserEventSource(makeJson(), '[1]').run({
        event: () => {
          throw Fail.output('disk full')
        },
      }),
    )
    assert.equal(err.code, 'OUTPUT_FAILED')
  })

  it('an aborted flag cancels the parse as ABORTED', () => {
    const abort = new AbortFlag()
    abort.abort()
    const seen = new Seen()
    assert.equal(thrown(() => new ParserEventSource(makeJson(), '[1]').abort(abort).run(seen)).code, 'ABORTED')
    assert.deepStrictEqual(seen.events, [])
  })

  it('metrics count the events delivered', () => {
    const metrics = new Metrics()
    const seen = new Seen()
    new ParserEventSource(makeJson(), '{"a":[1]}').metrics(metrics).run(seen)
    assert.equal(metrics.events, seen.events.length)
  })

  it('a source runs once', () => {
    const source = new ParserEventSource(makeJson(), '1')
    source.run(new Seen())
    assert.equal(thrown(() => source.run(new Seen())).code, 'STREAM_REUSED')
  })
})

function pass(name: string, tokens: Datum[], r: string, value?: Datum): RulePass {
  return {
    name,
    d: 1,
    i: 1,
    alt: { p: '', r, b: 0, g: [] },
    tokens: tokens.map((val) => ({ name: '#T', val, src: '', row: 1, col: 1 })),
    forced: false,
    n: [],
    ...(undefined === value ? {} : { value }),
  }
}

// A pass whose tokens are spelled `srcs`, each one's value its text.
function spelled(name: string, srcs: string[], r: string): RulePass {
  return {
    ...pass(name, [], r),
    tokens: srcs.map((src) => ({ name: '#T', val: Datum.string(src), src, row: 1, col: 1 })),
  }
}

function run(spec: TreeSpec, events: ParserEvent[]): EventRecorder {
  const out = new EventRecorder()
  const native = new TransduceTree(spec, out)
  for (const ev of events) native.event(ev)
  return out
}

describe('TransduceTree', () => {
  // The json part's spec over tabnas-json's own passes makes the document's
  // events, lexemes kept, as the incremental source makes them.
  it("the json spec makes the document's events", () => {
    for (const src of [
      '{"a":[1,2.50,"x",{"b":null}],"c":{},"d":[],"e":1e21,"f":true}',
      '[]',
      '{}',
      '[[[]],[{}]]',
      '{"a":{"b":{"c":[1,[2,[3]]]}}}',
      ' 42 ',
      '"s"',
      'null',
    ]) {
      const native = new EventRecorder()
      new ParserEventSource(makeJson(), src).run(new TransduceTree(JSON_SPEC, native))
      const incremental = new EventRecorder()
      new ParserSource(makeJson(), src)
        .grammar('json')
        .mode(SourceMode.incremental(Prune.never()))
        .run(incremental)
      assert.deepStrictEqual(native.events, incremental.events, src)
    }
  })

  // A key named by the spec is announced as it stands; a scalar from a
  // close pass's value; a rule the spec does not name yields nothing.
  it("a named key, a close's value and an unnamed rule", () => {
    const spec: TreeSpec = {
      rules: new Map<string, any>([
        ['doc', { container: 'object' }],
        ['v', { key: { name: 'k' }, scalar: 'value' }],
      ]),
    }
    const out = run(spec, [
      { type: 'open', pass: pass('doc', [], '') },
      { type: 'open', pass: pass('v', [Datum.number(9)], '') },
      { type: 'open', pass: pass('other', [Datum.number(8)], '') },
      { type: 'close', pass: pass('other', [], '', Datum.number(8)) },
      { type: 'close', pass: pass('v', [], '', Datum.number(7)) },
      { type: 'close', pass: pass('doc', [], '') },
      { type: 'end' },
    ])
    assert.deepStrictEqual(out.events, [Ev.objectStart, Ev.key('k'), Ev.number(7), Ev.objectEnd, Ev.end])
  })

  // The library's refusals, with its codes and texts. A failing pass writes
  // nothing, not even the key it would have announced.
  it("the library's refusals", () => {
    const keyed: TreeSpec = { rules: new Map<string, any>([['v', { key: { name: 'k' }, scalar: { token: 0 } }]]) }
    const out = new EventRecorder()
    let err = thrown(() => new TransduceTree(keyed, out).event({ type: 'open', pass: pass('v', [Datum.array([])], '') }))
    assert.equal(err.code, 'DSL_TYPE_ERROR')
    assert.equal(err.message, 'type_mismatch: scalar: the value must be null, a boolean, a number or a string, not a vector')
    assert.deepStrictEqual(out.events, [])
    err = thrown(() => run(JSON_SPEC, [{ type: 'open', pass: pass('pair', [Datum.bool(true)], '') }]))
    assert.equal(err.message, 'type_mismatch: key: the name must be a string, not a boolean')
    err = thrown(() => run(JSON_SPEC, [{ type: 'close', pass: pass('map', [], '') }]))
    assert.equal(err.message, 'type_mismatch: pop: the vector is empty; there is no last item to remove')
    err = thrown(() => run(JSON_SPEC, [{ type: 'open', pass: pass('list', [], '') }, { type: 'end' }]))
    assert.equal(err.code, 'PROTOCOL_ORDER_ERROR')
    assert.equal(err.message, "the rule passes ended inside a container, which a tree's never do")
  })

  // A close whose alternate replaces its rule with a container rule hands
  // the container on: one start, one end, across the two rules.
  it('a replaced container is one container', () => {
    const spec: TreeSpec = {
      rules: new Map<string, any>([
        ['a', { container: 'array' }],
        ['b', { container: 'array' }],
      ]),
    }
    const out = run(spec, [
      { type: 'open', pass: pass('a', [], '') },
      { type: 'close', pass: pass('a', [], 'b') },
      { type: 'open', pass: pass('b', [], '') },
      { type: 'close', pass: pass('b', [], '') },
      { type: 'end' },
    ])
    assert.deepStrictEqual(out.events, [Ev.arrayStart, Ev.arrayEnd, Ev.end])
  })

  // A member is announced after what its rule begins, here a container one
  // rule begins and a rule it replaces carries on; a `:text` rule's close
  // yields first the text the passes between its open and it consumed, its
  // own open's and close's tokens not among them, a rule the spec does not
  // name writing its tokens too (rs/src/transduce_tree.rs's test).
  it("a member after the start, and a rule's text", () => {
    const spec: TreeSpec = {
      rules: new Map<string, any>([
        ['row', { container: 'object', member: { name: 'a' }, text: true }],
        ['next', { container: 'object', member: { token: 0 }, text: true }],
      ]),
    }
    const out = run(spec, [
      { type: 'open', pass: spelled('row', ['<'], '') },
      { type: 'open', pass: spelled('cell', ['1', '2'], '') },
      { type: 'close', pass: spelled('cell', ['3'], '') },
      { type: 'close', pass: spelled('row', [','], 'next') },
      { type: 'open', pass: spelled('next', ['b'], '') },
      { type: 'open', pass: spelled('cell', ['x'], '') },
      { type: 'close', pass: spelled('cell', [], '') },
      { type: 'close', pass: spelled('next', ['>'], '') },
      { type: 'end' },
    ])
    assert.deepStrictEqual(out.events, [
      Ev.objectStart,
      Ev.key('a'),
      Ev.string('123'),
      Ev.key('b'),
      Ev.string('x'),
      Ev.objectEnd,
      Ev.end,
    ])
  })

  // A `:text` rule closed inside another joins its text to the one around
  // it, its own open's tokens before it and its close's after.
  it('a text joins the text around it', () => {
    const spec: TreeSpec = {
      rules: new Map<string, any>([
        ['outer', { text: true }],
        ['inner', { text: true }],
      ]),
    }
    const out = run(spec, [
      { type: 'open', pass: spelled('outer', ['['], '') },
      { type: 'open', pass: spelled('x', ['a'], '') },
      { type: 'close', pass: spelled('x', [], '') },
      { type: 'open', pass: spelled('inner', ['('], '') },
      { type: 'open', pass: spelled('x', ['b'], '') },
      { type: 'close', pass: spelled('x', [], '') },
      { type: 'close', pass: spelled('inner', [')'], '') },
      { type: 'close', pass: spelled('outer', [']'], '') },
      { type: 'end' },
    ])
    assert.deepStrictEqual(out.events, [Ev.string('b'), Ev.string('a(b)'), Ev.end])
  })

  // A text is held to the spec's bound at the library's joins, in its
  // order, and refused as its `string-join` refuses: a pass's tokens
  // together, then the innermost text with them, then a closed text with
  // the text around it; a pass that fails writes nothing, and a text at the
  // bound is not refused (rs/src/transduce_tree.rs).
  it('a text is held to its bound as the library holds it', () => {
    const spec: TreeSpec = {
      rules: new Map<string, any>([
        ['outer', { text: true }],
        ['inner', { text: true }],
      ]),
      maxTextBytes: 3,
    }
    const open = (name: string, srcs: string[]): ParserEvent => ({ type: 'open', pass: spelled(name, srcs, '') })
    const close = (name: string): ParserEvent => ({ type: 'close', pass: spelled(name, [], '') })
    const refused = (events: ParserEvent[], len: number): EventRecorder => {
      const out = new EventRecorder()
      const native = new TransduceTree(spec, out)
      const err = thrown(() => {
        for (const ev of events) native.event(ev)
      })
      assert.deepStrictEqual(err, treeTextRefusal(len, 3))
      return out
    }

    // A pass's tokens together, past the bound.
    assert.deepStrictEqual(refused([open('outer', []), open('x', ['ab', 'cd'])], 4).events, [])
    // The innermost text with a pass's tokens, its bytes counted in UTF-8.
    refused([open('outer', []), open('x', ['ab']), close('x'), open('x', ['cd'])], 4)
    refused([open('outer', []), open('x', ['é']), close('x'), open('x', ['é'])], 4)
    // A closed text with the text around it: the inner text, within the
    // bound, is not yielded when its join fails.
    const out = refused(
      [open('outer', []), open('x', ['ab']), close('x'), open('inner', []), open('x', ['cd']), close('x'), close('inner')],
      4,
    )
    assert.deepStrictEqual(out.events, [])
    // At the bound, nothing is refused.
    assert.deepStrictEqual(
      run(spec, [
        open('outer', []),
        open('x', ['a']),
        close('x'),
        open('inner', []),
        open('x', ['bc']),
        close('x'),
        close('inner'),
        close('outer'),
        { type: 'end' },
      ]).events,
      [Ev.string('bc'), Ev.string('abc'), Ev.end],
    )

    // An open pass's key is refused before its tokens' text, and its
    // tokens' text before its member.
    const fields: TreeSpec = {
      rules: new Map<string, any>([
        ['outer', { text: true }],
        ['keyed', { key: { token: 0 } }],
        ['member', { member: { token: 0 } }],
      ]),
      maxTextBytes: 3,
    }
    const wide = (name: string): ParserEvent => {
      const p = pass(name, [Datum.bool(true)], '')
      return { type: 'open', pass: { ...p, tokens: p.tokens.map((t) => ({ ...t, src: 'true' })) } }
    }
    let err = thrown(() => run(fields, [open('outer', []), wide('keyed')]))
    assert.equal(err.code, 'DSL_TYPE_ERROR')
    err = thrown(() => run(fields, [open('outer', []), wide('member')]))
    assert.deepStrictEqual(err, treeTextRefusal(4, 3))
  })

  // The library's refusals of the two fields, with its codes and texts: a
  // member whose token is not a string, before anything the pass would
  // write, and a `:text` rule's close with no text open.
  it("the library's refusals of a member and a text", () => {
    const membered: TreeSpec = {
      rules: new Map<string, any>([['row', { container: 'object', member: { token: 0 } }]]),
    }
    const out = new EventRecorder()
    let err = thrown(() =>
      new TransduceTree(membered, out).event({ type: 'open', pass: pass('row', [Datum.bool(true)], '') }),
    )
    assert.equal(err.code, 'DSL_TYPE_ERROR')
    assert.equal(err.message, 'type_mismatch: key: the name must be a string, not a boolean')
    assert.deepStrictEqual(out.events, [])
    const texted: TreeSpec = { rules: new Map<string, any>([['t', { text: true }]]) }
    err = thrown(() => run(texted, [{ type: 'close', pass: pass('t', [], '') }]))
    assert.equal(err.code, 'DSL_TYPE_ERROR')
    assert.equal(err.message, 'type_mismatch: top: the vector is empty; there is no last item')
  })
})
