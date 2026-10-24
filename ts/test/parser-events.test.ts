/* Copyright (c) 2026 tabnas, MIT License */

// `ParserEventSource`: the ports of the Rust unit tests in
// rs/src/source/parser_events.rs that a TSV row cannot carry (a sink that
// stops or fails, the abort flag, the metrics).

import { describe, it } from 'node:test'
import assert from 'node:assert'

import { make as makeJson } from '@tabnas/json'

import {
  AbortFlag,
  Datum,
  Fail,
  Flow,
  Limits,
  Metrics,
  ParserEvent,
  ParserEventSource,
  ParserSink,
  parserEventToJSON,
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
