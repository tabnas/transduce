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
      ['open', { name: 'val', d: 0, i: 1, alt: { p: '', r: '', b: 0, g: ['json', 'val'] }, tokens: [['#NR', 2.5, '2.50', 1, 1, 0]], forced: false, n: {} }],
      ['close', { name: 'val', d: 0, i: 1, alt: { p: '', r: '', b: 0, g: ['end', 'json'] }, tokens: [['#ZZ', null, '', 1, 5, 4]], forced: false, n: {}, value: 2.5 }],
      ['end'],
    ])
    const open = seen[0] as any
    assert.deepStrictEqual(open.pass.tokens[0].val, Datum.number(2.5, '2.50'))
  })

  // Each ignored token comes before the first pass that consumed a token
  // after it: the leading space before the list's open pass, which consumed
  // the bracket (the root's open consumed nothing, as its alternate gave the
  // bracket back), the space after the comma before the second number's
  // open, and the line break before the close that consumed the end of the
  // source. Offsets count UTF-8 bytes: the string holds a two-byte
  // character, one UTF-16 unit. rs/src/source/parser_events.rs's
  // a_sink_that_asks_gets_the_ignored_tokens_in_source_order_with_byte_offsets.
  it('a sink that asks gets the ignored tokens in source order, with byte offsets', () => {
    class Lexed extends Seen {
      wantsTokens(): boolean {
        return true
      }
    }
    const words = (events: ParserEvent[]) =>
      events
        .map((ev) =>
          'end' === ev.type
            ? 'end'
            : 'token' === ev.type
              ? `${ev.token.name}@${ev.token.offset}`
              : `${ev.pass.name}:${ev.type}${ev.pass.tokens.map((t) => `@${t.offset}`).join('')}`,
        )
        .join(' ')
    const src = ' ["é", 1]\n'
    const seen = new Lexed()
    assert.equal(new ParserEventSource(makeJson(), src).run(seen), 'continue')
    assert.equal(
      words(seen.events),
      'val:open #SP@0 list:open@1 elem:open val:open@2 val:close elem:close@6 ' +
        'elem:open #SP@7 val:open@8 val:close elem:close list:close@9 #LN@10 val:close@11 end',
    )
    // The same document to a sink that does not ask: the passes alone, their
    // offsets the same.
    assert.equal(words(passes(src)), words(seen.events).replace(/#(SP|LN)@\d+ /g, ''))
    // A source told to send them sends them to any sink.
    const told = new Seen()
    new ParserEventSource(makeJson(), src).tokens().run(told)
    assert.equal(words(told.events), words(seen.events))
    // A character of four bytes is two UTF-16 units and four bytes.
    const astral = new Lexed()
    new ParserEventSource(makeJson(), '["😀", 1]').run(astral)
    assert.equal(words(astral.events), 'val:open list:open@0 elem:open val:open@1 val:close elem:close@7 elem:open #SP@8 val:open@9 val:close elem:close list:close@10 val:close@11 end')
  })

  // DIVERGENCE.md 6: the engine's own space, line and comment tokens have no
  // value in TypeScript and Go, and their text in Rust. A part reads a token
  // item's text from its `src`.
  it('a space token item has no value', () => {
    class Lexed extends Seen {
      wantsTokens(): boolean {
        return true
      }
    }
    const seen = new Lexed()
    new ParserEventSource(makeJson(), '  1').run(seen)
    const space = seen.events.find((e) => 'token' === e.type) as any
    assert.deepStrictEqual([space.token.name, space.token.val, space.token.src], ['#SP', Datum.null, '  '])
  })

  it('a token item past max_scalar_bytes is refused by name', () => {
    class Lexed extends Seen {
      wantsTokens(): boolean {
        return true
      }
    }
    const seen = new Lexed()
    const limits = { ...Limits.default(), max_scalar_bytes: 3 }
    const err = thrown(() => new ParserEventSource(makeJson(), '[1,    2]').limits(limits).run(seen))
    assert.equal(err.code, 'RESOURCE_LIMIT_EXCEEDED')
    assert.equal(err.limit?.name, 'max_scalar_bytes')
    assert.ok(!seen.events.some((e) => 'token' === e.type))
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
