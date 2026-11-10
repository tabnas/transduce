/* Copyright (c) 2026 tabnas, MIT License */

// `ParserEventSource.fromReader` (admin ADR-30 and ADR-33): the rule passes
// of a source the parser reads from a reader, against the same text read
// whole. The port of rs/tests/reader_test.rs. Every committed fixture of a
// grammar the harness builds is handed over a few bytes at a time. JSON's,
// JSONC's and JSON Lines' grammars, whose matchers are the engine's own,
// are given `lex.window` and are read a window at a time; every other
// grammar reads the reader whole first. The passes, the value and the
// outcome must be the same, with token items and without, and a reader
// that fails, or bytes that are not UTF-8, are `INPUT_INVALID` with the
// engine's `unreadable`. Every token's byte offset is where its text
// stands in the document. The pruning, the abort flag, the limits and the
// metrics hold from a reader as they do over a text.

import { describe, it } from 'node:test'
import assert from 'node:assert'
import { readFileSync, readdirSync } from 'node:fs'
import { join } from 'node:path'

import {
  AbortFlag,
  Fail,
  Flow,
  Limits,
  Metrics,
  ParserEvent,
  ParserEventSource,
  ParserSink,
  parserEventToJSON,
} from '../dist/transduce'
import { ByteOffsets } from '../dist/parser-event-source'
import { GRAMMARS } from './common'
import { recordsJson } from './support'

// A reader that hands its bytes over at most `size` at a time.
class Pieces {
  private at = 0

  constructor(
    private readonly bytes: Uint8Array,
    private readonly size: number,
  ) {}

  read(buf: Uint8Array): number {
    const n = Math.min(this.size, buf.length, this.bytes.length - this.at)
    buf.set(this.bytes.subarray(this.at, this.at + n))
    this.at += n
    return n
  }
}

// A reader that hands over `good` bytes, a few at a time, and then fails.
class Failing {
  private at = 0

  constructor(
    private readonly bytes: Uint8Array,
    private readonly good: number,
  ) {}

  read(buf: Uint8Array): number {
    if (this.at >= this.good) throw new Error('the disk went away')
    const n = Math.min(3, buf.length, this.good - this.at)
    buf.set(this.bytes.subarray(this.at, this.at + n))
    this.at += n
    return n
  }
}

// The passes a run delivered, in the fixture encoding. With `abort`, the
// sink raises the flag once it has seen `abortAt` events; with `tokens`, it
// asks for token items.
class Passes implements ParserSink {
  seen: unknown[] = []

  constructor(
    private readonly abort?: AbortFlag,
    private readonly abortAt = 0,
    private readonly tokens = false,
  ) {}

  event(ev: ParserEvent): Flow {
    this.seen.push(parserEventToJSON(ev))
    if (undefined !== this.abort && this.seen.length === this.abortAt) this.abort.abort()
    return 'continue'
  }

  wantsTokens(): boolean {
    return this.tokens
  }
}

// Every token's byte offset is where its text stands in `text`, or, for a
// token whose text is not there (one a grammar makes, such as the YAML
// document marker it supplies at the end), the place its row and column
// name: a pass's tokens, `[name, val, src, row, col, offset]`, and a token
// item's. rs/tests/reader_test.rs's `offsets_hold`.
function offsetsHold(file: string, text: string, passes: unknown[]): void {
  const bytes = utf8(text)
  const lines = text.split('\n')
  const place = (row: number, col: number): number => {
    let at = 0
    for (let r = 1; r < row && r - 1 < lines.length; r++) at += utf8(lines[r - 1]).length + 1
    const line = Array.from(lines[row - 1] ?? '')
    return Math.min(bytes.length, at + utf8(line.slice(0, col - 1).join('')).length)
  }
  const check = (src: string, row: number, col: number, offset: number, what: unknown) => {
    const want = utf8(src)
    const there = offset + want.length <= bytes.length && want.every((b, i) => b === bytes[offset + i])
    assert.ok(
      there || offset === place(row, col),
      `${file}: byte ${offset} is neither where ${JSON.stringify(src)} is nor where its row and column are: ${JSON.stringify(what)}`,
    )
  }
  for (const pass of passes as any[]) {
    if ('token' === pass[0]) check(pass[1].src, pass[1].row, pass[1].col, pass[1].offset, pass)
    else if ('open' === pass[0] || 'close' === pass[0]) {
      for (const t of pass[1].tokens) check(t[2], t[3], t[4], t[5], t)
    }
  }
}

// The grammar a fixture's extension names, and whether its matchers are the
// engine's own, so that it may be given `lex.window`.
function grammarFor(file: string): [string, boolean] | undefined {
  const ext = file.slice(file.lastIndexOf('.') + 1)
  switch (ext) {
    case 'json':
    case 'jsonc':
    case 'jsonl':
      return [ext, true]
    case 'json5':
    case 'jsonic':
    case 'yaml':
    case 'zon':
    case 'csv':
    case 'toml':
    case 'ini':
    case 'xml':
    case 'pgn':
      return [ext, false]
    case 'md':
      return ['markdown', false]
    case 'rss':
      return ['feed', false]
  }
  return undefined
}

function parser(name: string, window: boolean): any {
  const make = GRAMMARS[name]
  if (undefined === make) throw new Error(`no grammar ${name} in the harness`)
  const parser = make()
  // Set either way: a grammar's own constructor may declare the window
  // (@tabnas/json's make() does from 0.5.20), and a test of the read-whole
  // route must turn it off.
  parser.options({ lex: { window } })
  return parser
}

// What a run showed: its outcome, its passes and the value it returned.
function run(
  source: ParserEventSource,
  sink: Passes = new Passes(),
): { outcome: string; passes: unknown[]; value: unknown } {
  try {
    const { flow, value } = source.runWithValue(sink)
    return { outcome: flow, passes: sink.seen, value }
  } catch (err) {
    if (!(err instanceof Fail)) throw err
    const outcome = `${err.code} ${err.message} ${err.row} ${err.col} ${err.limit?.name}`
    return { outcome, passes: sink.seen, value: undefined }
  }
}

const FIXTURES_DIR = join(__dirname, '..', '..', 'rs', 'tests', 'fixtures')

const utf8 = (text: string) => new TextEncoder().encode(text)

describe('ParserEventSource from a reader', () => {
  it('every fixture streams the same from a reader', () => {
    let compared = 0
    for (const file of readdirSync(FIXTURES_DIR).sort()) {
      const named = grammarFor(file)
      if (undefined === named) continue
      const [name, window] = named
      const text = readFileSync(join(FIXTURES_DIR, file), 'utf8')
      for (const tokens of [false, true]) {
        const whole = run(new ParserEventSource(parser(name, window), text), new Passes(undefined, 0, tokens))
        offsetsHold(file, text, whole.passes)
        if (!tokens) {
          assert.ok(!whole.passes.some((p: any) => 'token' === p[0]), `${file}: token items no sink asked for`)
        }
        for (const size of [1, 3, 64]) {
          const reader = new Pieces(utf8(text), size)
          const read = run(ParserEventSource.fromReader(parser(name, window), reader), new Passes(undefined, 0, tokens))
          assert.deepStrictEqual(read, whole, `${file} in pieces of ${size}, tokens ${tokens}`)
          compared++
        }
      }
    }
    console.log(`reader: ${compared} runs from a reader compared with the text read whole`)
    assert.ok(compared >= 120, String(compared))
  })

  it('a long document from a reader keeps its byte offsets past the text it drops', () => {
    // Comments and strings of characters of one, two, three and four UTF-8
    // bytes (two UTF-16 units for the last), well past the 64 KiB the
    // offsets keep behind their slower cursor, read a window at a time in
    // pieces of several sizes: each token's offset is where its text is,
    // and the run is the run over the text.
    const records: string[] = []
    for (let i = 0; 4000 > i; i++) {
      records.push(`  /* ${i} é 日 😀 */ {"k${i}": "a é 日 😀 ${i}"} // ü\n`)
    }
    const text = `[\n${records.join(',')}]\n`
    assert.ok(text.length > 200000, String(text.length))
    for (const tokens of [false, true]) {
      const whole = run(new ParserEventSource(parser('jsonc', true), text), new Passes(undefined, 0, tokens))
      assert.equal(whole.outcome, 'continue')
      offsetsHold('long.jsonc', text, whole.passes)
      for (const size of [7, 4096, 65536]) {
        const read = run(
          ParserEventSource.fromReader(parser('jsonc', true), new Pieces(utf8(text), size)),
          new Passes(undefined, 0, tokens),
        )
        assert.deepStrictEqual(read, whole, `long.jsonc in pieces of ${size}, tokens ${tokens}`)
      }
    }
  })

  it('a run that asks for token items keeps a few blocks of text when the items stop', () => {
    // White space before a long compact array, over 2 MB, read a window at
    // a time: a token item at the start, one at the end, and none between,
    // so the offsets' cursor for the token items stands still while the
    // passes' moves on. The text the offsets keep stays within a few of
    // their 64 KiB blocks: before the repair (tabnas/transduce#79's review)
    // the items' cursor held the document from the first space on.
    const strings: string[] = []
    for (let i = 0; 2200 > i; i++) strings.push(JSON.stringify(`${i} ${'a é 日 😀 '.repeat(70)}`))
    const text = ` [${strings.join(',')}]\n`
    assert.ok(utf8(text).length > 2000000, String(utf8(text).length))
    let kept = 0
    const feed = ByteOffsets.prototype.feed
    ByteOffsets.prototype.feed = function (this: any, bytes: Uint8Array): void {
      feed.call(this, bytes)
      kept = Math.max(kept, this.text.length)
    }
    let read
    try {
      read = run(
        ParserEventSource.fromReader(parser('json', true), new Pieces(utf8(text), 65536)),
        new Passes(undefined, 0, true),
      )
    } finally {
      ByteOffsets.prototype.feed = feed
    }
    assert.equal(read.outcome, 'continue')
    const items = read.passes.filter((p: any) => 'token' === p[0])
    assert.deepEqual(
      items.map((p: any) => [p[1].name, p[1].offset]),
      [
        ['#SP', 0],
        ['#LN', utf8(text).length - 1],
      ],
    )
    offsetsHold('compact.json', text, read.passes)
    assert.ok(kept < 4 * 65536, `the offsets kept ${kept} UTF-16 units of a document of ${text.length}`)
    const whole = run(new ParserEventSource(parser('json', true), text), new Passes(undefined, 0, true))
    assert.deepStrictEqual(read, whole)
  })

  it('an unreadable source is invalid input', () => {
    const text = utf8('[1,\n2,\n3333,\n4]')
    for (const window of [true, false]) {
      const passes = new Passes()
      const fail = failure(() =>
        ParserEventSource.fromReader(parser('json', window), new Failing(text, 9)).run(passes),
      )
      assert.equal(fail.code, 'INPUT_INVALID', String(fail))
      assert.ok(fail.message.startsWith('unreadable: '), fail.message)
      assert.ok(fail.message.includes('the disk went away'), fail.message)
      assert.deepEqual([fail.row, fail.col], [3, 3])
      // Read whole first, the source fails before any pass.
      if (!window) assert.deepEqual(passes.seen, [])
    }
    // Bytes that are not UTF-8 end the source where they stand.
    for (const window of [true, false]) {
      const bytes = new Uint8Array([0x5b, 0x31, 0x2c, 0xff, 0x5d])
      const fail = failure(() =>
        ParserEventSource.fromReader(parser('json', window), new Pieces(bytes, 2)).run(new Passes()),
      )
      assert.equal(fail.code, 'INPUT_INVALID', String(fail))
      assert.ok(fail.message.startsWith('unreadable: '), fail.message)
      assert.ok(fail.message.includes('stream did not contain valid UTF-8'), fail.message)
      assert.deepEqual([fail.row, fail.col], [1, 4])
    }
    assert.throws(
      () => ParserEventSource.fromReader(parser('json', false), {} as any),
      (err: any) => err instanceof TypeError && /read\(buffer\)/.test(err.message),
    )
  })

  it('pruning, the abort flag, the limits and the metrics hold from a reader as over a text', () => {
    const text = recordsJson(50)
    const sources = (window: boolean, configure: (source: ParserEventSource) => ParserEventSource) => [
      configure(new ParserEventSource(parser('json', window), text)),
      configure(ParserEventSource.fromReader(parser('json', window), new Pieces(utf8(text), 5))),
    ]
    for (const window of [true, false]) {
      // Pruning: the same passes, and the same tree left behind, its arrays
      // and, json's being listed in capability.PRUNES_MAPS, its maps emptied.
      const [overText, fromReader] = sources(window, (s) => s.grammar('json').prune()).map((s) => run(s))
      assert.equal(overText.outcome, 'continue')
      assert.deepStrictEqual(fromReader, overText)
      assert.deepEqual(JSON.parse(JSON.stringify(overText.value)), {})

      // The abort flag: raised by the sink part way, the run is ABORTED
      // after the same passes.
      const aborted = sources(window, (s) => s).map((s) => {
        const abort = new AbortFlag()
        return run(s.abort(abort), new Passes(abort, 7))
      })
      assert.ok(aborted[0].outcome.startsWith('ABORTED '), aborted[0].outcome)
      assert.equal(aborted[0].passes.length, 7)
      assert.deepStrictEqual(aborted[1], aborted[0])

      // A limit: refused by name before the sink sees the pass.
      const limited = sources(window, (s) => s.limits(Limits.with({ max_scalar_bytes: 8 }))).map((s) => run(s))
      assert.ok(
        limited[0].outcome.startsWith('RESOURCE_LIMIT_EXCEEDED ') && limited[0].outcome.endsWith(' max_scalar_bytes'),
        limited[0].outcome,
      )
      assert.deepStrictEqual(limited[1], limited[0])

      // The metrics count the events delivered.
      const counted = sources(window, (s) => s).map((s) => {
        const metrics = new Metrics()
        const ran = run(s.metrics(metrics))
        return [metrics.events, ran.passes.length]
      })
      assert.ok(0 < counted[0][0])
      assert.deepEqual(counted[1], counted[0])
      assert.equal(counted[0][0], counted[0][1])
    }
  })
})

function failure(fn: () => unknown): Fail {
  try {
    fn()
  } catch (err) {
    if (err instanceof Fail) return err
    throw err
  }
  throw new Error('expected a failure')
}
