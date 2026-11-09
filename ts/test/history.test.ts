/* Copyright (c) 2026 tabnas, MIT License */

// A source bounds the engine's rule history (`STREAM_RULE_HISTORY`).
//
// The engine links each rule to the rule it replaced and the rule that
// pushed it, and the TypeScript engine keeps the whole chain by default:
// a parse of a long flat document keeps every rule it made reachable until
// the document ends, whatever the stream prunes, and its memory grows with
// the document's length (tabnas/parser doc/rule-history-bound.md). A
// ParserEventSource bounds the history at three predecessors, the most any
// grammar in the fleet reads and what the Rust engine bounds it at by
// default, unless the grammar or the host bounded it already. Read from a
// reader and pruned, ten times the records then leave the heap's high
// water where it was. The port of this file is go/history_test.go.

import { describe, it } from 'node:test'
import assert from 'node:assert'
import v8 from 'node:v8'
import vm from 'node:vm'

import { make as makeJson } from '@tabnas/json'

import { Flow, ParserEvent, ParserEventSource, ParserSink, STREAM_RULE_HISTORY } from '../dist/transduce'

v8.setFlagsFromString('--expose_gc')
const gc: () => void = vm.runInNewContext('gc')

// A sink that collects garbage every `every` passes and keeps the heap in
// use then: what the parse holds live at those points.
class HeapSampler implements ParserSink {
  passes = 0
  high = 0
  constructor(private readonly every: number) {}
  event(_ev: ParserEvent): Flow {
    this.passes++
    if (0 === this.passes % this.every) {
      gc()
      this.high = Math.max(this.high, process.memoryUsage().heapUsed)
    }
    return 'continue'
  }
}

// One record whose strings are the same in every record and whose numbers
// are not, as go/history_test.go's: the Go lexer keeps every distinct short
// string a parse meets, which is the engine's to bound (tabnas/parser#324),
// and the two tests measure what a source bounds, the rules, over the same
// documents.
function streamRecord(i: number): string {
  return `{"id":${i},"person":{"name":"Person"},"account":{"balance":${i * 7}.${String(i % 100).padStart(2, '0')}}}`
}

// A JSON array of `records` records, made as it is read, so the document
// itself is never held whole.
function recordsReader(records: number): { read(buf: Uint8Array): number } {
  const encoder = new TextEncoder()
  let next = 0
  let pending = encoder.encode('[')
  let at = 0
  return {
    read(buf: Uint8Array): number {
      let n = 0
      while (n < buf.length) {
        if (at === pending.length) {
          if (next > records) break
          pending = encoder.encode(next === records ? ']' : (0 < next ? ',' : '') + streamRecord(next))
          at = 0
          next++
        }
        const take = Math.min(buf.length - n, pending.length - at)
        buf.set(pending.subarray(at, at + take), n)
        n += take
        at += take
      }
      return n
    },
  }
}

function mb(bytes: number): string {
  return (bytes / (1024 * 1024)).toFixed(1)
}

describe('rule history', () => {
  it('a source bounds a history the grammar left unbounded', () => {
    const parser = makeJson()
    assert.equal(parser.options.rule?.history ?? null, null)
    new ParserEventSource(parser, '[1,2]').run(new HeapSampler(Number.MAX_SAFE_INTEGER))
    assert.equal(parser.options.rule.history, STREAM_RULE_HISTORY)
    assert.equal(STREAM_RULE_HISTORY, 3)
  })

  it('a history already bounded is left as it is', () => {
    const parser = makeJson()
    parser.options({ rule: { history: 5 } })
    new ParserEventSource(parser, '[1,2]').run(new HeapSampler(Number.MAX_SAFE_INTEGER))
    assert.equal(parser.options.rule.history, 5)
  })

  // The records' arrays are emptied behind the stream (`prune`), the
  // document comes from a reader, so the engine holds a window of it, and
  // the history is bounded: nothing the parse keeps grows with the records.
  it('ten times the records leave the heap high water where it was', () => {
    const highWater = (records: number): { high: number; passes: number } => {
      const sink = new HeapSampler(5000)
      ParserEventSource.fromReader(makeJson(), recordsReader(records)).grammar('json').prune().run(sink)
      return { high: sink.high, passes: sink.passes }
    }
    const few = highWater(2000)
    console.log(`history: 1 of 2 (50%) 2,000 records, ${few.passes} passes, heap high water ${mb(few.high)} MB`)
    const many = highWater(20000)
    console.log(`history: 2 of 2 (100%) 20,000 records, ${many.passes} passes, heap high water ${mb(many.high)} MB`)
    assert.ok(9 * few.passes < many.passes, `${few.passes} then ${many.passes} passes`)
    assert.ok(many.high < 1.5 * few.high, `${mb(few.high)} MB then ${mb(many.high)} MB`)
  })
})
