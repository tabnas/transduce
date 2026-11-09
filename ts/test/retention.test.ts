/* Copyright (c) 2026 tabnas, MIT License */

// Retention does not grow with the number of rows.
//
// The port of rs/tests/retention_test.rs. The table transducer holds one
// row at a time and the router one capture at a time, so the bytes
// retained at their peak depend on the largest row, never on how many rows
// there are. Ten times the rows of the same size must leave
// `captured_bytes_high` (and so `retained_bytes_high`) exactly where it
// was.
//
// The rows reach the table from the parse of a JSON document, its value
// walked (`ParserSource`), through the source's guard, which counts the
// events as a source's guard counts them. The transducer's peak does not
// depend on how the events were made: a host that streams a format through
// its transducer part (alchemy's text over `ParserEventSource`'s passes,
// pruned) hands the same events to the same stages. What pruning leaves of
// the engine's tree is ./prune.test.ts's to hold flat, for every grammar;
// dmog, which runs the parts, holds a part's run to it.

import { describe, it } from 'node:test'
import assert from 'node:assert'

import { make as makeJson } from '@tabnas/json'

import {
  EventRecorder,
  Limits,
  Metrics,
  ParserSource,
  Schema,
  Selector,
  Table,
  TableFromJson,
  columnFromMeta,
  isScalar,
} from '../dist/transduce'
import { METADATA, record } from './support'

// The worked example with `rows` copies of one record, so every row has the
// same size and the peak is the row's, not the count's.
function sameRows(rows: number): string {
  const one = record(123_456)
  return `{"response":{"metadata":${METADATA},"payload":{"deep":{"records":[${Array(rows).fill(one).join(',')}]}}}}`
}

const recordsSelector = () =>
  Selector.root().property('response').property('payload').property('deep').property('records').eachIndex()

// The table over the document's events, the parsed value walked; `metrics`
// counts the events once, at the source's guard in front of the table.
function run(rows: number) {
  const metrics = new Metrics()
  const table = new TableFromJson(
    {
      schema: Schema.fromMetadata(
        Selector.root().property('response').property('metadata').property('fields'),
        columnFromMeta,
      ),
      rows: recordsSelector(),
    },
    Limits.default(),
    'reject',
    metrics,
    new Table(),
  )
  new ParserSource(makeJson(), sameRows(rows)).metrics(metrics).run(table)
  assert.ok(table.sink.ended)
  return {
    capturedHigh: metrics.captured_bytes_high,
    retainedHigh: metrics.retained_bytes_high,
    rows: table.sink.rows.length,
  }
}

describe('retention', () => {
  it('ten times the rows leave the retained high water flat', () => {
    const one = run(200)
    console.log(`retention: 200 rows, captured high-water ${one.capturedHigh} bytes`)
    const ten = run(2000)
    console.log(`retention: 2000 rows, captured high-water ${ten.capturedHigh} bytes`)
    assert.deepEqual([one.rows, ten.rows], [200, 2000])
    assert.ok(one.capturedHigh > 0)
    assert.equal(ten.capturedHigh, one.capturedHigh, "the peak is one row's, not the count's")
    assert.equal(ten.retainedHigh, one.retainedHigh)
  })

  // The README's chain shares one `Metrics` between the source and the
  // table transducer; the source counts are the source's, once.
  it('a chain sharing one Metrics counts the source events once', () => {
    const text = '{"meta":[{"title":"Id","path":["id"]}],"rows":[{"id":1},{"id":2}]}'
    const rows = Selector.root().property('rows').eachIndex()
    const metrics = new Metrics()
    const table = new TableFromJson(
      { schema: Schema.fromMetadata(Selector.root().property('meta'), columnFromMeta), rows },
      Limits.default(),
      'reject',
      metrics,
      new Table(),
    )
    new ParserSource(makeJson(), text).metrics(metrics).run(table)
    const rec = new EventRecorder()
    new ParserSource(makeJson(), text).run(rec)
    assert.equal(metrics.events, rec.events.length)
    assert.equal(metrics.keys, rec.events.filter((e) => 'key' === e.type).length)
    assert.equal(metrics.scalars, rec.events.filter(isScalar).length)
    assert.equal(metrics.rows, 2)
  })
})
