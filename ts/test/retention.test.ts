/* Copyright (c) 2026 tabnas, MIT License */

// Retention does not grow with the number of rows.
//
// The port of rs/tests/retention_test.rs. The table transducer holds one
// row at a time and the router one capture at a time, so the bytes
// retained at their peak depend on the largest row, never on how many rows
// there are. Ten times the rows of the same size must leave
// `captured_bytes_high` (and so `retained_bytes_high`) exactly where it
// was. The engine's tree after the run must hold no row for 200 rows and
// none for 2000 when pruning, and every row without it.

import { describe, it } from 'node:test'
import assert from 'node:assert'

import { make as makeJson } from '@tabnas/json'

import {
  Datum,
  EventRecorder,
  Limits,
  Metrics,
  ParserSource,
  Prune,
  Schema,
  Selector,
  SourceMode,
  Table,
  TableFromJson,
  columnFromMeta,
  isScalar,
} from '../dist/transduce'
import { METADATA, record } from './support'

// The worked example with `rows` copies of one record.
function sameRows(rows: number): string {
  const one = record(123_456)
  return `{"response":{"metadata":${METADATA},"payload":{"deep":{"records":[${Array(rows).fill(one).join(',')}]}}}}`
}

const recordsSelector = () =>
  Selector.root().property('response').property('payload').property('deep').property('records').eachIndex()

function run(rows: number, prune: Prune) {
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
  const { value } = new ParserSource(makeJson(), sameRows(rows))
    .grammar('json')
    .mode(SourceMode.incremental(prune))
    .metrics(metrics)
    .runWithValue(table)
  assert.ok(table.sink.ended)
  const tree = Datum.fromTabnas(value)
  const records = Datum.getPath(tree, ['response', 'payload', 'deep', 'records'])
  assert.ok(undefined !== records && 'array' === records.type)
  return {
    capturedHigh: metrics.captured_bytes_high,
    retainedHigh: metrics.retained_bytes_high,
    rows: table.sink.rows.length,
    treeRows: records.items.length,
    treeBytes: Datum.byteSize(tree),
  }
}

describe('retention', () => {
  it('ten times the rows leave the retained high water flat', () => {
    const prune = () => Prune.under(recordsSelector())
    const one = run(200, prune())
    console.log(`retention: 200 rows, captured high-water ${one.capturedHigh} bytes, tree ${one.treeBytes} bytes`)
    const ten = run(2000, prune())
    console.log(`retention: 2000 rows, captured high-water ${ten.capturedHigh} bytes, tree ${ten.treeBytes} bytes`)
    assert.deepEqual([one.rows, ten.rows], [200, 2000])
    assert.ok(one.capturedHigh > 0)
    assert.equal(ten.capturedHigh, one.capturedHigh, "the peak is one row's, not the count's")
    assert.equal(ten.retainedHigh, one.retainedHigh)
    assert.deepEqual([one.treeRows, ten.treeRows], [0, 0], "every streamed row was dropped from the engine's tree")
    assert.equal(ten.treeBytes, one.treeBytes, 'the tree left behind does not grow with the rows')
  })

  it("without pruning the engine's tree holds every row", () => {
    const one = run(200, Prune.never())
    const ten = run(2000, Prune.never())
    assert.deepEqual([one.treeRows, ten.treeRows], [200, 2000])
    assert.ok(ten.treeBytes > 9 * one.treeBytes)
    assert.equal(ten.capturedHigh, one.capturedHigh, "the router's peak is one row with or without pruning")
  })

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
    new ParserSource(makeJson(), text)
      .grammar('json')
      .mode(SourceMode.incremental(Prune.under(rows)))
      .metrics(metrics)
      .run(table)
    const rec = new EventRecorder()
    new ParserSource(makeJson(), text).run(rec)
    assert.equal(metrics.events, rec.events.length)
    assert.equal(metrics.keys, rec.events.filter((e) => 'key' === e.type).length)
    assert.equal(metrics.scalars, rec.events.filter(isScalar).length)
    assert.equal(metrics.rows, 2)
  })
})
