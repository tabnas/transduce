/* Copyright (c) 2026 tabnas, MIT License */

// The metadata-first table transducer: `JsonEvents/1` in, `TableRows/1`
// out.
//
// `TableFromJson` is a `Sink` built on a `Router` with at most two
// captures: the column metadata, when the schema comes from the document,
// and the rows. The schema must be known before the first row is emitted,
// and the transducer holds one row at a time. So metadata that has not
// completed when a row BEGINS is an `INPUT_ORDER_VIOLATION` raised at the
// row's start, through the router's `began` hook, before a byte of the row
// is retained; metadata that arrives twice is the same failure; and a
// document with no rows is a valid empty table, its schema emitted just
// before `end`. Rows are projected into schema order by path, so the order
// of members inside a row never matters, and a number keeps the lexeme the
// source events carried.

import {
  BoundColumn,
  CaptureId,
  CaptureSpec,
  Cell,
  ColumnMapper,
  Datum,
  Duplicates,
  Fail,
  Flow,
  JsonEvent,
  Limits,
  Metrics,
  NODE_BYTES,
  Path,
  PublicColumn,
  RouteSink,
  Selected,
  Selector,
  Sink,
  TableBinding,
  TableSink,
  boundColumn,
  getPath,
  utf8Bytes,
} from '@tabnas/alchemy/shared'

import { Router } from './route'

// Where the columns are, once known.
type Columns =
  | { type: 'static'; bound: BoundColumn[] }
  | { type: 'metadata'; selector: Selector; column: ColumnMapper; bound: BoundColumn[] | null }
  | { type: 'infer'; bound: BoundColumn[] | null }

// The route sink behind the transducer: it owns the schema state and the
// table sink.
class Core<S extends TableSink> implements RouteSink {
  readonly sink: S
  rowId: CaptureId
  metaId: CaptureId | null
  columns: Columns
  schemaSent = false
  private maxColumns: number
  // The bound on the inferred columns' names, the table's metadata when
  // the first row supplies it.
  private maxMetadataBytes: number
  private metrics: Metrics

  constructor(
    sink: S,
    rowId: CaptureId,
    metaId: CaptureId | null,
    columns: Columns,
    limits: Limits,
    metrics: Metrics,
  ) {
    this.sink = sink
    this.rowId = rowId
    this.metaId = metaId
    this.columns = columns
    this.maxColumns = limits.max_columns
    this.maxMetadataBytes = limits.max_metadata_bytes
    this.metrics = metrics
  }

  bind(columns: BoundColumn[], from: string): void {
    if (columns.length > this.maxColumns) {
      throw Fail.limit(
        'max_columns',
        this.maxColumns,
        `${from} declares ${columns.length} columns, more than ${this.maxColumns}`,
      )
    }
    this.columns.bound = columns
  }

  private publicColumns(): PublicColumn[] {
    return (this.columns.bound ?? []).map((c) => ({ label: c.label }))
  }

  private sendSchema(): Flow {
    this.schemaSent = true
    return this.sink.tableEvent({ type: 'schema', columns: this.publicColumns() })
  }

  private metadata(selected: Selected): void {
    const columns = this.columns
    if ('metadata' !== columns.type) {
      // The router only has a metadata capture when the binding selects one.
      throw Fail.protocol('metadata was delivered to a static schema')
    }
    const at = selected.path.toString()
    if (null !== columns.bound) {
      throw new Fail(
        'INPUT_ORDER_VIOLATION',
        `the column metadata at ${at} was selected twice; a table has one schema`,
      ).atPath(at)
    }
    const value = selected.value ?? Datum.null
    if ('array' !== value.type) {
      throw Fail.input(`the column metadata at ${at} is not an array`).atPath(at)
    }
    const descriptors = value.items
    if (descriptors.length > this.maxColumns) {
      throw Fail.limit(
        'max_columns',
        this.maxColumns,
        `the metadata at ${at} declares ${descriptors.length} columns, more than ` +
          `${this.maxColumns}`,
      ).atPath(at)
    }
    const bound = descriptors.map((d, i) => {
      try {
        return columns.column(d)
      } catch (err) {
        if (err instanceof Fail && null == err.path) {
          const p = selected.path.clone()
          p.push(i)
          err.path = p.toString()
        }
        throw err
      }
    })
    this.bind(bound, 'the metadata')
  }

  private row(selected: Selected): Flow {
    const row = selected.value ?? Datum.null
    if (!this.schemaSent) {
      if ('infer' === this.columns.type && null === this.columns.bound) {
        const at = selected.path.toString()
        if ('object' !== row.type) {
          throw Fail.input(
            `the first row at ${at} is not an object, so no columns can be inferred from it`,
          ).atPath(at)
        }
        // The names are the table's metadata for as long as it lasts, so
        // they are held to the bound a metadata capture is: measured as the
        // array of their strings would be.
        let bytes = NODE_BYTES
        for (const k of row.members.keys()) bytes += NODE_BYTES + utf8Bytes(k)
        if (bytes > this.maxMetadataBytes) {
          throw Fail.limit(
            'max_metadata_bytes',
            this.maxMetadataBytes,
            `the first row's ${row.members.size} member names take ${bytes} bytes as the ` +
              `table's columns, more than ${this.maxMetadataBytes}`,
          ).atPath(at)
        }
        this.bind(
          [...row.members.keys()].map((k) => boundColumn(k, [k])),
          'the first row',
        )
      }
      if ('stop' === this.sendSchema()) return 'stop'
    }
    const columns = this.columns.bound
    if (null === columns) {
      throw Fail.protocol('a row was projected before its schema was bound')
    }
    const cells: Cell[] = []
    for (const col of columns) {
      const found = getPath(row, col.source)
      if (undefined !== found) {
        cells.push(Cell.fromDatum(found))
        continue
      }
      switch (col.missing) {
        case 'missing':
          cells.push(Cell.missing)
          break
        case 'null':
          cells.push(Cell.null)
          break
        case 'error': {
          const at = new Path([...selected.path.segments, ...col.source]).toString()
          throw new Fail(
            'MISSING_VALUE',
            `column ${JSON.stringify(col.label)} has no value at ${at}`,
          ).atPath(at)
        }
      }
    }
    this.metrics.rows++
    return this.sink.tableEvent({ type: 'row', cells })
  }

  began(id: CaptureId, _tag: string): void {
    if (id === this.rowId && 'metadata' === this.columns.type && null === this.columns.bound) {
      throw new Fail(
        'INPUT_ORDER_VIOLATION',
        `a row began before the column metadata at ${this.columns.selector} had completed; ` +
          `rows must follow their metadata`,
      )
    }
  }

  selected(selected: Selected): Flow {
    if (selected.id === this.metaId) {
      this.metadata(selected)
      return 'continue'
    }
    return this.row(selected)
  }

  end(): Flow {
    if (!this.schemaSent) {
      if ('metadata' === this.columns.type && null === this.columns.bound) {
        const at = this.columns.selector.toString()
        throw Fail.input(`the document has no column metadata at ${at}`).atPath(at)
      }
      // No rows: nothing to infer from, so the table is empty.
      if ('infer' === this.columns.type && null === this.columns.bound) {
        this.bind([], 'the binding')
      }
      if ('stop' === this.sendSchema()) return 'stop'
    }
    return this.sink.tableEvent({ type: 'end' })
  }
}

// The table transducer. A `Sink` for one document's events; the table
// events go to the wrapped `TableSink` as the rows arrive.
export class TableFromJson<S extends TableSink> implements Sink {
  private router: Router<Core<S>>

  // Build the transducer. Rows are materialized under `max_record_bytes`,
  // metadata under `max_metadata_bytes`; a rows selector that may overlap
  // the metadata selector is refused as the router refuses any overlapping
  // materializations.
  constructor(
    binding: TableBinding,
    limits: Limits,
    duplicates: Duplicates,
    metrics: Metrics,
    sink: S,
  ) {
    const rowSpec = CaptureSpec.materialize('row', binding.rows).withBudget(
      limits.max_record_bytes,
      'max_record_bytes',
    )
    let columns: Columns
    let specs: CaptureSpec[]
    let metaId: CaptureId | null = null
    const schema = binding.schema
    switch (schema.type) {
      case 'static':
        columns = { type: 'static', bound: schema.columns.slice() }
        specs = [rowSpec]
        break
      case 'metadata':
        columns = {
          type: 'metadata',
          selector: schema.columns,
          column: schema.column,
          bound: null,
        }
        specs = [
          CaptureSpec.materialize('metadata', schema.columns).withBudget(
            limits.max_metadata_bytes,
            'max_metadata_bytes',
          ),
          rowSpec,
        ]
        metaId = 0
        break
      case 'infer':
        columns = { type: 'infer', bound: null }
        specs = [rowSpec]
        break
    }
    const core = new Core(sink, specs.length - 1, metaId, columns, limits, metrics)
    if ('static' === columns.type) core.bind(columns.bound, 'the binding')
    this.router = new Router(specs, limits, duplicates, metrics, core)
  }

  get sink(): S {
    return this.router.downstream.sink
  }

  // Whether the table's `end` has been emitted.
  ended(): boolean {
    return this.router.ended()
  }

  event(ev: JsonEvent): Flow {
    return this.router.event(ev)
  }
}
