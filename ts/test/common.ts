/* Copyright (c) 2026 tabnas, MIT License */

// The harness behind the shared fixtures in ../../test/spec, which every
// runtime of this package runs.
//
// What a row means is documented in docs/reference.md ("Shared fixtures"):
// which source its `grammar` and `mode` name, what each JSON column decodes
// to, and how the result is encoded as the fixture's expected JSON. This
// harness reproduces exactly that, as rs/tests/common/mod.rs does for
// Rust; nothing here is specific to TypeScript except the calls into this
// package's API.

import { Tabnas } from '@tabnas/parser'
import { jsonic } from '@tabnas/jsonic'
import { make as makeJson } from '@tabnas/json'
import { make as makeJsonl } from '@tabnas/jsonl'
import { Json5 } from '@tabnas/json5'
import { Jsonc } from '@tabnas/jsonc'
import { Yaml } from '@tabnas/yaml'
import { Zon } from '@tabnas/zon'
import { make as makeCsv } from '@tabnas/csv'
import { Toml } from '@tabnas/toml'
import { Ini } from '@tabnas/ini'
import { Xml } from '@tabnas/xml'
import { Markdown } from '@tabnas/markdown'
import { Feed } from '@tabnas/feed'
import { findSpecDir, parseExpect, equalValue, formatValue } from '@tabnas/support'

import {
  BoundColumn,
  CaptureSpec,
  Cell,
  Code,
  Datum,
  Duplicates,
  EventRecorder,
  Fail,
  Flow,
  JsonEvent,
  LineFormat,
  Limits,
  LinesSource,
  Metrics,
  ParserSource,
  Prune,
  RouteSink,
  Router,
  ScanEmit,
  Schema,
  Segment,
  Selected,
  Selector,
  Sink,
  SourceMode,
  TableBinding,
  TableEvent,
  TableFromJson,
  TableSink,
  Transition,
  ValueSource,
  columnFromMeta,
} from '../dist/transduce'

// A fixture row, as @tabnas/support's loader hands it over.
export type Row = {
  named(name: string): string
  where(): string
}

// The shared `test/spec` directory, found by walking up from here.
export function specDir(): string {
  return findSpecDir(__dirname)
}

// The fixture files, each with its runner: a new file without a runner
// fails `every fixture has a runner` rather than passing unread.
export const FIXTURES = [
  'events.tsv',
  'limits.tsv',
  'lines.tsv',
  'route.tsv',
  'scan.tsv',
  'table.tsv',
]

// Every grammar the harness and the differential suite can build, by its
// package's name, as a fresh parser per call (a source owns its parser).
export const GRAMMARS: Record<string, () => any> = {
  json: () => makeJson(),
  jsonl: () => makeJsonl(),
  json5: () => new Tabnas().use(jsonic).use(Json5),
  jsonc: () => new Tabnas().use(jsonic).use(Jsonc),
  jsonic: () => new Tabnas().use(jsonic),
  yaml: () => new Tabnas().use(jsonic).use(Yaml),
  zon: () => new Tabnas().use(jsonic).use(Zon),
  csv: () => makeCsv(),
  toml: () => new Tabnas().use(jsonic).use(Toml),
  ini: () => new Tabnas().use(jsonic).use(Ini),
  xml: () => new Tabnas().use(jsonic).use(Xml),
  markdown: () => new Tabnas().use(Markdown),
  feed: () => new Tabnas().use(jsonic).use(Feed),
}

// The grammar a row names, as a fresh parser.
export function grammar(name: string): any {
  const make = GRAMMARS[name]
  if (undefined === make) throw new Error(`no grammar ${JSON.stringify(name)} in the harness`)
  return make()
}

// A run that failed: the failure, and for an event stream the events that
// left before it, which a row may pin in its `prefix` column.
export class Failed {
  fail: unknown
  prefix: unknown

  constructor(fail: unknown, prefix?: unknown) {
    this.fail = fail
    this.prefix = prefix
  }
}

// A failure as the shared runner sees it: the code, and the position when
// the failure has one. A row may also pin the failure's `path`, the
// `limit` it names and the `prefix` of events emitted before it; when one
// differs, the code is reported with the difference attached, so the row
// fails with it in its report.
export function toFailure(failed: unknown, row: Row): Error {
  const f = failed instanceof Failed ? failed : new Failed(failed)
  const fail = f.fail
  if (!(fail instanceof Fail)) {
    // Not a failure of this package's: a bug, reported as it is.
    return fail instanceof Error ? fail : new Error(String(fail))
  }
  const differences: string[] = []
  const wantPath = row.named('path')
  if ('' !== wantPath) {
    const got = fail.path ?? '<none>'
    if (got !== wantPath) differences.push(`path ${got}, the fixture pins ${wantPath}`)
  }
  const wantLimit = row.named('limit')
  if ('' !== wantLimit) {
    const got = fail.limit?.name ?? '<none>'
    if (got !== wantLimit) differences.push(`limit ${got}, the fixture pins ${wantLimit}`)
  }
  const wantPrefix = row.named('prefix')
  if ('' !== wantPrefix) {
    const want = parseExpect(wantPrefix)
    if (undefined === f.prefix || !equalValue(f.prefix, want)) {
      differences.push(
        `prefix ${undefined === f.prefix ? '<none>' : formatValue(f.prefix)}, ` +
          `the fixture pins ${formatValue(want)}`,
      )
    }
  }
  let code: string = fail.code
  if (0 < differences.length) code = `${code} (${differences.join('; ')})`
  const err: any = new Error(fail.toString())
  err.code = code
  if (null != fail.row && null != fail.col) {
    err.row = fail.row
    err.col = fail.col
  }
  return err
}

// Run a stage and turn its failure into the runner's error.
export function runRow(row: Row, stage: () => unknown): unknown {
  try {
    return stage()
  } catch (err) {
    throw toFailure(err, row)
  }
}

// ---------------------------------------------------------------------
// Reading the columns.

function jsonCell(row: Row, name: string): any {
  const cell = row.named(name)
  if ('' === cell) return undefined
  try {
    return JSON.parse(cell)
  } catch (err) {
    throw new Error(`${row.where()}: column ${name} is not JSON: ${err}: ${cell}`)
  }
}

// A selector from its JSON steps: a string is a property, a non-negative
// integer an index, `{"each":"index"}` every element, `{"each":"member"}`
// every member value.
export function selector(steps: unknown): Selector {
  if (!Array.isArray(steps)) throw new Error(`a selector is an array of steps: ${steps}`)
  let sel = Selector.root()
  for (const step of steps) {
    if ('string' === typeof step) sel = sel.property(step)
    else if ('number' === typeof step) sel = sel.index(step)
    else if ('index' === step?.each) sel = sel.eachIndex()
    else if ('member' === step?.each) sel = sel.eachMember()
    else throw new Error(`not a selector step: ${JSON.stringify(step)}`)
  }
  return sel
}

// A concrete path from its JSON segments: a string is a key, a
// non-negative integer an index.
export function segments(json: unknown): Segment[] {
  if (!Array.isArray(json)) throw new Error(`a path is an array of segments: ${json}`)
  return json.map((seg) => {
    if ('string' === typeof seg) return seg
    if ('number' === typeof seg && Number.isInteger(seg) && seg >= 0) return seg
    throw new Error(`not a path segment: ${JSON.stringify(seg)}`)
  })
}

// `Limits.default()` with the row's `limits` column applied over it.
export function limits(row: Row): Limits {
  const out = Limits.default()
  const json = jsonCell(row, 'limits')
  if (undefined === json) return out
  for (const [name, value] of Object.entries(json)) {
    if (!(name in out)) throw new Error(`${row.where()}: no limit ${JSON.stringify(name)}`)
    if ('number' !== typeof value || !Number.isInteger(value) || value < 0) {
      throw new Error(`${row.where()}: limit ${name} is not a count`)
    }
    ;(out as any)[name] = value
  }
  return out
}

// The row's `duplicates` policy: `reject` (the default), `last_wins` or
// `first_wins`.
export function duplicates(row: Row): Duplicates {
  const d = row.named('duplicates')
  if ('' === d || 'reject' === d) return 'reject'
  if ('last_wins' === d || 'first_wins' === d) return d
  throw new Error(`${row.where()}: no duplicates policy ${JSON.stringify(d)}`)
}

function prune(row: Row): Prune {
  const json = jsonCell(row, 'prune')
  if (undefined === json) return Prune.never()
  if ('all' === json) return Prune.allArrays()
  return Prune.under(selector(json))
}

// The line format and chunk size from the row's `options` column:
// `header` (default true), `object`, `number`, `value`, `trim` and
// `strict` for the CSV grammar, and `chunk_bytes` for the source.
function lineOptions(row: Row): { format: LineFormat; chunk?: number } {
  const json = jsonCell(row, 'options') ?? {}
  const chunk = 'number' === typeof json.chunk_bytes ? json.chunk_bytes : undefined
  const name = row.named('grammar')
  if ('jsonl' === name) return { format: LineFormat.jsonl(), chunk }
  if ('csv' === name) {
    const options: Record<string, unknown> = {}
    for (const flag of ['object', 'strict', 'number', 'value', 'trim']) {
      if ('boolean' === typeof json[flag]) options[flag] = json[flag]
    }
    const header = 'boolean' === typeof json.header ? json.header : true
    return { format: LineFormat.csv(header, options), chunk }
  }
  throw new Error(`${row.where()}: no line format for grammar ${JSON.stringify(name)}`)
}

// ---------------------------------------------------------------------
// Driving a source.

// Run the row's source into `sink`: `grammar` and `mode` name it.
//
// - `materialize` and `incremental`: `ParserSource` over the grammar,
//   named with `.grammar`, in that mode (`prune` column for incremental).
// - `value`: the grammar's parse (an engine error is `Fail.fromTabnas`),
//   then `ValueSource` over the value. No limits apply on this path.
// - `lines` and `lines-incremental`: `LinesSource` over the text, the
//   walking path (`run`) and the incremental path (`runIncremental`).
export function drive(row: Row, input: string, sink: Sink): Flow {
  const name = row.named('grammar')
  const mode = row.named('mode')
  switch (mode) {
    case 'materialize':
    case 'incremental':
      return new ParserSource(grammar(name), input)
        .grammar(name)
        .mode('materialize' === mode ? SourceMode.materialize() : SourceMode.incremental(prune(row)))
        .limits(limits(row))
        .run(sink)
    case 'value': {
      let value: unknown
      try {
        value = grammar(name).parse(input)
      } catch (err) {
        throw Fail.fromTabnas(err)
      }
      return new ValueSource(value).run(sink)
    }
    case 'lines':
    case 'lines-incremental': {
      const { format, chunk } = lineOptions(row)
      const source = new LinesSource(Buffer.from(input, 'utf8'), format).limits(limits(row))
      if (undefined !== chunk) source.chunkBytes(chunk)
      return 'lines' === mode ? source.run(sink) : source.runIncremental(sink)
    }
    default:
      throw new Error(`${row.where()}: no mode ${JSON.stringify(mode)}`)
  }
}

// ---------------------------------------------------------------------
// The encodings.

function number(value: number, lexeme: string | null): unknown[] {
  return ['number', value, lexeme]
}

// One `JsonEvents/1` event: `["object_start"]`, `["key", name]`,
// `["number", value, lexeme or null]`, `["end"]` and so on.
export function event(ev: JsonEvent): unknown {
  switch (ev.type) {
    case 'key':
      return ['key', ev.key]
    case 'bool':
      return ['bool', ev.value]
    case 'number':
      return number(ev.value, ev.lexeme)
    case 'string':
      return ['string', ev.value]
    default:
      return [ev.type]
  }
}

// A retained value: `null`, booleans and strings as themselves, `["number",
// value, lexeme or null]`, `["array", item...]` and `["object", [key,
// value]...]`, members in their order.
export function datum(d: Datum): unknown {
  switch (d.type) {
    case 'null':
      return null
    case 'bool':
    case 'string':
      return d.value
    case 'number':
      return number(d.value, d.lexeme)
    case 'array':
      return ['array', ...d.items.map(datum)]
    case 'object':
      return ['object', ...[...d.members].map(([k, v]) => [k, datum(v)])]
  }
}

// A table cell: as a datum's scalars, and `["missing"]`.
export function cell(c: Cell): unknown {
  switch (c.type) {
    case 'null':
      return null
    case 'bool':
    case 'string':
      return c.value
    case 'number':
      return number(c.value, c.lexeme)
    case 'missing':
      return ['missing']
  }
}

// ---------------------------------------------------------------------
// The stages.

// The row's events, recorded.
export function events(row: Row, input: string): unknown {
  const recorder = new EventRecorder()
  try {
    drive(row, input, recorder)
  } catch (err) {
    throw new Failed(err, recorder.events.map(event))
  }
  return recorder.events.map(event)
}

// What a route delivered: `[tag, path]` for an observed capture, `[tag,
// path, value]` for a materialized one, and `"end"` when the router called
// `end`.
class Deliveries implements RouteSink {
  out: unknown[] = []

  selected(selected: Selected): Flow {
    const entry: unknown[] = [selected.tag, selected.path.toString()]
    if (null !== selected.value) entry.push(datum(selected.value))
    this.out.push(entry)
    return 'continue'
  }

  end(): Flow {
    this.out.push('end')
    return 'continue'
  }
}

// The row's `captures` column: an array of `{"tag", "select", "mode"}`.
function captures(row: Row): CaptureSpec[] {
  const json = jsonCell(row, 'captures') ?? []
  return json.map((spec: any) => {
    const select = selector(spec.select)
    if (undefined === spec.mode || 'materialize' === spec.mode) {
      return CaptureSpec.materialize(spec.tag, select)
    }
    if ('observe' === spec.mode) return CaptureSpec.observe(spec.tag, select)
    throw new Error(`${row.where()}: no capture mode ${JSON.stringify(spec.mode)}`)
  })
}

// The row's source through a `Router` over its captures.
export function route(row: Row, input: string): unknown {
  const deliveries = new Deliveries()
  const router = new Router(captures(row), limits(row), duplicates(row), new Metrics(), deliveries)
  drive(row, input, router)
  return deliveries.out
}

// `TableRows/1`, recorded in order.
class TableLog implements TableSink {
  out: unknown[] = []

  tableEvent(ev: TableEvent): Flow {
    switch (ev.type) {
      case 'schema':
        this.out.push(['schema', ev.columns.map((c) => c.label)])
        break
      case 'row':
        this.out.push(['row', ev.cells.map(cell)])
        break
      case 'end':
        this.out.push(['end'])
        break
    }
    return 'continue'
  }
}

// The row's `binding` column.
function binding(row: Row): TableBinding {
  const json = jsonCell(row, 'binding')
  if (undefined === json) throw new Error(`${row.where()}: a table row has a binding`)
  const rows = selector(json.rows)
  const schema = json.schema
  if ('infer' === schema) return { schema: Schema.infer(), rows }
  if (null != schema && 'object' === typeof schema && 'metadata' in schema) {
    return { schema: Schema.fromMetadata(selector(schema.metadata), columnFromMeta), rows }
  }
  if (null != schema && 'object' === typeof schema && 'static' in schema) {
    const columns: BoundColumn[] = schema.static.map((c: any) => {
      const missing = c.missing ?? 'missing'
      if (!['missing', 'null', 'error'].includes(missing)) {
        throw new Error(`no missing policy ${JSON.stringify(missing)}`)
      }
      return { label: c.label, source: segments(c.source), missing }
    })
    return { schema: Schema.static(columns), rows }
  }
  throw new Error(`${row.where()}: not a schema: ${JSON.stringify(schema)}`)
}

// The row's source through `TableFromJson` over its binding.
export function table(row: Row, input: string): unknown {
  const log = new TableLog()
  const transducer = new TableFromJson(binding(row), limits(row), duplicates(row), new Metrics(), log)
  drive(row, input, transducer)
  return log.out
}

// The stage a `limits.tsv` row names in its `stage` column.
export function staged(row: Row, input: string): unknown {
  const stage = row.named('stage')
  switch (stage) {
    case 'events':
      return events(row, input)
    case 'route':
      return route(row, input)
    case 'table':
      return table(row, input)
    default:
      throw new Error(`${row.where()}: no stage ${JSON.stringify(stage)}`)
  }
}

// ---------------------------------------------------------------------
// scan-emit.

type Item = { add: number } | { emit: string[] } | { fail: Code }

// A `scan.tsv` script through `ScanEmit`: a running sum whose step emits
// `+n` for an integer item, the given strings for `{"emit": [...]}` (the
// state unchanged), and fails with the code `{"fail": CODE}` names; the
// finish emits `=<sum>`. `"finish"` calls `finish`. The output sink answers
// `stop` for the output the `stop_on` column names. The result is `{"out":
// [output...], "flows": ["continue" | "stop", one per op]}`.
export function scan(row: Row, script: string): unknown {
  const ops = JSON.parse(script)
  const stopOn = row.named('stop_on')
  const out: string[] = []
  const operator = new ScanEmit<number, Item, string>(
    0,
    (sum, item) => {
      if ('add' in item) return Transition.emit(sum + item.add, `+${item.add}`)
      if ('emit' in item) return Transition.of(sum, item.emit)
      throw new Fail(item.fail, 'the step failed')
    },
    (sum) => [`=${sum}`],
    (output) => {
      out.push(output)
      return output === stopOn ? 'stop' : 'continue'
    },
  )
  const flows: Flow[] = []
  for (const op of ops) {
    if ('finish' === op) flows.push(operator.finish())
    else if ('number' === typeof op) flows.push(operator.item({ add: op }))
    else if (Array.isArray(op?.emit)) flows.push(operator.item({ emit: op.emit }))
    else if ('string' === typeof op?.fail) {
      const code = Code.parse(op.fail)
      if (undefined === code) throw new Error(`not a code: ${op.fail}`)
      flows.push(operator.item({ fail: code }))
    } else throw new Error(`${row.where()}: not a scan op: ${JSON.stringify(op)}`)
  }
  return { out, flows }
}

// ---------------------------------------------------------------------
// Running a fixture file.

// A row this runtime cannot pass for a reason of the runtime's, recorded in
// DIVERGENCE.md: the row's escape-decoded input and its `mode` (or stage),
// and why. A skipped row is reported as skipped, by this reason, never
// dropped silently; a skip that matches no row fails the run.
export type Skip = { input: string; mode: string; why: string }

// Run one fixture file through @tabnas/support's runner, the input in
// column `input` and the expected value in `expected`, every row through
// `stage` except the `skips`.
export function runFixture(
  file: string,
  input: string,
  stage: (input: string, row: Row) => unknown,
  skips: Skip[] = [],
): void {
  const { join } = require('node:path')
  const { describe, it } = require('node:test')
  const assert = require('node:assert')
  const { loadSpec, makeRunner } = require('@tabnas/support')
  const spec = loadSpec(join(specDir(), file))
  const used = new Set<Skip>()
  const kept = spec.rows.filter((row: any) => {
    const text = row.unescNamed(input)
    const mode = row.named('mode') || row.named('stage')
    const skip = skips.find((s) => s.input === text && s.mode === mode)
    if (undefined === skip) return true
    used.add(skip)
    describe('spec: ' + file + ' (runtime divergence)', () => {
      it(`row ${row.line}: ${JSON.stringify(text)}`, { skip: skip.why }, () => {})
    })
    return false
  })
  describe('spec: ' + file + ' skips', () => {
    it('every skip names a row', () => {
      assert.deepEqual(
        skips.filter((s) => !used.has(s)),
        [],
        'a skip that matches no row is stale; remove it',
      )
    })
  })
  spec.rows = kept
  makeRunner({
    input,
    expected: 'expected',
    parse: (text: string, row: Row) => runRow(row, () => stage(text, row)),
  }).spec(spec)
}
