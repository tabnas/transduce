/* Copyright (c) 2026 tabnas, MIT License */

// `LinesSource`: JSON Lines and CSV a record (or a chunk of records) at a
// time, from a synchronous iterable of chunks or pushed chunk by chunk.
//
// The engine parses a whole string, so a document is retained at least
// once however it is consumed. Line-delimited formats do not need to be one
// document: JSON Lines is one value per line, and CSV is a header plus
// independent records, so each can be parsed a piece at a time with one
// reused parser and the memory a run needs stops depending on the input's
// size. Its events are exactly the whole-input parse's (the array of the
// per-line values, or of the records).
//
// JSON Lines: each non-blank line is parsed with one `@tabnas/json` parser.
// On the incremental path (`runIncremental`, or a writer asked for it) the
// line goes through the rule-event adapter, reset per line, so numbers
// keep their lexemes; on the walking path (`run`) the value is walked and
// numbers carry none. A line that does not parse is `INPUT_INVALID` with
// the line's number as the row.
//
// CSV: the input is cut into chunks of whole records at newlines outside
// quotes (a `"` toggles quoting, so `""` inside a field is two toggles and
// a quoted field may span lines). The header record is kept and prepended
// to every chunk after the first when `header` is on, so the reused
// `@tabnas/csv` parser names each record's fields as the whole input
// would. A chunk closes at the first record boundary at or past
// `DEFAULT_CHUNK_BYTES` (or the configured size). CSV is walked on both
// paths.
//
// Every count is in UTF-8 bytes of the input as it arrives. A single
// record larger than `max_record_bytes` (a line, for JSON Lines; the line
// ending counts) fails with that limit's name the moment it passes the
// limit, whatever its length, rather than growing a chunk without bound.

import { make as makeCsv } from '@tabnas/csv'
import { make as makeJson } from '@tabnas/json'

import { Fail, enginePosition } from './error'
import { Ev } from './event'
import { Guarded } from './guard'
import { AbortFlag, Limits, Metrics } from './limits'
import { Adapter, notStreamable, prepare } from './rule-events'
import { Flow, Sink } from './sink'
import { Prune, Source, engineFailure, walkValue } from './source'

// How much of the input one CSV parse holds, at most one record over.
export const DEFAULT_CHUNK_BYTES = 256 * 1024

// The line-delimited format to read: one JSON value per line (`jsonl`;
// blank lines are skipped), or CSV records (`csv`; `header` says whether
// the first record names the fields and overrides `options.header`; the
// other options are the grammar's, as `@tabnas/csv`'s `make` takes them).
export type LineFormat =
  | { type: 'jsonl' }
  | { type: 'csv'; header: boolean; options: Record<string, unknown> }

export const LineFormat = Object.freeze({
  jsonl(): LineFormat {
    return { type: 'jsonl' }
  },
  // CSV with a header line and the grammar's default options, or the given
  // ones.
  csv(header = true, options: Record<string, unknown> = {}): LineFormat {
    return { type: 'csv', header, options }
  },
})

// One chunk of input: text, or bytes that should be UTF-8.
export type LinesChunk = string | Uint8Array

// What a `LinesSource` reads: one text, one buffer, or a synchronous
// iterable of chunks (a generator, an array, a file read in pieces).
export type LinesInput = LinesChunk | Iterable<LinesChunk>

// The push form of a line source: hand it the input chunk by chunk with
// `write`, then call `end` once. Each returns `'stop'` once the sink has
// stopped (later calls do nothing) and throws a `Fail` on failure (later
// calls throw it again).
export interface LinesWriter {
  write(chunk: LinesChunk): Flow
  end(): Flow
}

// A line-delimited input as a source.
export class LinesSource implements Source {
  private input: LinesInput | null
  private format: LineFormat
  private sourceLimits: Limits = Limits.default()
  private abortFlag: AbortFlag = new AbortFlag()
  private sourceMetrics: Metrics = new Metrics()
  private chunkSize = DEFAULT_CHUNK_BYTES
  private consumed = false

  // `input` may be `null` for a source used only through `writer`.
  constructor(input: LinesInput | null, format: LineFormat) {
    this.input = input
    this.format = format
  }

  limits(limits: Limits): this {
    this.sourceLimits = limits
    return this
  }

  abort(abort: AbortFlag): this {
    this.abortFlag = abort
    return this
  }

  metrics(metrics: Metrics): this {
    this.sourceMetrics = metrics
    return this
  }

  // The CSV chunk size; a chunk closes at the first record boundary at or
  // past it. Zero closes a chunk at every record.
  chunkBytes(bytes: number): this {
    this.chunkSize = bytes
    return this
  }

  // Both formats through the walk; JSON Lines numbers carry no lexeme.
  run(sink: Sink): Flow {
    return this.drive(this.writer(sink, false))
  }

  // JSON Lines through the rule-event adapter (lexemes kept), CSV through
  // the walk.
  runIncremental(sink: Sink): Flow {
    return this.drive(this.writer(sink, true))
  }

  private drive(writer: LinesWriter): Flow {
    if (this.consumed) {
      throw new Fail('STREAM_REUSED', 'a LinesSource reads its input once')
    }
    if (null === this.input) {
      throw new TypeError('LinesSource: no input; use writer() to push chunks')
    }
    const input = this.input
    this.input = null
    this.consumed = true
    const chunks: Iterable<LinesChunk> =
      'string' === typeof input || input instanceof Uint8Array ? [input] : input
    for (const chunk of chunks) {
      if ('stop' === writer.write(chunk)) return 'stop'
    }
    return writer.end()
  }

  // A writer that pushes the input into `sink`, on the incremental path
  // when `incremental` is true.
  writer(sink: Sink, incremental = false): LinesWriter {
    const options: DriverOptions = {
      limits: this.sourceLimits,
      abort: this.abortFlag,
      metrics: this.sourceMetrics,
      chunkBytes: this.chunkSize,
    }
    const driver =
      'jsonl' === this.format.type
        ? incremental
          ? new JsonlIncremental(sink, options)
          : new JsonlWalk(sink, options)
        : new CsvWalk(sink, options, this.format.header, this.format.options)
    return new Writer(driver)
  }
}

type DriverOptions = {
  limits: Limits
  abort: AbortFlag
  metrics: Metrics
  chunkBytes: number
}

// What a format does with the input: `begin` before the first byte, a line
// at a time, then `finish` at the end. Each returns the flow or throws.
interface Driver {
  splitter: LineSplitter
  begin(): Flow
  line(number: number, raw: Uint8Array): Flow
  finish(): Flow
  // Called once the run is over, however it ended.
  close(): void
}

// The writer's state machine: a stop or a failure ends it for good.
class Writer implements LinesWriter {
  private driver: Driver
  private started = false
  private state: 'open' | 'stopped' | 'ended' | 'failed' = 'open'
  private failure: unknown = null

  constructor(driver: Driver) {
    this.driver = driver
  }

  private guard(step: () => Flow): Flow {
    if ('stopped' === this.state) return 'stop'
    if ('failed' === this.state) throw this.failure
    if ('ended' === this.state) throw Fail.protocol('the line source has already ended')
    try {
      if (!this.started) {
        this.started = true
        if ('stop' === this.driver.begin()) return this.finish('stopped')
      }
      const flow = step()
      if ('stop' === flow) return this.finish('stopped')
      return flow
    } catch (err) {
      this.failure = err
      this.finish('failed')
      throw err
    }
  }

  private finish(state: 'stopped' | 'ended' | 'failed'): Flow {
    this.state = state
    this.driver.close()
    return 'stop'
  }

  write(chunk: LinesChunk): Flow {
    return this.guard(() => {
      const bytes = 'string' === typeof chunk ? Buffer.from(chunk, 'utf8') : chunk
      return this.driver.splitter.feed(bytes, (n, raw) => this.driver.line(n, raw))
    })
  }

  end(): Flow {
    const flow = this.guard(() => {
      if ('stop' === this.driver.splitter.end((n, raw) => this.driver.line(n, raw))) {
        return 'stop'
      }
      return this.driver.finish()
    })
    if ('open' === this.state) {
      this.finish('ended')
      return flow
    }
    return flow
  }
}

const NEWLINE = 0x0a

// Lines from chunks of bytes, numbered from 1, each with its line ending,
// refused the moment one passes `maxBytes` (the line ending counts).
class LineSplitter {
  private pieces: Uint8Array[] = []
  private size = 0
  private number = 0
  private maxBytes: number

  constructor(maxBytes: number) {
    this.maxBytes = maxBytes
  }

  private over(): Fail {
    const number = this.number + 1
    return Fail.limit(
      'max_record_bytes',
      this.maxBytes,
      `line ${number} is longer than ${this.maxBytes} bytes`,
    ).at(number, 1)
  }

  private take(): Uint8Array {
    const line = 1 === this.pieces.length ? this.pieces[0] : Buffer.concat(this.pieces)
    this.pieces = []
    this.size = 0
    return line
  }

  feed(bytes: Uint8Array, line: (n: number, raw: Uint8Array) => Flow): Flow {
    let from = 0
    while (from < bytes.length) {
      const nl = bytes.indexOf(NEWLINE, from)
      const to = nl < 0 ? bytes.length : nl + 1
      this.size += to - from
      if (this.size > this.maxBytes) throw this.over()
      this.pieces.push(bytes.subarray(from, to))
      from = to
      if (nl < 0) break
      this.number++
      if ('stop' === line(this.number, this.take())) return 'stop'
    }
    return 'continue'
  }

  end(line: (n: number, raw: Uint8Array) => Flow): Flow {
    if (0 === this.size) return 'continue'
    this.number++
    return line(this.number, this.take())
  }
}

const DECODER = new TextDecoder('utf-8', { ignoreBOM: true })

// The index of the first byte of the first invalid UTF-8 sequence, or -1.
function utf8Invalid(b: Uint8Array): number {
  let i = 0
  const n = b.length
  while (i < n) {
    const c = b[i]
    if (c < 0x80) {
      i++
      continue
    }
    let need: number
    let min: number
    if (c >= 0xc2 && c <= 0xdf) {
      need = 1
      min = 0x80
    } else if (c >= 0xe0 && c <= 0xef) {
      need = 2
      min = 0x800
    } else if (c >= 0xf0 && c <= 0xf4) {
      need = 3
      min = 0x10000
    } else {
      return i
    }
    if (i + need >= n) return i
    let cp = c & (0xff >> (need + 2))
    for (let k = 1; k <= need; k++) {
      const d = b[i + k]
      if (undefined === d || (d & 0xc0) !== 0x80) return i
      cp = (cp << 6) | (d & 0x3f)
    }
    if (cp < min || cp > 0x10ffff || (cp >= 0xd800 && cp <= 0xdfff)) return i
    i += need + 1
  }
  return -1
}

// Bytes as text, refusing input that is not UTF-8 at its line and column.
function decode(raw: Uint8Array, number: number): string {
  const bad = utf8Invalid(raw)
  if (0 <= bad) {
    throw Fail.input(
      `line ${number} is not UTF-8: invalid utf-8 sequence from index ${bad}`,
    ).at(number, bad + 1)
  }
  return DECODER.decode(raw)
}

// A line without its line ending (`\n`, or `\r\n`).
function stripEnding(raw: Uint8Array): Uint8Array {
  let end = raw.length
  if (0 < end && NEWLINE === raw[end - 1]) {
    end--
    if (0 < end && 0x0d === raw[end - 1]) end--
  }
  return raw.subarray(0, end)
}

// Whether a line holds only white space, by Unicode's White_Space.
function isBlank(line: string): boolean {
  for (let i = 0; i < line.length; i++) {
    const c = line.charCodeAt(i)
    const space =
      (c >= 0x09 && c <= 0x0d) ||
      0x20 === c ||
      0x85 === c ||
      0xa0 === c ||
      0x1680 === c ||
      (c >= 0x2000 && c <= 0x200a) ||
      0x2028 === c ||
      0x2029 === c ||
      0x202f === c ||
      0x205f === c ||
      0x3000 === c
    if (!space) return false
  }
  return true
}

// An engine error on one line: the line's number is the row.
function lineFailure(error: unknown, line: number, abort: AbortFlag): Fail {
  const fail = engineFailure(error, abort)
  if ('ABORTED' !== fail.code) {
    fail.row = line
    fail.col = enginePosition(error)?.col ?? 0
  }
  return fail
}

// JSON Lines through the walk.
class JsonlWalk implements Driver {
  splitter: LineSplitter
  private guarded: Guarded<Sink>
  private parser: any
  private abort: AbortFlag

  constructor(sink: Sink, options: DriverOptions) {
    this.splitter = new LineSplitter(options.limits.max_record_bytes)
    this.guarded = new Guarded(sink, options.limits, options.abort, options.metrics)
    this.abort = options.abort
    this.parser = makeJson()
    const abort = options.abort
    prepare(this.parser, () => !abort.isAborted())
  }

  begin(): Flow {
    return this.guarded.event(Ev.arrayStart)
  }

  line(number: number, raw: Uint8Array): Flow {
    const line = decode(stripEnding(raw), number)
    if (isBlank(line)) return 'continue'
    let value: unknown
    try {
      value = this.parser.parse(line)
    } catch (err) {
      throw lineFailure(err, number, this.abort)
    }
    return walkValue(value, this.guarded)
  }

  finish(): Flow {
    if ('stop' === this.guarded.event(Ev.arrayEnd)) return 'stop'
    return this.guarded.event(Ev.end)
  }

  close(): void {
    this.guarded.flush()
  }
}

// JSON Lines through the rule-event adapter: one parser, one subscriber,
// the adapter reset before each line.
class JsonlIncremental implements Driver {
  splitter: LineSplitter
  private adapter: Adapter<Sink>
  private parser: any
  private abort: AbortFlag

  constructor(sink: Sink, options: DriverOptions) {
    this.splitter = new LineSplitter(options.limits.max_record_bytes)
    const stop = new AbortFlag()
    const abort = options.abort
    this.abort = abort
    const adapter = new Adapter(sink, options.limits, abort, options.metrics, Prune.never(), stop)
    this.adapter = adapter
    this.parser = makeJson()
    prepare(
      this.parser,
      () => !abort.isAborted() && !stop.isAborted(),
      (rule, done) => adapter.onDone(rule, done),
    )
  }

  // The adapter's own outcome, when it stopped or failed inside a parse.
  private status(): Flow | null {
    const status = this.adapter.status
    if ('failed' === status.type) throw status.error
    if ('stopped' === status.type) return 'stop'
    return null
  }

  begin(): Flow {
    return this.adapter.send(Ev.arrayStart)
  }

  line(number: number, raw: Uint8Array): Flow {
    const line = decode(stripEnding(raw), number)
    if (isBlank(line)) return 'continue'
    let parsed: { ok: true; value: unknown } | { ok: false; error: unknown }
    try {
      parsed = { ok: true, value: this.parser.parse(line) }
    } catch (error) {
      parsed = { ok: false, error }
    }
    const status = this.status()
    if (null !== status) return status
    const adapter = this.adapter
    if (!parsed.ok) throw lineFailure(parsed.error, number, this.abort)
    if (adapter.complete()) {
      adapter.reset()
      return 'continue'
    }
    if (adapter.idle()) {
      if ('stop' === adapter.walkWhole(parsed.value)) return 'stop'
      adapter.reset()
      return 'continue'
    }
    throw notStreamable()
  }

  finish(): Flow {
    const status = this.status()
    if (null !== status) return status
    if ('stop' === this.adapter.send(Ev.arrayEnd)) return 'stop'
    return this.adapter.send(Ev.end)
  }

  close(): void {
    this.adapter.sink.flush()
  }
}

// CSV through the walk: lines into whole records, records into chunks, each
// chunk parsed with one reused parser.
class CsvWalk implements Driver {
  splitter: LineSplitter
  private guarded: Guarded<Sink>
  private parser: any
  private abort: AbortFlag
  private maxRecordBytes: number
  private chunkBytes: number
  private wantHeader: boolean

  // The record being read: its text, bytes, first line, and whether a
  // quote is open.
  private record = ''
  private recordBytes = 0
  private recordLine = 0
  private inQuotes = false

  // The header record's text, once read.
  private header: string | null = null
  private headerBytes = 0

  // The chunk being filled: its text and bytes, the lines before its first
  // record (1 when the header is in the text), and its first record's line.
  private chunk = ''
  private chunkSize = 0
  private prefixLines = 0
  private firstLine = 0

  constructor(
    sink: Sink,
    options: DriverOptions,
    header: boolean,
    grammar: Record<string, unknown>,
  ) {
    this.splitter = new LineSplitter(options.limits.max_record_bytes)
    this.guarded = new Guarded(sink, options.limits, options.abort, options.metrics)
    this.abort = options.abort
    this.maxRecordBytes = options.limits.max_record_bytes
    this.chunkBytes = options.chunkBytes
    this.wantHeader = header
    this.parser = makeCsv({ ...grammar, header } as any)
    const abort = options.abort
    prepare(this.parser, () => !abort.isAborted())
  }

  begin(): Flow {
    return this.guarded.event(Ev.arrayStart)
  }

  line(number: number, raw: Uint8Array): Flow {
    const text = decode(raw, number)
    if (0 === this.recordBytes) this.recordLine = number
    for (let i = 0; i < raw.length; i++) {
      if (0x22 === raw[i]) this.inQuotes = !this.inQuotes
    }
    this.record += text
    this.recordBytes += raw.length
    if (this.recordBytes > this.maxRecordBytes) {
      throw Fail.limit(
        'max_record_bytes',
        this.maxRecordBytes,
        `the record starting at line ${this.recordLine} is longer than ${this.maxRecordBytes} bytes`,
      ).at(this.recordLine, 1)
    }
    if (this.inQuotes) return 'continue'
    return this.completeRecord()
  }

  // One whole record is read: it is the header, or it joins the chunk.
  private completeRecord(): Flow {
    const text = this.record
    const bytes = this.recordBytes
    const line = this.recordLine
    this.record = ''
    this.recordBytes = 0
    this.inQuotes = false
    if (this.wantHeader && null === this.header) {
      // The first chunk carries the header as its own first record.
      this.header = text
      this.headerBytes = bytes
      this.chunk = text
      this.chunkSize = bytes
      this.prefixLines = 1
      return 'continue'
    }
    if (0 === this.firstLine) {
      if (0 === this.chunkSize && null !== this.header) {
        this.chunk = this.header
        this.chunkSize = this.headerBytes
        this.prefixLines = 1
      }
      this.firstLine = line
    }
    this.chunk += text
    this.chunkSize += bytes
    if (this.chunkSize >= this.chunkBytes) return this.flush()
    return 'continue'
  }

  // Parse the chunk and walk its records.
  private flush(): Flow {
    if (0 === this.firstLine) return 'continue'
    const text = this.chunk
    const firstLine = this.firstLine
    const prefixLines = this.prefixLines
    this.chunk = ''
    this.chunkSize = 0
    this.prefixLines = 0
    this.firstLine = 0
    let value: unknown
    try {
      value = this.parser.parse(text)
    } catch (err) {
      const pos = enginePosition(err)
      const rowInChunk = Math.max(pos?.row ?? 0, 1)
      // An error in the prepended header itself is the file's first line.
      const line = rowInChunk > prefixLines ? firstLine + rowInChunk - prefixLines - 1 : 1
      throw lineFailure(err, line, this.abort)
    }
    // The grammar returns an array of records; anything else has none.
    const records = Array.isArray(value) ? value : []
    for (const record of records) {
      if ('stop' === walkValue(record, this.guarded)) return 'stop'
    }
    return 'continue'
  }

  finish(): Flow {
    // An unterminated quote at the end of the input stays in the text for
    // the parser to report as the grammar does.
    if (0 < this.recordBytes && 'stop' === this.completeRecord()) return 'stop'
    if ('stop' === this.flush()) return 'stop'
    if ('stop' === this.guarded.event(Ev.arrayEnd)) return 'stop'
    return this.guarded.event(Ev.end)
  }

  close(): void {
    this.guarded.flush()
  }
}
