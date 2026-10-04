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
// Both formats cut the input where the grammar ends a record, reading the
// text as the grammar's own lexer does (with the line characters, quotes,
// separators and comments its parser's resolved options set), and nothing
// read is left unparsed but a blank JSON Lines record, which the grammar
// skips too.
//
// JSON Lines: each record is parsed with one `@tabnas/json` parser. A
// record is the text between line characters outside a string, so a lone
// `\r` ends one as `\n` does. A record of nothing but the grammar's spaces
// (a space or a tab) is blank and skipped; any other is parsed, so a line
// holding a form feed or a no-break space fails as the grammar fails it.
// On the incremental path (`runIncremental`, or a writer asked for it) the
// record goes through the rule-event adapter, reset per record, so numbers
// keep their lexemes; on the walking path (`run`) the value is walked and
// numbers carry none. A record that does not parse is `INPUT_INVALID` with
// its line's number as the row.
//
// CSV: the input is cut into chunks of whole records. A quote opens a
// quoted field only where the lexer starts a token (a record's start, after
// the field separator or a space), so a quote inside a field is that
// field's text; the configured quote reads `""` as one quote; a quoted
// field, a backtick string and a block comment may span lines; and a line
// character anywhere else, a lone `\r` as much as `\n`, ends a record. The
// header record, the first record the grammar reads (a blank or comment
// line before it is none), is kept and prepended to every chunk after the
// first when `header` is on, so the reused `@tabnas/csv` parser names each
// record's fields as the whole input would. A chunk closes at the first
// record boundary at or past `DEFAULT_CHUNK_BYTES` (or the configured
// size), so one chunk, and never a fraction of a record, is what a parse
// holds. An input that ends inside a quoted field fails with the grammar's
// `unterminated_string`, in the header as anywhere. CSV is walked on both
// paths.
//
// The input is read a piece at a time, and a piece ends just past each of
// the grammar's line endings: one of its line characters (a lone `\r` or a
// configured separator as much as `\n`), a `\r\n`, or under `record.empty`
// the whole line token the lexer reads. A line character inside one of the
// grammar's fixed tokens (a field separator such as `"\n~"`) is the token's
// and ends no piece. So a record never waits for a `\n` to end, and a chunk
// can close after any record, however many of them one `\n`-terminated line
// holds. Where the input so far ends inside what may be one line ending,
// the piece waits for the next chunk of it.
//
// Every count is in UTF-8 bytes of the input as it arrives. A single
// record larger than `max_record_bytes` (a record with its line ending; for
// the CSV header, everything up to its end) fails with that limit's name
// the moment it passes the limit, whatever its length, rather than growing
// a chunk without bound.

// The grammars are optional peers. A program that builds no JSON Lines or
// CSV line source needs neither installed (render reads events, never
// lines), so each is loaded when a line source starts, not when this
// package is, and a run without it fails as a usage error naming it.
type CsvModule = typeof import('@tabnas/csv')
type JsonModule = typeof import('@tabnas/json')

function grammar<T>(name: string, format: string, load: () => T): T {
  try {
    return load()
  } catch (error: any) {
    // Only the grammar itself missing, not something it requires.
    if (
      'MODULE_NOT_FOUND' === error?.code &&
      String(error.message).startsWith(`Cannot find module '${name}'`)
    ) {
      throw new TypeError(
        `LinesSource: ${format} needs ${name}, an optional peer of ` +
          `@tabnas/transduce; install it alongside`,
      )
    }
    throw error
  }
}

function makeCsv(options: Record<string, unknown>): any {
  return grammar('@tabnas/csv', 'CSV', (): CsvModule => require('@tabnas/csv')).make(
    options as any,
  )
}

function makeJson(): any {
  return grammar('@tabnas/json', 'JSON Lines', (): JsonModule => require('@tabnas/json')).make()
}

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

// What a format does with the input: `begin` before the first byte, a piece
// at a time, then `finish` at the end. Each returns the flow or throws.
interface Driver {
  splitter: PieceSplitter
  begin(): Flow
  piece(row: number, text: string, bytes: number): Flow
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
  private onPiece: PieceFn

  constructor(driver: Driver) {
    this.driver = driver
    this.onPiece = (row, text, bytes) => this.driver.piece(row, text, bytes)
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
      return this.driver.splitter.feed(bytes, this.onPiece)
    })
  }

  end(): Flow {
    const flow = this.guard(() => {
      if ('stop' === this.driver.splitter.end(this.onPiece)) return 'stop'
      return this.driver.finish()
    })
    if ('open' === this.state) {
      this.finish('ended')
      return flow
    }
    return flow
  }
}

// One piece of the input: the row it starts on, its text, and its length
// in UTF-8 bytes.
type PieceFn = (row: number, text: string, bytes: number) => Flow

const EMPTY = new Uint8Array(0)
const ENCODER = new TextEncoder()

// The characters of one of the engine's resolved character maps while it
// lexes their kind, else none.
function lexed(on: boolean, chars: Record<string, unknown> | undefined): string[] {
  return on && chars ? Object.keys(chars) : []
}

// The input in pieces, each ending just past a line ending: one of the
// grammar's line characters, a `\r\n`, or under `line.single`
// (`record.empty`) the whole line token the lexer reads, every line
// character up to a repeated one. A line character inside one of the
// grammar's fixed tokens (a field separator such as `"\n~"`) is the token's
// and ends no piece, and where a fixed token holds one, a piece takes the
// whole run of line characters the lexer reads as one token, so that no
// piece starts inside a line token. A record therefore never ends inside a
// piece, only at its end. Each piece carries the row the engine gives its
// first character: one more than the row characters before it. Where the
// input so far ends inside what may be a line ending, the piece waits for
// more.
class PieceSplitter {
  // The bytes of the piece being read, and any after it not yet taken.
  private pending: Uint8Array = EMPTY
  // How much of `pending` has been searched for a line ending.
  private scanned = 0
  // Once a line ending is found: where its token ends so far, and its
  // characters.
  private tokenEnd = -1
  private token: string[] = []
  // Where the next piece starts: its row, and its byte offset in it.
  private row = 1
  private offset = 0
  private readonly line: string[]
  private readonly forms: Uint8Array[]
  private readonly ends = new Uint8Array(256)
  private readonly single: boolean
  // The fixed tokens that hold a line character, as UTF-8.
  private readonly spanning: Uint8Array[]
  private readonly rows: string[]
  // How many more bytes the piece being read may take, and the failure
  // when it takes more, given the row it starts on: the bound is the
  // record's, so the format that reads the records sets both.
  budget: () => number = () => Infinity
  over: (row: number) => Fail = (row) => Fail.input(`line ${row} is too long`).at(row, 1)

  constructor(line: string[], rows: string[], single: boolean, fixed: string[]) {
    // The engine keeps a character outside the basic plane as its two
    // UTF-16 halves, each a line character; the input holds it as one
    // four-byte form, which ends a piece when both halves are line
    // characters.
    this.line = line.filter((c) => !isHigh(c) && !isLow(c))
    for (const h of line.filter(isHigh)) {
      for (const l of line.filter(isLow)) this.line.push(h + l)
    }
    this.forms = this.line.map((c) => ENCODER.encode(c))
    for (const form of this.forms) this.ends[form[form.length - 1]] = 1
    this.single = single
    this.spanning = fixed
      .filter((token) => Array.from({ length: token.length }, (_, i) => token[i]).some((c) => line.includes(c)))
      .map((token) => ENCODER.encode(token))
    this.rows = rows
  }

  // The row the next piece starts on.
  get nextRow(): number {
    return this.row
  }

  feed(bytes: Uint8Array, piece: PieceFn): Flow {
    if (0 === bytes.length) return 'continue'
    this.pending = 0 === this.pending.length ? bytes : Buffer.concat([this.pending, bytes])
    return this.drain(piece, false)
  }

  end(piece: PieceFn): Flow {
    if ('stop' === this.drain(piece, true)) return 'stop'
    if (0 === this.pending.length) return 'continue'
    const raw = this.pending
    this.pending = EMPTY
    this.scanned = 0
    return this.emit(raw, piece)
  }

  private drain(piece: PieceFn, atEnd: boolean): Flow {
    for (;;) {
      if (this.tokenEnd < 0 && !this.findEnding(atEnd)) {
        if (this.pending.length > this.budget()) throw this.over(this.row)
        return 'continue'
      }
      if (!this.followToken(atEnd)) return 'continue'
      const raw = this.pending.subarray(0, this.tokenEnd)
      this.pending = this.pending.subarray(this.tokenEnd)
      this.scanned = 0
      this.tokenEnd = -1
      this.token = []
      if ('stop' === this.emit(raw, piece)) return 'stop'
    }
  }

  // Finds the first line ending past `scanned`, checked against the line
  // character's whole UTF-8 form, and starts its token. False while the
  // input so far holds none, or cannot yet tell where one ends.
  private findEnding(atEnd: boolean): boolean {
    const p = this.pending
    for (let i = this.scanned; i < p.length; i++) {
      if (!this.ends[p[i]]) continue
      const k = this.formEnding(i + 1)
      if (k < 0) continue
      const at = i + 1 - this.forms[k].length
      const inside = this.inFixed(at, k, atEnd)
      if (true === inside) continue
      if (null === inside) {
        this.scanned = at
        return false
      }
      this.tokenEnd = i + 1
      this.token = [this.line[k]]
      if (this.tokenEnd > this.budget()) throw this.over(this.row)
      return true
    }
    this.scanned = p.length
    return false
  }

  // Whether the `k`th line character, at `at`, lies inside one of the fixed
  // tokens that hold a line character; null while the input so far cannot
  // tell.
  private inFixed(at: number, k: number, atEnd: boolean): boolean | null {
    const p = this.pending
    const form = this.forms[k]
    let unsure = false
    for (const token of this.spanning) {
      for (let j = 0; j + form.length <= token.length; j++) {
        if (!sameBytes(token, j, form, 0, form.length)) continue
        const start = at - j
        if (start < 0 || !sameBytes(p, start, token, 0, j)) continue
        const after = at + form.length
        const rest = token.length - j - form.length
        const have = Math.min(rest, p.length - after)
        if (!sameBytes(p, after, token, j + form.length, have)) continue
        if (have === rest) return true
        if (!atEnd) unsure = true
      }
    }
    return unsure ? null : false
  }

  // The line character whose UTF-8 form `pending` holds just before `end`,
  // by its index, or -1.
  private formEnding(end: number): number {
    const p = this.pending
    next: for (let k = 0; k < this.forms.length; k++) {
      const form = this.forms[k]
      if (end < form.length) continue
      for (let j = 0; j < form.length; j++) {
        if (p[end - form.length + j] !== form[j]) continue next
      }
      return k
    }
    return -1
  }

  // Takes the rest of the line token the piece ends with: under
  // `line.single`, every line character not yet in it, as the lexer reads
  // one; where a fixed token holds a line character, every line character,
  // as the lexer reads a run; otherwise a `\n` after a `\r`, so that `\r\n`
  // is one line ending and a record is held to its own line. False while
  // the input so far cannot tell whether the token goes on.
  private followToken(atEnd: boolean): boolean {
    const p = this.pending
    for (;;) {
      let next = -1
      let partial = false
      for (let k = 0; k < this.line.length && next < 0; k++) {
        const c = this.line[k]
        const goesOn = this.single
          ? !this.token.includes(c)
          : 0 < this.spanning.length || (1 === this.token.length && '\r' === this.token[0] && '\n' === c)
        if (!goesOn) continue
        const form = this.forms[k]
        const have = p.length - this.tokenEnd
        let prefix = true
        for (let j = 0; j < Math.min(have, form.length); j++) {
          if (p[this.tokenEnd + j] !== form[j]) {
            prefix = false
            break
          }
        }
        if (!prefix) continue
        if (have >= form.length) next = k
        else partial = true
      }
      if (0 <= next) {
        this.tokenEnd += this.forms[next].length
        this.token.push(this.line[next])
        if (this.tokenEnd > this.budget()) throw this.over(this.row)
        continue
      }
      return !partial || atEnd
    }
  }

  private emit(raw: Uint8Array, piece: PieceFn): Flow {
    const row = this.row
    const bad = utf8Invalid(raw)
    if (0 <= bad) {
      const valid = DECODER.decode(raw.subarray(0, bad))
      const [badRow, offset] = advance(this.rows, this.row, this.offset, valid)
      throw Fail.input(`line ${badRow} is not UTF-8 from its byte ${offset + 1}`).at(
        badRow,
        offset + 1,
      )
    }
    const text = DECODER.decode(raw)
    ;[this.row, this.offset] = advance(this.rows, this.row, this.offset, text)
    return piece(row, text, raw.length)
  }
}

// The row, and the byte offset in it, just after `text`, which starts at
// `row` and `offset`: a row character starts the next row. The engine reads
// a UTF-16 code unit at a time, so a row character outside the basic plane
// is its two halves, and counts as both.
function advance(rows: string[], row: number, offset: number, text: string): [number, number] {
  for (let i = 0; i < text.length; i++) {
    const c = text[i]
    if (rows.includes(c)) {
      row++
      offset = 0
    } else {
      // A character outside the basic plane is four bytes, counted at its
      // first half.
      const u = c.charCodeAt(0)
      offset += u < 0x80 ? 1 : u < 0x800 ? 2 : isHigh(c) ? 4 : isLow(c) ? 0 : 3
    }
  }
  return [row, offset]
}

// Whether one UTF-16 code unit is the first or the second half of a
// character outside the basic plane.
function isHigh(c: string): boolean {
  const u = c.charCodeAt(0)
  return 1 === c.length && 0xd800 <= u && u < 0xdc00
}

function isLow(c: string): boolean {
  const u = c.charCodeAt(0)
  return 1 === c.length && 0xdc00 <= u && u < 0xe000
}

// Whether `n` bytes of `a` from `ai` are those of `b` from `bi`.
function sameBytes(a: Uint8Array, ai: number, b: Uint8Array, bi: number, n: number): boolean {
  if (ai + n > a.length || bi + n > b.length) return false
  for (let j = 0; j < n; j++) if (a[ai + j] !== b[bi + j]) return false
  return true
}

// The sources of a parser's fixed tokens, while it lexes them.
function fixedTokens(config: any): string[] {
  return config.fixed.lex
    ? Object.values(config.fixed.token as Record<string, number>)
        .map((tin) => config.fixed.ref[tin])
        .filter((src: unknown): src is string => 'string' === typeof src && 0 < src.length)
    : []
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

// An engine error in the record or chunk that starts on row `start`, at
// row `line` of the input. An abort names `start` and no column: it lands
// between two of the engine's steps, where the engine often holds no token
// to place it at and falls back to 1:1 of the text it was parsing, so its
// own position says nothing.
function lineFailure(error: unknown, line: number, start: number, abort: AbortFlag): Fail {
  const fail = engineFailure(error, abort)
  if ('ABORTED' === fail.code) {
    fail.row = start
  } else {
    fail.row = line
    fail.col = enginePosition(error)?.col ?? 0
  }
  return fail
}

// A failure while the record or chunk that starts on row `start` was read:
// an abort that names no row names that one.
function atRecord(error: unknown, start: number): unknown {
  if (error instanceof Fail && 'ABORTED' === error.code && undefined === error.row) {
    error.row = start
  }
  return error
}

// JSON Lines records from the input's pieces. A record ends at a line
// character outside a string, as the grammar's lexer ends one: a line
// character inside a string is the string's, which the grammar refuses
// there, so the record goes on into the next piece and fails whole as the
// grammar fails it. A record of nothing but the grammar's spaces is blank
// and skipped.
class JsonRecords {
  readonly line: string[]
  readonly rows: string[]
  readonly single: boolean
  readonly fixed: string[]
  private readonly space: string[]
  private readonly quotes: string[]
  private readonly escape: string
  private readonly maxBytes: number

  // The record being read: its text and bytes so far, the row it starts on
  // (0 before its first piece), and whether a string is open in it.
  private text = ''
  private bytes = 0
  private start = 0
  private quote: string | null = null
  private escaped = false

  constructor(parser: any, maxBytes: number) {
    const config = parser.internal().config
    this.line = lexed(config.line.lex, config.line.chars)
    this.rows = Object.keys(config.line.rowChars ?? {})
    this.single = !!config.line.single
    this.fixed = fixedTokens(config)
    this.space = lexed(config.space.lex, config.space.chars)
    this.quotes = lexed(config.string.lex, config.string.quoteMap)
    this.escape = config.string.escChar ?? '\\'
    this.maxBytes = maxBytes
  }

  // A splitter for this grammar's line endings, bounded by the record.
  splitter(): PieceSplitter {
    const splitter = new PieceSplitter(this.line, this.rows, this.single, this.fixed)
    splitter.budget = () => this.maxBytes - this.bytes
    splitter.over = (row) => {
      const first = this.start || row
      return Fail.limit(
        'max_record_bytes',
        this.maxBytes,
        `line ${first} is longer than ${this.maxBytes} bytes`,
      ).at(first, 1)
    }
    return splitter
  }

  // Reads one piece, and hands `record` each record it ends that is not
  // blank, with the row it starts on.
  piece(row: number, text: string, bytes: number, record: (row: number, text: string) => Flow): Flow {
    if (0 === this.start) this.start = row
    let end = -1
    for (let i = 0; i < text.length; i++) {
      const c = text[i]
      if (null !== this.quote) {
        if (this.escaped) this.escaped = false
        else if (c === this.escape) this.escaped = true
        else if (c === this.quote) this.quote = null
      } else if (this.quotes.includes(c)) {
        this.quote = c
      } else if (this.line.includes(c)) {
        end = i
        break
      }
    }
    if (end < 0) {
      this.text += text
      this.bytes += bytes
      return 'continue'
    }
    return this.take(this.text + text.slice(0, end), record)
  }

  // The record the input ends inside, if any.
  end(record: (row: number, text: string) => Flow): Flow {
    if (0 === this.start) return 'continue'
    return this.take(this.text, record)
  }

  private take(text: string, record: (row: number, text: string) => Flow): Flow {
    const start = this.start
    this.text = ''
    this.bytes = 0
    this.start = 0
    this.quote = null
    this.escaped = false
    for (const c of text) {
      if (!this.space.includes(c)) return record(start, text)
    }
    return 'continue'
  }
}

// JSON Lines through the walk.
class JsonlWalk implements Driver {
  splitter: PieceSplitter
  private records: JsonRecords
  private onRecord: (row: number, text: string) => Flow
  private guarded: Guarded<Sink>
  private parser: any
  private abort: AbortFlag

  constructor(sink: Sink, options: DriverOptions) {
    this.guarded = new Guarded(sink, options.limits, options.abort, options.metrics)
    this.abort = options.abort
    this.parser = makeJson()
    const abort = options.abort
    prepare(this.parser, () => !abort.isAborted())
    this.records = new JsonRecords(this.parser, options.limits.max_record_bytes)
    this.splitter = this.records.splitter()
    this.onRecord = (row, text) => this.record(row, text)
  }

  begin(): Flow {
    return this.guarded.event(Ev.arrayStart)
  }

  piece(row: number, text: string, bytes: number): Flow {
    return this.records.piece(row, text, bytes, this.onRecord)
  }

  private record(number: number, text: string): Flow {
    let value: unknown
    try {
      value = this.parser.parse(text)
    } catch (err) {
      throw lineFailure(err, number, number, this.abort)
    }
    try {
      return walkValue(value, this.guarded)
    } catch (err) {
      throw atRecord(err, number)
    }
  }

  finish(): Flow {
    if ('stop' === this.records.end(this.onRecord)) return 'stop'
    if ('stop' === this.guarded.event(Ev.arrayEnd)) return 'stop'
    return this.guarded.event(Ev.end)
  }

  close(): void {
    this.guarded.flush()
  }
}

// JSON Lines through the rule-event adapter: one parser, one subscriber,
// the adapter reset before each record.
class JsonlIncremental implements Driver {
  splitter: PieceSplitter
  private records: JsonRecords
  private onRecord: (row: number, text: string) => Flow
  private adapter: Adapter<Sink>
  private parser: any
  private abort: AbortFlag
  // The record being read, which an abort the adapter raised names.
  private reading = 0

  constructor(sink: Sink, options: DriverOptions) {
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
    this.records = new JsonRecords(this.parser, options.limits.max_record_bytes)
    this.splitter = this.records.splitter()
    this.onRecord = (row, text) => this.record(row, text)
  }

  // The adapter's own outcome, when it stopped or failed inside a parse.
  private status(): Flow | null {
    const status = this.adapter.status
    if ('failed' === status.type) {
      throw 0 < this.reading ? atRecord(status.error, this.reading) : status.error
    }
    if ('stopped' === status.type) return 'stop'
    return null
  }

  begin(): Flow {
    return this.adapter.send(Ev.arrayStart)
  }

  piece(row: number, text: string, bytes: number): Flow {
    return this.records.piece(row, text, bytes, this.onRecord)
  }

  private record(number: number, text: string): Flow {
    this.reading = number
    let parsed: { ok: true; value: unknown } | { ok: false; error: unknown }
    try {
      parsed = { ok: true, value: this.parser.parse(text) }
    } catch (error) {
      parsed = { ok: false, error }
    }
    const status = this.status()
    if (null !== status) return status
    const adapter = this.adapter
    if (!parsed.ok) throw lineFailure(parsed.error, number, number, this.abort)
    if (adapter.complete()) {
      adapter.reset()
      return 'continue'
    }
    if (adapter.idle()) {
      let flow: Flow
      try {
        flow = adapter.walkWhole(parsed.value)
      } catch (err) {
        throw atRecord(err, number)
      }
      if ('stop' === flow) return 'stop'
      adapter.reset()
      return 'continue'
    }
    throw notStreamable()
  }

  finish(): Flow {
    if ('stop' === this.records.end(this.onRecord)) return 'stop'
    const status = this.status()
    if (null !== status) return status
    if ('stop' === this.adapter.send(Ev.arrayEnd)) return 'stop'
    return this.adapter.send(Ev.end)
  }

  close(): void {
    this.adapter.sink.flush()
  }
}

// What decides where the CSV grammar ends a record, read from its parser's
// resolved options and from the options the plugin builds its quote
// matcher from, so a separator, quote, record separator or comment setting
// moves the chunker's cut as it moves the grammar. Each set is empty while
// the engine does not lex its kind.
class Lexis {
  // Line characters: outside a token, each ends a record.
  line: string[] = []
  // Whether a run of line characters stops at a repeated one
  // (`record.empty`), so that `\n\n` is two line tokens.
  single = false
  rows: string[] = []
  space: string[] = []
  // The fixed tokens: the field separator, and outside strict mode the
  // JSON structure characters.
  fixed: string[] = []
  // The RFC 4180 quote, while the grammar's own matcher reads it: a
  // doubled one inside is one quote, and a line character is text.
  quote: string | null = null
  // The engine's own string quotes, after that one: an `escape` takes the
  // next character whatever it is, and a line character is text only
  // inside the `multi` ones (a backtick).
  strings: string[] = []
  multi: string[] = []
  escape = '\\'
  // The comment markers, longest first, each with its end: `null` for a
  // line comment, which a line character ends without being part of.
  comments: [string, string | null][] = []
  // What ends a run of text, as characters and as token starts.
  stops: string[] = []
  stopPrefixes: string[] = []
  // Whether the grammar ignores a space (outside strict mode) and a
  // comment, so that a record of nothing else is blank.
  spaceIgnored = false
  commentIgnored = false

  static csv(parser: any): Lexis {
    const config = parser.internal().config
    const options = parser.options.plugin?.csv ?? {}
    const lexis = new Lexis()
    lexis.line = lexed(config.line.lex, config.line.chars)
    lexis.single = !!config.line.single
    lexis.rows = Object.keys(config.line.rowChars ?? {})
    lexis.space = lexed(config.space.lex, config.space.chars)
    lexis.fixed = fixedTokens(config)
    // The longest marker first, and a tie by name: the engine's order.
    lexis.comments = config.comment.lex
      ? Object.entries(config.comment.def as Record<string, any>)
          .filter(([, def]) => def.lex && def.start)
          .sort(([an, a], [bn, b]) => b.start.length - a.start.length || (an < bn ? -1 : an > bn ? 1 : 0))
          .map(([, def]): [string, string | null] => [def.start, def.line ? null : (def.end ?? '')])
      : []
    // The RFC 4180 matcher as the plugin installs it: on in strict mode
    // unless `string.csv` is false, off otherwise unless it is true, and
    // inert for a quote that is not one UTF-16 code unit.
    const matcher = options.strict ? false !== options.string?.csv : true === options.string?.csv
    const quote = options.string?.quote
    lexis.quote = matcher && 'string' === typeof quote && 1 === quote.length ? quote : null
    lexis.strings = lexed(config.string.lex, config.string.quoteMap)
    lexis.multi = Object.keys(config.string.multiChars ?? {})
    lexis.escape = config.string.escChar ?? '\\'
    // What the engine's text matcher stops at: its ender pattern, which is
    // the space and line characters, the fixed tokens, the comment markers
    // and the grammar's own enders.
    lexis.stops = [...lexis.space, ...lexis.line]
    const enders = parser.options.ender
    lexis.stopPrefixes = [
      ...lexis.fixed,
      ...lexis.comments.map(([start]) => start),
      ...('string' === typeof enders ? [...enders] : Array.isArray(enders) ? enders : []).filter(
        (e: unknown) => 'string' === typeof e && 0 < e.length,
      ),
    ]
    const ignored = config.tokenSetTins?.IGNORE ?? {}
    lexis.spaceIgnored = !!ignored[parser.token('#SP')]
    lexis.commentIgnored = !!ignored[parser.token('#CM')]
    return lexis
  }

  // The length of the longest fixed token at `i`, or 0.
  fixedAt(text: string, i: number): number {
    let len = 0
    for (const source of this.fixed) {
      if (source.length > len && text.startsWith(source, i)) len = source.length
    }
    return len
  }

  // The comment that starts at `i`, by its index, or -1.
  commentAt(text: string, i: number): number {
    return this.comments.findIndex(([start]) => text.startsWith(start, i))
  }

  // The length of the line token at `i`.
  lineRun(text: string, i: number): number {
    let j = i
    while (j < text.length) {
      const c = text[j]
      if (!this.line.includes(c) || (this.single && text.slice(i, j).includes(c))) break
      j++
    }
    return j - i
  }

  // Whether a run of text stops at `i`.
  endsText(text: string, i: number): boolean {
    return this.stops.includes(text[i]) || this.stopPrefixes.some((p) => text.startsWith(p, i))
  }
}

// Where the record scanner is, between two characters.
type At =
  | { type: 'start' } // where the lexer starts a token
  | { type: 'text' } // inside text, a number or a keyword
  | { type: 'quoted' } // inside an RFC 4180 quoted field
  | { type: 'str'; quote: string; multi: boolean } // inside one of the engine's own strings
  | { type: 'comment'; index: number } // inside a comment

const START: At = { type: 'start' }
const TEXT: At = { type: 'text' }
const QUOTED: At = { type: 'quoted' }

// Follows CSV text through the grammar's tokens, a piece at a time, to
// find where its records end.
class Scanner {
  readonly lexis: Lexis
  private at: At = START
  // Whether the record so far holds anything the grammar does not ignore,
  // so that it is not blank.
  private content = false

  constructor(lexis: Lexis) {
    this.lexis = lexis
  }

  // Scan one piece of the input, which ends just past a line ending unless
  // it is the last, calling `end(offset, content)` where each record in it
  // ends: `offset` is just past the line token that ends it, and `content`
  // says whether it held anything the grammar does not ignore. Returns
  // whether the piece ends a record, so that a chunk may close after it.
  piece(text: string, end: (offset: number, content: boolean) => void): boolean {
    const lexis = this.lexis
    let ended = false
    let i = 0
    while (i < text.length) {
      const c = text[i]
      ended = false
      const at = this.at
      switch (at.type) {
        // In the lexer's order: the RFC 4180 matcher, fixed tokens, space,
        // lines, strings, comments, and then a run of text.
        case 'start': {
          let len = 0
          if (lexis.quote === c) {
            this.at = QUOTED
            this.content = true
            i++
          } else if (0 < (len = lexis.fixedAt(text, i))) {
            this.content = true
            i += len
          } else if (lexis.space.includes(c)) {
            this.content ||= !lexis.spaceIgnored
            i++
          } else if (lexis.line.includes(c)) {
            i += lexis.lineRun(text, i)
            end(i, this.content)
            this.content = false
            ended = true
          } else if (lexis.strings.includes(c)) {
            this.at = { type: 'str', quote: c, multi: lexis.multi.includes(c) }
            this.content = true
            i++
          } else {
            const comment = lexis.commentAt(text, i)
            if (0 <= comment) {
              this.at = { type: 'comment', index: comment }
              this.content ||= !lexis.commentIgnored
              i += lexis.comments[comment][0].length
            } else {
              // A character no token starts with (an ender) is one the
              // grammar refuses, and lexing resumes after it.
              if (!lexis.endsText(text, i)) this.at = TEXT
              this.content = true
              i++
            }
          }
          break
        }
        case 'text':
          if (lexis.endsText(text, i)) this.at = START
          else i++
          break
        case 'quoted':
          i++
          if (lexis.quote === c) {
            if (text[i] === c) i++
            else this.at = START
          }
          break
        case 'str':
          if (c === at.quote) {
            this.at = START
            i++
          } else if (c === lexis.escape) {
            i += i + 1 < text.length ? 2 : 1
          } else if (!at.multi && lexis.line.includes(c)) {
            // The grammar refuses the string here, so the line character
            // is read as a line.
            this.at = START
          } else {
            i++
          }
          break
        case 'comment': {
          const close = lexis.comments[at.index][1]
          if (null !== close && '' !== close && text.startsWith(close, i)) {
            this.at = START
            i += close.length
          } else if (null === close && lexis.line.includes(c)) {
            this.at = START
          } else {
            i++
          }
          break
        }
      }
    }
    return ended
  }
}

// The UTF-8 length of a string.
function utf8Bytes(text: string): number {
  return Buffer.byteLength(text, 'utf8')
}

// CSV through the walk: pieces into whole records, records into chunks,
// each chunk parsed with one reused parser.
class CsvWalk implements Driver {
  splitter: PieceSplitter
  private guarded: Guarded<Sink>
  private parser: any
  private abort: AbortFlag
  private scanner: Scanner
  private maxRecordBytes: number
  private chunkBytes: number
  // Whether the first record names the fields, and whether a blank line is
  // a record (`record.empty`): together, which record the header is.
  private wantHeader: boolean
  private recordEmpty: boolean

  // The header record as the grammar reads it, once read, and its bytes:
  // every chunk after the first starts with it.
  private header: string | null = null
  private headerBytes = 0
  private started = false

  // The chunk being filled, while one is: its text and bytes; where it may
  // first close (past the header in front of it, or in the first chunk past
  // the header record itself), as an index and in bytes; the file row its
  // own text starts on, and the rows of the header in front of it; where
  // the text it cannot close inside starts (one record, or before the
  // header everything up to its end), in bytes and as a row; and where the
  // last record ended, which is where the next one starts.
  private open = false
  private text = ''
  private bytes = 0
  private body = 0
  private bodyBytes = 0
  private firstLine = 0
  private prefixLines = 0
  private recordAt = 0
  private recordRow = 0
  private lastEnd = 0

  constructor(
    sink: Sink,
    options: DriverOptions,
    header: boolean,
    grammar: Record<string, unknown>,
  ) {
    this.guarded = new Guarded(sink, options.limits, options.abort, options.metrics)
    this.abort = options.abort
    this.maxRecordBytes = options.limits.max_record_bytes
    this.chunkBytes = options.chunkBytes
    this.wantHeader = header
    this.parser = makeCsv({ ...grammar, header } as any)
    this.recordEmpty = !!this.parser.options.plugin?.csv?.record?.empty
    const abort = options.abort
    prepare(this.parser, () => !abort.isAborted())
    const lexis = Lexis.csv(this.parser)
    this.scanner = new Scanner(lexis)
    const splitter = new PieceSplitter(lexis.line, lexis.rows, lexis.single, lexis.fixed)
    splitter.budget = () => this.maxRecordBytes - (this.open ? this.bytes - this.recordAt : 0)
    splitter.over = (row) => {
      const first = (this.open && this.recordRow) || row
      return Fail.limit(
        'max_record_bytes',
        this.maxRecordBytes,
        `the record starting at line ${first} is longer than ${this.maxRecordBytes} bytes`,
      ).at(first, 1)
    }
    this.splitter = splitter
  }

  begin(): Flow {
    return this.guarded.event(Ev.arrayStart)
  }

  // Opens a chunk: after the first, the header goes in front of it.
  private openChunk(): void {
    this.open = true
    this.text = this.started ? (this.header ?? '') : ''
    this.bytes = this.started ? this.headerBytes : 0
    this.started = true
    this.prefixLines = advance(this.scanner.lexis.rows, 0, 0, this.text)[0]
    this.body = this.text.length
    this.bodyBytes = this.bytes
    this.firstLine = 0
    this.recordAt = this.bytes
    this.recordRow = 0
    this.lastEnd = 0
  }

  piece(row: number, text: string, bytes: number): Flow {
    if (!this.open) this.openChunk()
    if (0 === this.firstLine) this.firstLine = row
    if (0 === this.recordRow) this.recordRow = row
    const at = this.text.length
    const atBytes = this.bytes
    this.text += text
    this.bytes += bytes
    const seeking = this.wantHeader && null === this.header
    const found = { from: -1, to: -1 }
    const ended = this.scanner.piece(text, (end, content) => {
      if (seeking && found.to < 0 && (content || this.recordEmpty)) {
        found.from = this.lastEnd
        found.to = at + end
      }
      this.lastEnd = at + end
    })
    if (0 <= found.to) {
      const { from, to } = found
      this.header = this.text.slice(from, to)
      this.headerBytes = utf8Bytes(this.header)
      this.body = to
      this.bodyBytes = atBytes + utf8Bytes(text.slice(0, to - at))
    }
    // A chunk may close where a piece ends a record, once the header is
    // behind it.
    if (ended && !(this.wantHeader && null === this.header)) {
      this.recordAt = this.bytes
      this.recordRow = 0
      if (this.bytes > this.bodyBytes && this.bytes >= this.chunkBytes) return this.flush()
    }
    return 'continue'
  }

  // Parse the chunk and walk its records.
  private flush(): Flow {
    if (!this.open) return 'continue'
    const text = this.text
    const firstLine = this.firstLine
    const prefixLines = this.prefixLines
    this.open = false
    this.text = ''
    let value: unknown
    try {
      value = this.parser.parse(text)
    } catch (err) {
      const pos = enginePosition(err)
      const rowInChunk = Math.max(pos?.row ?? 0, 1)
      // An error in the prepended header itself is the file's first line.
      const line = rowInChunk > prefixLines ? firstLine + rowInChunk - prefixLines - 1 : 1
      throw lineFailure(err, line, firstLine, this.abort)
    }
    // The grammar returns an array of records; anything else has none.
    const records = Array.isArray(value) ? value : []
    try {
      for (const record of records) {
        if ('stop' === walkValue(record, this.guarded)) return 'stop'
      }
    } catch (err) {
      throw atRecord(err, firstLine)
    }
    return 'continue'
  }

  finish(): Flow {
    if ('stop' === this.flush()) return 'stop'
    if ('stop' === this.guarded.event(Ev.arrayEnd)) return 'stop'
    return this.guarded.event(Ev.end)
  }

  close(): void {
    this.guarded.flush()
  }
}
