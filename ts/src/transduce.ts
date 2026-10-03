/* Copyright (c) 2026 tabnas, MIT License */

// Streaming transducers over any tabnas parser.
//
// A transducer decides what a source contributes to an output model; a
// renderer decides how that model becomes text. This package is the
// transducer side:
//
// - the source protocol `JsonEvent` (`JsonEvents/1`) and the push boundary
//   `Sink`;
// - sources that produce it from a parsed value, from a live tabnas parse
//   through the engine's rule events, or from line-delimited input;
// - `Selector`s and the matcher and router that recognize selected scopes
//   in one pass and materialize them under a byte limit;
// - the table protocol `TableEvent` (`TableRows/1`) and the standard
//   metadata-first table transducer;
// - `Limits`, `Metrics`, `AbortFlag` and the stable failure `Code`s every
//   stage reports.
//
// Everything is synchronous and push-based: the parser calls the first
// sink from inside its own callback, and a slow writer at the end of the
// chain slows the parse at the start. That is the backpressure.

// This package's version, as package.json declares it.
export const VERSION = '0.1.2'

export {
  Datum,
  DatumBuilder,
  byteSize,
  fromJSON,
  fromTabnas,
  getPath,
  takePath,
  toJSON,
  toText,
  walkDatum,
} from './datum'
export type { Duplicates } from './datum'
export { Code, Fail, isFail } from './error'
export type { Limit } from './error'
export { Ev, eventEquals, eventText, isEnd, isScalar, isStart } from './event'
export type { JsonEvent, JsonEventType } from './event'
export { isJsonNumber, jsonNumber, jsonString, numberText } from './json'
export { AbortFlag, LIMIT_NAMES, Limits, Metrics, NODE_BYTES, utf8Bytes } from './limits'
export { Matcher } from './matcher'
export type { CaptureId, Hit, HitKind } from './matcher'
export { CaptureSpec, FnRoute, Router, SelectedRecorder } from './route'
export type { Budget, CaptureMode, RouteSink, Selected } from './route'
export { ScanEmit, Transition } from './scan'
export { Path, Selector, keyText, pathText } from './selector'
export type { Segment, Step } from './selector'
export { CountSink, EventRecorder, FnSink, TreeContract, replay } from './sink'
export type { Flow, Sink } from './sink'
export { Prune, SourceMode, ValueSource, engineFailure, walkValue } from './source'
export type { Source } from './source'
export { Guarded } from './guard'
export { ParserSource } from './parser-source'
export { DEFAULT_CHUNK_BYTES, LineFormat, LinesSource } from './lines'
export type { LinesChunk, LinesInput, LinesWriter } from './lines'
export { INCREMENTAL, capability, isIncremental } from './capability'
export { Cell, Schema, Table, boundColumn, columnFromMeta } from './table'
export type {
  BoundColumn,
  ColumnMapper,
  MissingPolicy,
  PublicColumn,
  TableBinding,
  TableEvent,
  TableSink,
} from './table'
export { TableFromJson } from './table-from-json'
