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
//
// The protocols, `Fail` and its codes, `Limits`, `Metrics`, `Selector`,
// `Datum` and the captures' types are alchemy's shared types
// (`@tabnas/alchemy/shared`), which this package imports and re-exports
// under the names it always exported them by. `routers` is this package's
// stages as alchemy's `Routers`, for a host to pass to alchemy's `compile`.

// This package's version, as package.json declares it.
export const VERSION = '0.2.4'

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
} from '@tabnas/alchemy/shared'
export type { Duplicates } from '@tabnas/alchemy/shared'
export { Code, Fail, isFail } from '@tabnas/alchemy/shared'
export type { Limit } from '@tabnas/alchemy/shared'
export { Ev, eventEquals, eventText, isEnd, isScalar, isStart } from '@tabnas/alchemy/shared'
export type { JsonEvent, JsonEventType } from '@tabnas/alchemy/shared'
export { isJsonNumber, jsonNumber, jsonString, numberText } from '@tabnas/alchemy/shared'
export {
  AbortFlag,
  LIMIT_NAMES,
  Limits,
  Metrics,
  NODE_BYTES,
  utf8Bytes,
} from '@tabnas/alchemy/shared'
export { Matcher } from './matcher'
export type { Hit, HitKind } from './matcher'
export type { CaptureId } from '@tabnas/alchemy/shared'
export { FnRoute, Router, SelectedRecorder } from './route'
export { CaptureSpec } from '@tabnas/alchemy/shared'
export type { Budget, CaptureMode, RouteSink, Selected } from '@tabnas/alchemy/shared'
export { ScanEmit } from './scan'
export { Transition } from '@tabnas/alchemy/shared'
export { Path, Selector, keyText, pathText } from '@tabnas/alchemy/shared'
export type { Segment, Step } from '@tabnas/alchemy/shared'
export { CountSink, EventRecorder, FnSink, TreeContract, replay } from '@tabnas/alchemy/shared'
export type { Flow, Sink } from '@tabnas/alchemy/shared'
export { Prune, SourceMode, ValueSource, engineFailure, walkValue } from './source'
export type { Source } from './source'
export { Guarded } from './guard'
export { ParserSource } from './parser-source'
export { DEFAULT_CHUNK_BYTES, LineFormat, LinesSource } from './lines'
export type { LinesChunk, LinesInput, LinesWriter } from './lines'
export { INCREMENTAL, capability, isIncremental } from './capability'
export { Cell, Schema, Table, boundColumn, columnFromMeta } from '@tabnas/alchemy/shared'
export type {
  BoundColumn,
  ColumnMapper,
  MissingPolicy,
  PublicColumn,
  TableBinding,
  TableEvent,
  TableSink,
} from '@tabnas/alchemy/shared'
export { TableFromJson } from './table-from-json'
export { routers } from './routers'
