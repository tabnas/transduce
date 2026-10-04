/* Copyright (c) 2026 tabnas, MIT License */

// This package's stages as alchemy's `Routers`: what an alchemy program's
// runtime builds its routes, tables, scans and guards from. Alchemy
// declares the interface (`@tabnas/alchemy/shared`) and imports no
// transducer; a host passes `routers` to its `compile`, with render's
// `renderers`: `compile(src, file, { routers, renderers })`.

import {
  AbortFlag,
  CaptureSpec,
  Duplicates,
  Flow,
  Limits,
  Metrics,
  RouteSink,
  Routers,
  Scan,
  Sink,
  TableBinding,
  TableSink,
  Transition,
} from '@tabnas/alchemy/shared'

import { Guarded } from './guard'
import { Router } from './route'
import { ScanEmit } from './scan'
import { TableFromJson } from './table-from-json'

// Each method constructs the stage it names with the same parameters.
export const routers: Routers = Object.freeze({
  router(
    specs: readonly CaptureSpec[],
    limits: Limits,
    duplicates: Duplicates,
    metrics: Metrics,
    downstream: RouteSink,
  ): Sink {
    return new Router(specs, limits, duplicates, metrics, downstream)
  },

  tableFromJson(
    binding: TableBinding,
    limits: Limits,
    duplicates: Duplicates,
    metrics: Metrics,
    sink: TableSink,
  ): Sink {
    return new TableFromJson(binding, limits, duplicates, metrics, sink)
  },

  scanEmit<S, I, O>(
    initial: S,
    step: (state: S, item: I) => Transition<S, O>,
    finish: (state: S) => O[],
    out: (output: O) => Flow,
  ): Scan<I> {
    return new ScanEmit<S, I, O>(initial, step, finish, out)
  },

  guarded(inner: Sink, limits: Limits, abort: AbortFlag, metrics: Metrics): Sink {
    return new Guarded(inner, limits, abort, metrics)
  },
})
