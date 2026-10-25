/* Copyright (c) 2026 tabnas, MIT License */

// This package's stages as alchemy's `Routers`: what an alchemy program's
// runtime builds its routes, scans and guards from. Alchemy declares the
// interface (`@tabnas/alchemy/shared`) and imports no transducer; a host
// passes `routers` to its `compile`, with render's `renderers`:
// `compile(src, file, { routers, renderers })`. The standard library's
// `table-from-json`, `csv`, `transduce-tree` and `transduce-records` are
// alchemy's own text, run by its interpreter over these stages in every
// runtime; this package offers nothing in their place.

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
  Transition,
} from '@tabnas/alchemy/shared'

import { Guarded } from './guard'
import { Router } from './route'
import { ScanEmit } from './scan'

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
