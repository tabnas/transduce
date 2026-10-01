/* Copyright (c) 2026 tabnas, MIT License */

// The source-side limits, applied to events as they are produced.
//
// Every source emits through a `Guarded` sink, so the three limits a source
// owns (`max_depth`, `max_key_bytes`, `max_scalar_bytes`) are checked once,
// in one place, whether the events come from a live parse or from a walk
// over a parsed value; the abort flag is polled per event so a walk stops
// as promptly as a parse does; and the source metrics (events, keys,
// scalars) are counted locally and flushed in one step.

import { Fail } from './error'
import { JsonEvent } from './event'
import { AbortFlag, Limits, Metrics, utf8Bytes } from './limits'
import { Flow, Sink } from './sink'

// A sink wrapper that enforces the source limits and counts.
export class Guarded<S extends Sink> implements Sink {
  readonly inner: S
  private maxDepth: number
  private maxKeyBytes: number
  private maxScalarBytes: number
  private abort: AbortFlag
  private metrics: Metrics
  private open = 0
  private events = 0
  private keys = 0
  private scalars = 0

  constructor(inner: S, limits: Limits, abort: AbortFlag, metrics: Metrics) {
    this.inner = inner
    this.maxDepth = limits.max_depth
    this.maxKeyBytes = limits.max_key_bytes
    this.maxScalarBytes = limits.max_scalar_bytes
    this.abort = abort
    this.metrics = metrics
  }

  // Add the counts so far to the shared metrics and start again.
  flush(): void {
    this.metrics.events += this.events
    this.metrics.keys += this.keys
    this.metrics.scalars += this.scalars
    this.events = 0
    this.keys = 0
    this.scalars = 0
  }

  // Open containers right now.
  depth(): number {
    return this.open
  }

  private scalar(bytes: number): void {
    this.scalars++
    if (bytes > this.maxScalarBytes) {
      throw Fail.limit(
        'max_scalar_bytes',
        this.maxScalarBytes,
        `a scalar of ${bytes} bytes is larger than ${this.maxScalarBytes}`,
      )
    }
  }

  event(ev: JsonEvent): Flow {
    if (this.abort.isAborted()) throw Fail.aborted()
    this.events++
    switch (ev.type) {
      case 'object_start':
      case 'array_start':
        this.open++
        if (this.open > this.maxDepth) {
          throw Fail.limit(
            'max_depth',
            this.maxDepth,
            `a container is nested deeper than ${this.maxDepth}`,
          )
        }
        break
      case 'object_end':
      case 'array_end':
        this.open = Math.max(0, this.open - 1)
        break
      case 'key': {
        this.keys++
        const n = utf8Bytes(ev.key)
        if (n > this.maxKeyBytes) {
          throw Fail.limit(
            'max_key_bytes',
            this.maxKeyBytes,
            `a key of ${n} bytes is longer than ${this.maxKeyBytes}`,
          )
        }
        break
      }
      case 'string':
        this.scalar(utf8Bytes(ev.value))
        break
      case 'number':
        this.scalar(null != ev.lexeme ? utf8Bytes(ev.lexeme) : 0)
        break
      case 'null':
      case 'bool':
        this.scalar(0)
        break
      case 'end':
        this.flush()
        break
    }
    return this.inner.event(ev)
  }
}
