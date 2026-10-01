/* Copyright (c) 2026 tabnas, MIT License */

// Captures: recognize selected scopes and deliver them, complete, in
// source order.
//
// A `Router` is a `Sink` that feeds every event through one `Matcher` for
// all of its `CaptureSpec`s and hands each completed match to a
// `RouteSink`. A `materialize` capture is built into a `Datum` under a
// byte budget and delivered when its value completes; an `observe` capture
// delivers only the path, at the value's end, and retains nothing.
// Materialized captures may not nest or coincide: the router holds at most
// one value at a time, which is what makes its retention one selected
// scope rather than a document. That is checked at construction,
// conservatively, with `Selector.mayOverlap`, and again at run time so a
// stream the check could not foresee still fails rather than mixing two
// values.

import { Datum, DatumBuilder, Duplicates } from './datum'
import { Fail } from './error'
import { JsonEvent } from './event'
import { Limits, Metrics } from './limits'
import { CaptureId, Matcher } from './matcher'
import { Path, Selector } from './selector'
import { Flow, Sink } from './sink'

// What a capture keeps of its match: build the value and deliver it whole
// (`materialize`), or deliver only that the value occurred, and where,
// when it ends (`observe`).
export type CaptureMode = 'materialize' | 'observe'

// The byte budget one materialized capture may not exceed, named after the
// `Limits` field the failure reports.
export type Budget = { bytes: number; name: string }

// One capture: a tag for the consumer, the selector to match, the mode,
// and for a `materialize` capture an optional budget (else the router's
// `max_capture_bytes`).
export class CaptureSpec {
  readonly tag: string
  readonly selector: Selector
  readonly mode: CaptureMode
  budget: Budget | null = null

  constructor(tag: string, selector: Selector, mode: CaptureMode) {
    this.tag = tag
    this.selector = selector
    this.mode = mode
  }

  static materialize(tag: string, selector: Selector): CaptureSpec {
    return new CaptureSpec(tag, selector, 'materialize')
  }

  static observe(tag: string, selector: Selector): CaptureSpec {
    return new CaptureSpec(tag, selector, 'observe')
  }

  // This spec with its own budget, whose failure names `name`.
  withBudget(bytes: number, name: string): CaptureSpec {
    this.budget = { bytes, name }
    return this
  }
}

// One completed match: the spec's position in the router's list, its tag,
// the concrete path, and the value for a `materialize` capture (`null`
// for `observe`).
export type Selected = {
  id: CaptureId
  tag: string
  path: Path
  value: Datum | null
}

// The consumer of a router's matches.
export interface RouteSink {
  // A capture's value is beginning. Nothing has been retained for it yet,
  // so a consumer that knows the value is out of order can refuse it here
  // at no cost (throw a `Fail`); the router adds the path to a failure
  // that has none.
  began?(id: CaptureId, tag: string): void

  selected(selected: Selected): Flow

  // The document ended, validated. Exactly once, after the last match.
  end(): Flow
}

// A route sink that records every match.
export class SelectedRecorder implements RouteSink {
  readonly selections: Selected[] = []
  ended = false

  selected(selected: Selected): Flow {
    this.selections.push(selected)
    return 'continue'
  }

  end(): Flow {
    this.ended = true
    return 'continue'
  }
}

// A route sink made of a function; `end` continues.
export class FnRoute implements RouteSink {
  private fn: (selected: Selected) => Flow | void

  constructor(fn: (selected: Selected) => Flow | void) {
    this.fn = fn
  }

  selected(selected: Selected): Flow {
    return this.fn(selected) || 'continue'
  }

  end(): Flow {
    return 'continue'
  }
}

// The one materialization in progress.
type Active = {
  id: CaptureId
  builder: DatumBuilder
  // The depth its value began at.
  depth: number
}

// Recognizes every capture in one pass and delivers completed matches.
export class Router<D extends RouteSink> implements Sink {
  readonly specs: readonly CaptureSpec[]
  readonly downstream: D
  private matcher: Matcher
  private active: Active | null = null
  // Observed values in progress: `[capture, depth]`, innermost last.
  private observing: [CaptureId, number][] = []
  private maxDepth: number
  private maxCaptureBytes: number
  private duplicates: Duplicates
  // For the capture accounting only (`captured_bytes` and its high-water
  // marks). The source counts events, keys and scalars.
  private metrics: Metrics
  private isEnded = false

  // Build a router; two specs that may overlap are refused unless both
  // observe (`CAPTURE_OVERLAP_UNSUPPORTED`).
  constructor(
    specs: readonly CaptureSpec[],
    limits: Limits,
    duplicates: Duplicates,
    metrics: Metrics,
    downstream: D,
  ) {
    for (let i = 0; i < specs.length; i++) {
      for (let j = i + 1; j < specs.length; j++) {
        const a = specs[i]
        const b = specs[j]
        const bothObserve = 'observe' === a.mode && 'observe' === b.mode
        if (!bothObserve && a.selector.mayOverlap(b.selector)) {
          throw new Fail(
            'CAPTURE_OVERLAP_UNSUPPORTED',
            `captures ${JSON.stringify(a.tag)} (${a.selector}) and ` +
              `${JSON.stringify(b.tag)} (${b.selector}) may select overlapping scopes; ` +
              `only observed captures may overlap`,
          )
        }
      }
    }
    this.specs = specs.slice()
    this.matcher = new Matcher(specs.map((s) => s.selector))
    this.downstream = downstream
    this.maxDepth = limits.max_depth
    this.maxCaptureBytes = limits.max_capture_bytes
    this.duplicates = duplicates
    this.metrics = metrics
  }

  // Whether `end` has been delivered.
  ended(): boolean {
    return this.isEnded
  }

  // Start the capture `id` at the value beginning at `depth`.
  private begin(id: CaptureId, depth: number): void {
    const spec = this.specs[id]
    if (this.downstream.began) {
      try {
        this.downstream.began(id, spec.tag)
      } catch (err) {
        if (err instanceof Fail && null == err.path) {
          err.path = this.matcher.path(depth).toString()
        }
        throw err
      }
    }
    if ('observe' === spec.mode) {
      this.observing.push([id, depth])
      return
    }
    if (null !== this.active) {
      const path = this.matcher.path(depth).toString()
      throw new Fail(
        'CAPTURE_OVERLAP_UNSUPPORTED',
        `capture ${JSON.stringify(spec.tag)} began at ${path} while capture ` +
          `${JSON.stringify(this.specs[this.active.id].tag)} was still being materialized`,
      ).atPath(path)
    }
    const budget = spec.budget ?? { bytes: this.maxCaptureBytes, name: 'max_capture_bytes' }
    this.active = {
      id,
      builder: new DatumBuilder(budget.bytes, budget.name, this.duplicates),
      depth,
    }
  }

  private deliver(id: CaptureId, path: Path, value: Datum | null): Flow {
    return this.downstream.selected({ id, tag: this.specs[id].tag, path, value })
  }

  event(ev: JsonEvent): Flow {
    const hit = this.matcher.event(ev)
    if ('start' === hit.kind && hit.depth + 1 > this.maxDepth) {
      const path = this.matcher.path(hit.depth).toString()
      throw Fail.limit(
        'max_depth',
        this.maxDepth,
        `a container at ${path} is nested deeper than ${this.maxDepth}`,
      ).atPath(path)
    }
    for (let k = 0; k < hit.begins; k++) {
      this.begin(this.matcher.begins()[k], hit.depth)
    }
    const active = this.active
    if (null !== active) {
      try {
        active.builder.event(ev)
      } catch (err) {
        // The builder reports no position; the value's path is spelled
        // here, only now that something went wrong.
        if (err instanceof Fail) err.atPath(this.matcher.path(active.depth).toString())
        throw err
      }
      if (active.builder.finished()) {
        const bytes = active.builder.bytes()
        const value = active.builder.take() as Datum
        this.active = null
        const path = this.matcher.path(hit.depth)
        this.metrics.capture(bytes)
        let flow: Flow
        try {
          flow = this.deliver(active.id, path, value)
        } finally {
          this.metrics.release(bytes)
        }
        if ('stop' === flow) return 'stop'
      }
    }
    if ('scalar' === hit.kind || 'close' === hit.kind) {
      while (0 < this.observing.length) {
        const [id, depth] = this.observing[this.observing.length - 1]
        if (depth !== hit.depth) break
        this.observing.pop()
        if ('stop' === this.deliver(id, this.matcher.path(hit.depth), null)) {
          return 'stop'
        }
      }
    }
    if ('end' === hit.kind) {
      this.isEnded = true
      return this.downstream.end()
    }
    return 'continue'
  }
}
