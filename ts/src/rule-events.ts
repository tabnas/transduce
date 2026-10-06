/* Copyright (c) 2026 tabnas, MIT License */

// The rule-event adapter: `JsonEvents/1` from a live tabnas parse.
//
// The engine tells a `ruleDone` subscriber about every rule pass, with the
// rule's node: the value the rule accumulates. Rules pushed or replaced
// from a rule whose node is a container share that container (the same
// object), which is the identity the adapter follows; it watches those
// containers and nothing grammar-specific, which is what lets one adapter
// serve every grammar. The algorithm is the Rust runtime's, step for step
// (rs/src/source/rule_events.rs has the full commentary):
//
// - A rule whose node is a container at the end of its OPEN pass, a
//   container not already on the frame stack, STARTS a container.
// - At the end of a CLOSE pass whose node is the top frame's container, a
//   longer container means new entries: the tail of an array, the last
//   members of a map (insertion ordered). A new entry that is the
//   container which completed last was already streamed; any other is
//   walked whole, late.
// - A key is announced early when the rule stashed it in `u.key` at its
//   OPEN pass; a key the adapter only learns at insertion is announced
//   then.
// - A repeated member name does not grow the map. When the announcing
//   rule's CLOSE pass leaves the map at its old length the adapter HOLDS
//   what the map has under the name until its next event, which a pass
//   that failed the parse never sends: a held scalar is streamed, the
//   container just streamed is complete already, any other container is a
//   merge of both values and fails the run with `DUPLICATE_MEMBER`.
// - A map whose names changed after they were streamed (a YAML merge key)
//   fails when its frame ends, with `STREAMABILITY_UNKNOWN`.
// - The top frame ENDS when a rule at the frame's depth whose node is the
//   frame's container closes, or the rule that started it closes, unless
//   that close REPLACES the rule (`alt.r`).
// - A root scalar is emitted at the depth-0 rule's close, unless that close
//   replaces the rule. A frame that starts at the root after a root value
//   completed is refused, and so is a parse root that closes for good over
//   a value that is not the one streamed: `STREAMABILITY_UNKNOWN`, before
//   `end`. `end` is not the adapter's to emit: the source sends it after
//   the engine has returned, so a document is complete only when it
//   validated.
//
// Number lexemes are best effort: at the close of a rule whose node is a
// number, the first open token's source text is kept when it is a JSON
// number that reads as the same value, and attached to that number when it
// is inserted next.
//
// Pruning drops the elements of an array frame from the engine's array
// after they were emitted, so a document's rows do not pile up in the
// engine's tree while the transducer streams them. It changes the value
// the engine returns, which is why only the incremental source, which
// discards that value, ever asks for it.

import {
  AbortFlag,
  Ev,
  Fail,
  Flow,
  JsonEvent,
  Limits,
  Metrics,
  Selector,
  Sink,
  engineKeys,
  engineScalar,
  isEngineContainer,
  isEngineMap,
  isJsonNumber,
} from '@tabnas/alchemy/shared'
import type { Context, Rule, RuleDone, RuleDoneSub, Tabnas, TabnasOptions } from '@tabnas/parser'

import { Guarded } from './guard'
import { Matcher } from './matcher'
import { Prune, walkValue } from './source'

// One open container.
type Frame = {
  cell: object
  array: boolean
  len: number
  ruleI: number
  depth: number
  // The last member name streamed; the one announced early for the next
  // member, when `hasKey`.
  key: string
  hasKey: boolean
  prune: boolean
  // The distinct names streamed into this map, to compare with the map's
  // names when the frame ends.
  names: Set<string>
}

// A member announced early whose rule closed without growing the map, held
// until the adapter's next event shows that the close stood. `found` is
// false when the grammar announced a member it never stored.
type Held = { key: string; found: boolean; value: unknown }

// How the run stands, as seen from inside the callbacks.
export type Status =
  | { type: 'running' }
  // The sink answered `stop`.
  | { type: 'stopped' }
  // The sink or a limit failed; the parse was told to cancel.
  | { type: 'failed'; error: unknown }

type PruneState = { type: 'never' } | { type: 'all' } | { type: 'under'; matcher: Matcher }

// The adapter's state, behind the engine's subscriber.
export class Adapter<S extends Sink> {
  private frames: Frame[] = []
  // The container that completed last: the next entry inserted into its
  // parent is this one, already streamed.
  private lastCompleted: object | null = null
  private held: Held | null = null
  private lexeme = ''
  private lexemeValue = 0
  private lexemeReady = false
  readonly sink: Guarded<S>
  status: Status = { type: 'running' }
  private stop: AbortFlag
  private prune: PruneState
  private pruneHit = false
  // A whole root value (a scalar, or the outermost frame) has been emitted.
  private rootDone = false
  // Anything at all has been emitted for the current document.
  private emitted = false

  constructor(
    sink: S,
    limits: Limits,
    abort: AbortFlag,
    metrics: Metrics,
    prune: Prune,
    stop: AbortFlag,
  ) {
    this.sink = new Guarded(sink, limits, abort, metrics)
    this.stop = stop
    switch (prune.type) {
      case 'never':
        this.prune = { type: 'never' }
        break
      case 'all_arrays':
        this.prune = { type: 'all' }
        break
      case 'under': {
        // A selector that names the elements names the array one step up;
        // one that names the array is taken as is.
        const steps = prune.selector.steps
        const last = steps[steps.length - 1]
        const array =
          undefined !== last && 'each_index' === last.type
            ? new Selector(steps.slice(0, -1))
            : prune.selector
        this.prune = { type: 'under', matcher: new Matcher([array]) }
        break
      }
    }
  }

  // Ready for another document into the same sink: the JSON Lines source
  // parses one value per line with one parser and one subscriber.
  reset(): void {
    this.frames.length = 0
    this.lastCompleted = null
    this.held = null
    this.lexemeReady = false
    this.pruneHit = false
    this.rootDone = false
    this.emitted = false
  }

  // Whether one whole root value was emitted and every frame closed.
  complete(): boolean {
    return this.rootDone && 0 === this.frames.length
  }

  // Whether nothing at all was emitted for the current document, so the
  // value the engine returned can be walked in its place.
  idle(): boolean {
    return !this.emitted
  }

  // Emit a whole value through the sink, outside the parse.
  walkWhole(value: unknown): Flow {
    this.emitted = true
    return walkValue(value, this.sink)
  }

  // Send one event straight to the sink, outside the parse.
  send(ev: JsonEvent): Flow {
    return this.sink.event(ev)
  }

  private fail(error: unknown): void {
    this.status = { type: 'failed', error }
    this.stop.abort()
  }

  // Emit one event. `false` means stop: the sink stopped or failed, and the
  // parse has been told to cancel.
  private emit(ev: JsonEvent): boolean {
    this.emitted = true
    if ('under' === this.prune.type) {
      try {
        const hit = this.prune.matcher.event(ev)
        this.pruneHit = 'start' === hit.kind && 0 < hit.begins
      } catch (err) {
        this.fail(err)
        return false
      }
    }
    let flow: Flow
    try {
      flow = this.sink.event(ev)
    } catch (err) {
      this.fail(err)
      return false
    }
    if ('stop' === flow) {
      this.status = { type: 'stopped' }
      this.stop.abort()
      return false
    }
    return true
  }

  // Emit a scalar value, with the remembered lexeme when it is this
  // number's.
  private scalar(value: unknown): boolean {
    const s = engineScalar(value)
    if (undefined === s) return true
    switch (s.type) {
      case 'null':
        return this.emit(Ev.null)
      case 'bool':
        return this.emit(Ev.bool(s.value))
      case 'string':
        return this.emit(Ev.string(s.value))
      case 'number':
        if (this.lexemeReady && this.lexemeValue === s.value) {
          this.lexemeReady = false
          return this.emit(Ev.number(s.value, this.lexeme))
        }
        // A remembered lexeme that is not this number's may be a later
        // entry's of the same batch; the batch's end drops it.
        return this.emit(Ev.number(s.value))
    }
  }

  // Emit a whole value the adapter did not see being built (late).
  private walk(value: unknown): boolean {
    this.lexemeReady = false
    if (Array.isArray(value)) {
      if (!this.emit(Ev.arrayStart)) return false
      for (const item of value) {
        if (!this.walk(item)) return false
      }
      return this.emit(Ev.arrayEnd)
    }
    if (isEngineMap(value)) {
      if (!this.emit(Ev.objectStart)) return false
      for (const k of engineKeys(value)) {
        if (!this.emit(Ev.key(k)) || !this.walk(value[k])) return false
      }
      return this.emit(Ev.objectEnd)
    }
    return this.scalar(value)
  }

  // Open a frame. Its `len` starts at zero whatever the container holds:
  // entries present when the adapter first sees a container were never
  // streamed (jsonic's promoted first value), and the next close pass
  // emits them before the new ones.
  private pushFrame(cell: object, array: boolean, rule: Rule, prune: boolean): void {
    this.frames.push({
      cell,
      array,
      len: 0,
      ruleI: rule.i,
      depth: rule.d,
      key: '',
      hasKey: false,
      prune,
      names: new Set(),
    })
  }

  // The subscriber's body.
  onDone(rule: Rule, done: RuleDone): void {
    if ('running' !== this.status.type) return
    try {
      this.step(rule, done)
    } catch (err) {
      // Not a failure the adapter reports itself (those set the status and
      // return): a defect, kept as it is rather than read as the input's.
      this.fail(err)
    }
  }

  private step(rule: Rule, done: RuleDone): void {
    // Any further event means the pass that held a member stood.
    if (!this.flushHeld()) return
    if (null != done.alt?.err) return
    if ('o' === done.state) {
      this.opened(rule, rule.node)
    } else {
      const replaces = null != done.alt && '' !== (done.alt.r ?? '')
      this.closed(rule, rule.node, replaces)
    }
  }

  private opened(rule: Rule, cell: unknown): void {
    if (!isEngineContainer(cell)) return
    const node = cell as object
    const array = Array.isArray(node)
    if (!this.frames.some((f) => f.cell === node)) {
      if (0 === this.frames.length && this.rootDone) {
        this.fail(wrappedRoot())
        return
      }
      if (null !== this.lastCompleted) {
        // The container that completed last was never stored in the one
        // around it, and the grammar is opening another: what left cannot
        // be taken back.
        this.fail(unstoredContainer())
        return
      }
      const top = this.frames[this.frames.length - 1]
      if (undefined !== top && !top.array && !top.hasKey) {
        // A container opening in a map whose next member has no key yet:
        // its events would leave before the key, which no tree's events do.
        this.fail(valueBeforeKey())
        return
      }
      this.lexemeReady = false
      if (!this.emit(array ? Ev.arrayStart : Ev.objectStart)) return
      let prune = false
      switch (this.prune.type) {
        case 'all':
          prune = array
          break
        case 'under':
          prune = array && this.pruneHit
          break
      }
      this.pushFrame(node, array, rule, prune)
    } else if (!array) {
      const top = this.frames[this.frames.length - 1]
      if (top.cell === node && !top.hasKey && top.len === containerLen(node)) {
        const key = rule.u?.key
        if ('string' === typeof key) {
          if (!this.emit(Ev.key(key))) return
          top.key = key
          top.hasKey = true
        }
      }
    }
  }

  private closed(rule: Rule, cell: unknown, replaces: boolean): void {
    // A root scalar's lexeme has to be known before it is emitted below.
    this.rememberLexeme(rule)
    // Whether a whole root value had left before this pass: the pass that
    // completes the root is exempt from the check at the end.
    const wasDone = this.rootDone

    let pruneFrom = -1
    const topI = this.frames.length - 1
    // An engine container (`isEngineContainer`): an array or a map.
    if (0 <= topI && this.frames[topI].cell === cell && (Array.isArray(cell) || isEngineMap(cell))) {
      const node: unknown[] | Record<string, unknown> = cell
      const array = Array.isArray(node)
      const keys = array ? null : engineKeys(node)
      const len = array ? node.length : (keys as string[]).length
      const frame = this.frames[topI]
      const old = frame.len
      if (!array && frame.hasKey && len === old) {
        // The announced member did not grow the map, and the rule that
        // announced it is closing: it replaced or merged an earlier member
        // of the same name, or the pass failed the parse before storing it.
        // Hold what the map has under the name; the next event decides.
        if (rule.u?.key === frame.key) this.holdPending(node, topI)
      }
      if (len > old) {
        for (let i = old; i < len; i++) {
          let value: unknown
          if (array) {
            value = node[i]
          } else {
            const key = (keys as string[])[i]
            value = node[key]
            const top = this.frames[topI]
            if (top.hasKey && top.key !== key) {
              // A member landed ahead of the one announced; that one keeps
              // its place in the stream.
              if (!this.settlePending(node, topI)) return
            }
            const announced = top.hasKey
            top.hasKey = false
            top.names.add(key)
            if (!announced) {
              top.key = key
              if (!this.emit(Ev.key(key))) return
            }
          }
          if (!this.entry(value)) return
        }
        this.frames[topI].len = len
        this.lexemeReady = false
        if (array && this.frames[topI].prune) pruneFrom = old
      }
    }
    if (0 <= pruneFrom) {
      ;(cell as unknown[]).length = pruneFrom
      this.frames[this.frames.length - 1].len = pruneFrom
    }

    if (replaces) return

    const top = this.frames[this.frames.length - 1]
    if (undefined !== top && (top.ruleI === rule.i || (top.cell === cell && top.depth === rule.d))) {
      const array = top.array
      // A member held by this very pass goes before the frame ends.
      if (!this.flushHeld()) return
      if (top.hasKey) {
        // The announcing rule never closed on this container: the member is
        // whatever the map holds under the name now.
        if (!this.settlePending(cell, this.frames.length - 1)) return
      }
      if (null !== this.lastCompleted) {
        // A container built inside this one was streamed and never stored
        // (jsonic drops a pair's value inside a list when `list.pair` is
        // off): the frame cannot end as the walk's would.
        this.fail(unstoredContainer())
        return
      }
      if (!array) {
        const names = isEngineMap(cell) ? engineKeys(cell) : []
        const same = names.length === top.names.size && names.every((k) => top.names.has(k))
        if (!same) {
          this.fail(rewrittenMap())
          return
        }
      }
      this.frames.pop()
      this.lexemeReady = false
      if (!this.emit(array ? Ev.arrayEnd : Ev.objectEnd)) return
      this.lastCompleted = isEngineContainer(cell) ? (cell as object) : null
      if (0 === this.frames.length) this.rootDone = true
    }

    if (0 === rule.d && 0 === this.frames.length && !this.rootDone) {
      if (!isEngineContainer(cell)) {
        if (!this.scalar(cell)) return
        this.rootDone = true
      }
    }

    // The parse root closing for good after the root value left: the node
    // must still hold what was streamed.
    if (wasDone && 0 === rule.d && 0 === this.frames.length) {
      const container = isEngineContainer(cell)
      const streamed =
        null !== this.lastCompleted ? container && cell === this.lastCompleted : !container
      if (!streamed) this.fail(rewrittenRoot())
    }
  }

  // Emit one entry that just landed in the top frame's container: a scalar
  // as itself, the container that completed last as nothing (it has been
  // streamed), any other container whole, late.
  private entry(value: unknown): boolean {
    if (null !== this.lastCompleted) {
      if (isEngineContainer(value) && value === this.lastCompleted) {
        this.lastCompleted = null
        return true
      }
      // Something else landed: the container streamed last was never
      // stored, and its events cannot be taken back.
      this.fail(unstoredContainer())
      return false
    }
    if (!isEngineContainer(value)) return this.scalar(value)
    return this.walk(value)
  }

  // The member announced on the frame `topI` did not grow its map: stream
  // what the map holds under that name now.
  private settlePending(node: unknown, topI: number): boolean {
    const top = this.frames[topI]
    top.hasKey = false
    const m = member(node, top.key)
    return this.settle(top.key, m.found, m.value)
  }

  // Like `settlePending`, but the member is held rather than streamed: the
  // announcing rule's close pass may be one the engine reports although an
  // action failed the parse in it.
  private holdPending(node: unknown, topI: number): void {
    const top = this.frames[topI]
    top.hasKey = false
    const m = member(node, top.key)
    this.held = { key: top.key, found: m.found, value: m.value }
  }

  // Stream the member held at the last pass, if any. `false` means stop.
  private flushHeld(): boolean {
    const held = this.held
    if (null === held) return true
    this.held = null
    return this.settle(held.key, held.found, held.value)
  }

  // Stream what a map holds under a repeated member's name: a scalar as
  // itself; the container that completed last as nothing; any other
  // container fails the run as a merge; no value at all fails it as a
  // member the grammar announced and never stored.
  private settle(key: string, found: boolean, value: unknown): boolean {
    if (!found) {
      this.fail(
        new Fail(
          'STREAMABILITY_UNKNOWN',
          `the grammar announced member ${JSON.stringify(key)} and never stored it, which the ` +
            `incremental source cannot follow; run it with SourceMode.materialize`,
        ),
      )
      return false
    }
    if (!isEngineContainer(value)) return this.scalar(value)
    if (null !== this.lastCompleted && value === this.lastCompleted) {
      this.lastCompleted = null
      return true
    }
    this.fail(mergedMember(key))
    return false
  }

  // Keep the source text of a number rule's first token when it is a JSON
  // number spelling the node's value.
  private rememberLexeme(rule: Rule): void {
    const node = rule.node
    if ('number' !== typeof node) return
    const token = rule.o?.[0]
    if (null == token) return
    const src = token.src
    if ('string' === typeof src && isJsonNumber(src) && Number(src) === node) {
      this.lexeme = src
      this.lexemeValue = node
      this.lexemeReady = true
    } else {
      this.lexemeReady = false
    }
  }
}

function containerLen(node: object): number {
  return Array.isArray(node) ? node.length : engineKeys(node).length
}

// The member of a map under `key`.
function member(node: unknown, key: string): { found: boolean; value: unknown } {
  if (isEngineMap(node) && Object.prototype.hasOwnProperty.call(node, key)) {
    return { found: true, value: node[key] }
  }
  return { found: false, value: undefined }
}

// The subscriber and the parse budget on `parser`: the subscriber hands
// every rule pass to `onDone`, and the budget cancels the parse as soon as
// `proceed` answers false, while still consulting any budget check the
// grammar installed at its own cadence. Member order is recorded on the
// engine's maps (`map.ordered`), which leaves the values' shape alone.
export function prepare(
  parser: Tabnas,
  proceed: () => boolean,
  onDone?: (rule: Rule, done: RuleDone) => void,
): void {
  // The engine's `options` answers a `Record<string, any>`; what it holds
  // under `parse` is what `TabnasOptions` describes.
  const parse: TabnasOptions['parse'] = parser.options?.parse
  const budget = parse?.budget ?? {}
  const prev = 'function' === typeof budget.onCheck ? budget.onCheck : null
  const every = budget.checkEveryN ?? 0
  const prevN = null !== prev && 0 < every ? every : 0
  parser.options({
    map: { ordered: true },
    parse: {
      budget: {
        checkEveryN: 1,
        onCheck: (ctx: Context) => {
          if (!proceed()) return false
          if (null !== prev && 0 < prevN && 0 === ctx.kI % prevN) return prev(ctx)
          return true
        },
      },
    },
  } satisfies TabnasOptions)
  if (onDone) {
    const ruleDone: RuleDoneSub = (rule, _ctx, done) => onDone(rule, done)
    parser.sub({ ruleDone })
  }
}

// The failure for an incremental run whose events did not amount to one
// whole document.
export function notStreamable(): Fail {
  return new Fail(
    'STREAMABILITY_UNKNOWN',
    'the grammar did not build its value through rule events the incremental source can ' +
      'follow (the events did not amount to one whole document); run it with ' +
      'SourceMode.materialize',
  )
}

// The failure for a container that starts at the root after a root value
// has completed.
export function wrappedRoot(): Fail {
  return new Fail(
    'STREAMABILITY_UNKNOWN',
    "the grammar wrapped a value already streamed as the document's root in a list (a YAML " +
      'stream of several documents, a jsonic top-level implicit list); the incremental source ' +
      'cannot take the root back, so run it with SourceMode.materialize',
  )
}

// The failure for a parse root whose node no longer holds the value
// streamed as the document when the root rule closes for good.
export function rewrittenRoot(): Fail {
  return new Fail(
    'STREAMABILITY_UNKNOWN',
    "the grammar replaced the document's root after the incremental source streamed it (a " +
      'YAML stream of several documents is wrapped in a list when the source ends); the ' +
      'incremental source cannot take the root back, so run it with SourceMode.materialize',
  )
}

// The failure for a container the adapter streamed as the grammar built it
// and the grammar then never stored in the container around it.
export function unstoredContainer(): Fail {
  return new Fail(
    'STREAMABILITY_UNKNOWN',
    'the grammar built a container the incremental source streamed and then never stored it ' +
      'in the container around it (jsonic drops a pair inside a list when list.pair is off), ' +
      "so the stream would not be the document's; the incremental source cannot follow a " +
      'grammar that builds a container so: run it with SourceMode.materialize',
  )
}

// The failure for a container the grammar opens inside a map before the
// member it belongs to has a key.
export function valueBeforeKey(): Fail {
  return new Fail(
    'STREAMABILITY_UNKNOWN',
    "the grammar opened a container inside a map before announcing the member's key (it " +
      'builds the value, or a key that is itself a container, in a rule of its own and ' +
      'names the member only when the pair closes); its events would leave before the key, ' +
      "so the stream would not be the document's; the incremental source cannot follow a " +
      'grammar that builds a member so: run it with SourceMode.materialize',
  )
}

// The failure for a map whose members the grammar rewrote after the adapter
// streamed them.
export function rewrittenMap(): Fail {
  return new Fail(
    'STREAMABILITY_UNKNOWN',
    'the grammar rewrote the members of a map after the incremental source streamed them ' +
      "(a YAML merge key does), so the stream would not be the document's; run it with " +
      'SourceMode.materialize',
  )
}

// The failure for a repeated member whose values the grammar merged.
export function mergedMember(key: string): Fail {
  return new Fail(
    'DUPLICATE_MEMBER',
    `member ${JSON.stringify(key)} appears twice and the grammar merged the two values; the ` +
      'first was already streamed, so the incremental source cannot emit the merged member: ' +
      'run it with SourceMode.materialize',
  )
}
