/* Copyright (c) 2026 tabnas, MIT License */

// Shared-prefix matching of many selectors in one pass.
//
// The router has to recognize every capture's selector over one event
// stream, and a document can be hundreds of megabytes, so the matcher
// never spells out a path per event: the selectors are compiled into one
// trie of steps, and the matcher walks the document with a stack of open
// containers (a level per container: its kind, how many values it has
// started, the current member's key) and, per level, the trie nodes that
// named that container, kept on one flat stack.
//
// The matcher also validates the protocol as it goes, because it holds the
// state that makes a malformed stream visible (a key outside an object, an
// array ending an object, a value after the root). Every stage downstream
// of it can therefore trust the sequence. A concrete `Path` is built only
// on request, for a delivered match or a failure.

import { Fail } from './error'
import { JsonEvent } from './event'
import { Path, Segment, Selector } from './selector'

// Which selector matched: its position in the list given to the matcher.
export type CaptureId = number

const ROOT = 0

// One trie node: the selectors' next steps below one position.
type Node = {
  properties: Map<string, number>
  indexes: Map<number, number>
  eachIndex: number
  eachMember: number
  // The selectors that end here, in id order.
  terminals: CaptureId[]
}

function newNode(): Node {
  return {
    properties: new Map(),
    indexes: new Map(),
    eachIndex: -1,
    eachMember: -1,
    terminals: [],
  }
}

// One open container.
type Level = {
  array: boolean
  // Values started in this container so far; the current or last one is
  // at index `started - 1`.
  started: number
  // The current member's key, for objects.
  key: string
  // Between a key and its value.
  inValue: boolean
  // The trie nodes that named this container: `nodeStack[start..end]`.
  start: number
  end: number
}

// What one event did to the document's structure: a member name (`key`),
// a container began (`start`), a whole scalar (`scalar`), a container
// completed (`close`), the document completed (`end`).
export type HitKind = 'key' | 'start' | 'scalar' | 'close' | 'end'

// The matcher's answer for one event.
export type Hit = {
  kind: HitKind
  // How many captures begin with this event: their ids are
  // `Matcher.begins()`, valid until the next event.
  begins: number
  // For `start` and `scalar`, the number of containers enclosing the
  // value; for `close`, the number enclosing the container that closed.
  depth: number
}

// Recognizes every selector of a set over one event stream.
export class Matcher {
  private nodes: Node[] = [newNode()]
  private levels: Level[] = []
  private open = 0
  private nodeStack: number[] = []
  private begun: CaptureId[] = []
  private rootDone = false
  private isEnded = false

  // Compile the selectors; their positions are the capture ids.
  constructor(selectors: readonly Selector[]) {
    selectors.forEach((selector, id) => {
      let at = ROOT
      for (const step of selector.steps) {
        const node = this.nodes[at]
        let next: number | undefined
        switch (step.type) {
          case 'property':
            next = node.properties.get(step.name)
            break
          case 'index':
            next = node.indexes.get(step.index)
            break
          case 'each_index':
            next = node.eachIndex < 0 ? undefined : node.eachIndex
            break
          case 'each_member':
            next = node.eachMember < 0 ? undefined : node.eachMember
            break
        }
        if (undefined === next) {
          next = this.nodes.length
          this.nodes.push(newNode())
          switch (step.type) {
            case 'property':
              node.properties.set(step.name, next)
              break
            case 'index':
              node.indexes.set(step.index, next)
              break
            case 'each_index':
              node.eachIndex = next
              break
            case 'each_member':
              node.eachMember = next
              break
          }
        }
        at = next
      }
      this.nodes[at].terminals.push(id)
    })
  }

  // Open containers right now.
  depth(): number {
    return this.open
  }

  // Whether `end` has been seen.
  ended(): boolean {
    return this.isEnded
  }

  // The concrete path of the value at `depth` enclosing containers. Asked
  // at a `start` or `scalar` hit it names the value; asked at a `close`
  // hit it names the container that closed.
  path(depth: number): Path {
    const segments: Segment[] = []
    const n = Math.min(depth, this.open)
    for (let i = 0; i < n; i++) {
      const level = this.levels[i]
      segments.push(level.array ? Math.max(0, level.started - 1) : level.key)
    }
    return new Path(segments)
  }

  // The captures that began with the last event, in id order.
  begins(): readonly CaptureId[] {
    return this.begun
  }

  // One event. A malformed sequence is a `PROTOCOL_ORDER_ERROR`.
  event(ev: JsonEvent): Hit {
    if (this.isEnded) {
      throw Fail.protocol('an event arrived after the document ended')
    }
    switch (ev.type) {
      case 'key': {
        const level = 0 < this.open ? this.levels[this.open - 1] : undefined
        if (undefined === level || level.array) {
          throw Fail.protocol('a key arrived outside an object')
        }
        if (level.inValue) {
          throw Fail.protocol('a key arrived where a value was expected')
        }
        level.key = ev.key
        level.inValue = true
        this.begun.length = 0
        return { kind: 'key', begins: 0, depth: this.open }
      }
      case 'object_start':
      case 'array_start': {
        const depth = this.open
        const start = this.beginValue()
        const end = this.nodeStack.length
        const array = 'array_start' === ev.type
        const level: Level = {
          array,
          started: 0,
          key: '',
          inValue: false,
          start,
          end,
        }
        if (depth === this.levels.length) this.levels.push(level)
        else this.levels[depth] = level
        this.open++
        return { kind: 'start', begins: this.begun.length, depth }
      }
      case 'object_end':
      case 'array_end': {
        const wantArray = 'array_end' === ev.type
        const level = 0 < this.open ? this.levels[this.open - 1] : undefined
        if (undefined === level) {
          throw Fail.protocol('a container ended that had not started')
        }
        if (level.array !== wantArray) {
          throw Fail.protocol(
            wantArray ? 'an array ended inside an object' : 'an object ended inside an array',
          )
        }
        if (level.inValue) {
          throw Fail.protocol('an object ended after a key without its value')
        }
        this.open--
        this.nodeStack.length = level.start
        this.completeValue()
        this.begun.length = 0
        return { kind: 'close', begins: 0, depth: this.open }
      }
      case 'end':
        if (0 < this.open) {
          throw Fail.protocol('the document ended inside a container')
        }
        if (!this.rootDone) {
          throw Fail.protocol('the document ended before its root value')
        }
        this.isEnded = true
        this.begun.length = 0
        return { kind: 'end', begins: 0, depth: 0 }
      default: {
        const depth = this.open
        const start = this.beginValue()
        this.nodeStack.length = start
        this.completeValue()
        return { kind: 'scalar', begins: this.begun.length, depth }
      }
    }
  }

  // A value begins at the current position: check that one is allowed
  // here, push the trie nodes naming it onto the node stack, and fill
  // `begun`. Returns where on the node stack the value's nodes start.
  private beginValue(): number {
    let base: number
    if (0 === this.open) {
      if (this.rootDone) throw Fail.protocol('a second root value arrived')
      this.nodeStack.length = 0
      this.nodeStack.push(ROOT)
      base = 0
    } else {
      const level = this.levels[this.open - 1]
      if (!level.array && !level.inValue) {
        throw Fail.protocol('a value arrived inside an object without a key')
      }
      const index = level.started
      level.started++
      this.nodeStack.length = level.end
      for (let at = level.start; at < level.end; at++) {
        const node = this.nodes[this.nodeStack[at]]
        if (level.array) {
          if (0 <= node.eachIndex) this.nodeStack.push(node.eachIndex)
          const c = node.indexes.get(index)
          if (undefined !== c) this.nodeStack.push(c)
        } else {
          if (0 <= node.eachMember) this.nodeStack.push(node.eachMember)
          const c = node.properties.get(level.key)
          if (undefined !== c) this.nodeStack.push(c)
        }
      }
      base = level.end
    }
    this.begun.length = 0
    for (let i = base; i < this.nodeStack.length; i++) {
      const terminals = this.nodes[this.nodeStack[i]].terminals
      for (const t of terminals) this.begun.push(t)
    }
    if (this.begun.length > 1) this.begun.sort((a, b) => a - b)
    return base
  }

  // The value at the current position is complete.
  private completeValue(): void {
    if (0 === this.open) this.rootDone = true
    else this.levels[this.open - 1].inValue = false
  }
}
