/* Copyright (c) 2026 tabnas, MIT License */

// `scan-emit`: declarative state evolution over a stream.
//
// A pure step function takes the state and one item and returns the next
// state with the items to emit; a finish function turns the final state
// into the closing items. The operator owns the state and the iteration;
// the functions own nothing. This is what the DSL's `scan-emit` lowers to,
// and the table transducer is one instance of it.

import { Fail } from './error'
import { Flow } from './sink'

// The result of one step: the next state and what to emit for it.
export type Transition<S, O> = { state: S; outputs: O[] }

export const Transition = Object.freeze({
  of<S, O>(state: S, outputs: O[]): Transition<S, O> {
    return { state, outputs }
  },
  // A step that emits nothing.
  stay<S, O>(state: S): Transition<S, O> {
    return { state, outputs: [] }
  },
  // A step that emits one item.
  emit<S, O>(state: S, output: O): Transition<S, O> {
    return { state, outputs: [output] }
  },
})

// The operator. Feed items with `item`, then call `finish` exactly once
// when the input completed successfully; never call it after a failure or
// a cancellation. A step or the finish throws a `Fail` to fail the run; a
// step that failed leaves the operator without a state, so a later item or
// `finish` is a `PROTOCOL_ORDER_ERROR`.
export class ScanEmit<S, I, O> {
  private state: { value: S } | null
  private step: (state: S, item: I) => Transition<S, O>
  private finisher: ((state: S) => O[]) | null
  private out: (output: O) => Flow

  constructor(
    initial: S,
    step: (state: S, item: I) => Transition<S, O>,
    finish: (state: S) => O[],
    out: (output: O) => Flow,
  ) {
    this.state = { value: initial }
    this.step = step
    this.finisher = finish
    this.out = out
  }

  // One input item. Its outputs go downstream before this returns.
  item(item: I): Flow {
    const state = this.state
    if (null === state) {
      throw Fail.protocol('scan-emit received an item after it finished')
    }
    this.state = null
    const transition = this.step(state.value, item)
    this.state = { value: transition.state }
    for (const o of transition.outputs) {
      if ('stop' === this.out(o)) return 'stop'
    }
    return 'continue'
  }

  // The input completed: emit the closing items.
  finish(): Flow {
    const state = this.state
    const finish = this.finisher
    if (null === state || null === finish) {
      throw Fail.protocol('scan-emit finished twice')
    }
    this.state = null
    this.finisher = null
    for (const o of finish(state.value)) {
      if ('stop' === this.out(o)) return 'stop'
    }
    return 'continue'
  }
}
