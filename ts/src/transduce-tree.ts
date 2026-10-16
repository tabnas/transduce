/* Copyright (c) 2026 tabnas, MIT License */

// `TransduceTree`: alchemy's `transduce-tree` run natively, the parser's
// rule passes (`ParserEvents/1`) in and a tree's `JsonEvents/1` out, as a
// `TreeSpec` keyed by rule name says (rs/src/transduce_tree.rs).
//
// The library's text (`stdlib/transduce.alc`) is the specification, and
// alchemy's `TreeSpec` documents what a native must do: each pass is looked
// up by its rule's name, one lookup per pass whatever the spec's size; an
// open pass announces its key, then begins its container or yields its
// scalar from one of its tokens; a close pass ends its container, or
// yields its own value; and a container whose rule is replaced by a rule
// the spec names a container is one container, so that close ends nothing
// and the replacing open begins nothing. The state is the count of open
// containers and whether a close handed its container on, so the native
// retains the document's nesting and nothing of its width or length, as
// the library's step does. alchemy-cli's differential test holds the two
// to the same bytes.
//
// A pass's events are worked out before the first of them is written, as
// the library's step returns them together: a pass that fails writes none.
// The failures are the library's, with its codes and texts.

import { Datum, Ev, Fail, isTreeContainer } from '@tabnas/alchemy/shared'
import type { Flow, JsonEvent, ParserEvent, ParserSink, RulePass, Sink, TreeSpec } from '@tabnas/alchemy/shared'

// The library's `transduce-tree`, natively, writing to `down`.
export class TransduceTree implements ParserSink {
  // Containers open.
  open = 0
  // A close handed its container on to the rule replacing it.
  carry = false

  constructor(
    readonly spec: TreeSpec,
    readonly down: Sink,
  ) {}

  event(ev: ParserEvent): Flow {
    switch (ev.type) {
      case 'open':
        return this.openPass(ev.pass)
      case 'close':
        return this.closePass(ev.pass)
      case 'end':
        if (0 < this.open) {
          throw new Fail('PROTOCOL_ORDER_ERROR', "the rule passes ended inside a container, which a tree's never do")
        }
        return this.down.event(Ev.end)
    }
  }

  private openPass(pass: RulePass): Flow {
    const rule = this.spec.rules.get(pass.name)
    if (undefined === rule) return 'continue'
    let key: string | undefined
    if (undefined !== rule.key) {
      if ('name' in rule.key) key = rule.key.name
      else {
        const token = pass.tokens[rule.key.token]
        if (undefined !== token) {
          if ('string' !== token.val.type) {
            throw typeError(`key: the name must be a string, not ${kind(token.val)}`)
          }
          key = token.val.value
        }
      }
    }
    let then: JsonEvent | undefined
    if (undefined !== rule.container) {
      if (this.carry) this.carry = false
      else {
        this.open++
        then = 'object' === rule.container ? Ev.objectStart : Ev.arrayStart
      }
    } else if (undefined !== rule.scalar && 'value' !== rule.scalar) {
      const token = pass.tokens[rule.scalar.token]
      if (undefined !== token) then = scalar(token.val)
    }
    if (undefined !== key && 'stop' === this.down.event(Ev.key(key))) return 'stop'
    return undefined === then ? 'continue' : this.down.event(then)
  }

  private closePass(pass: RulePass): Flow {
    const rule = this.spec.rules.get(pass.name)
    if (undefined === rule) return 'continue'
    if (undefined === rule.container) {
      if ('value' === rule.scalar && undefined !== pass.value) return this.down.event(scalar(pass.value))
      return 'continue'
    }
    if (undefined !== pass.alt && isTreeContainer(this.spec, pass.alt.r)) {
      this.carry = true
      return 'continue'
    }
    if (0 === this.open) throw typeError('pop: the vector is empty; there is no last item to remove')
    this.open--
    this.carry = false
    return this.down.event('object' === rule.container ? Ev.objectEnd : Ev.arrayEnd)
  }
}

// A value as the library's `scalar` takes it, or its refusal.
function scalar(value: Datum): JsonEvent {
  switch (value.type) {
    case 'null':
      return Ev.null
    case 'bool':
      return Ev.bool(value.value)
    case 'number':
      return Ev.number(value.value, value.lexeme)
    case 'string':
      return Ev.string(value.value)
    default:
      throw typeError(`scalar: the value must be null, a boolean, a number or a string, not ${kind(value)}`)
  }
}

// What a value is, in the runtime's words.
function kind(value: Datum): string {
  switch (value.type) {
    case 'null':
      return 'null'
    case 'bool':
      return 'a boolean'
    case 'number':
      return 'a number'
    case 'string':
      return 'a string'
    case 'array':
      return 'a vector'
    case 'object':
      return 'a record'
  }
}

// The runtime's type error: `DSL_TYPE_ERROR`, the `type_mismatch` word
// first.
function typeError(message: string): Fail {
  return new Fail('DSL_TYPE_ERROR', `type_mismatch: ${message}`)
}
