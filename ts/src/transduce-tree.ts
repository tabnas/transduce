/* Copyright (c) 2026 tabnas, MIT License */

// `TransduceTree`: alchemy's `transduce-tree` run natively, the parser's
// rule passes (`ParserEvents/1`) in and a tree's `JsonEvents/1` out, as a
// `TreeSpec` keyed by rule name says (rs/src/transduce_tree.rs).
//
// The library's text (`stdlib/transduce.alc`) is the specification, and
// alchemy's `TreeSpec` documents what a native must do: each pass is looked
// up by its rule's name, one lookup per pass whatever the spec's size; an
// open pass announces its key, then begins its container or yields its
// scalar from one of its tokens, then announces its member; a close pass
// yields its text, when its rule is a `:text` rule, then ends its
// container, or yields its own value; and a container whose rule is
// replaced by a rule the spec names a container is one container, so that
// close ends nothing and the replacing open begins nothing. The state is
// the count of open containers, whether a close handed its container on,
// and the text each open `:text` rule has gathered, to which every pass
// writes its tokens' source text, so the native retains the document's
// nesting and an open rule's text, and nothing else of its width or
// length, as the library's step does. alchemy-cli's differential test
// holds the two to the same bytes.
//
// A text is held to the spec's `maxTextBytes` as it grows, at the joins
// the library's `string-join` makes and in their order: a pass's tokens
// together, then the innermost text with them, and a closed text with the
// text around it, each refused past the bound with alchemy's
// `treeTextRefusal`. The routers say this native reads `:member` and
// `:text` (routers.ts), which is what lets the lowering hand it a spec
// that uses either.
//
// A pass's events are worked out before the first of them is written, as
// the library's step returns them together: a pass that fails writes none.
// The failures are the library's, with its codes and texts, and in its
// order: an open pass's key, then what it begins or yields, then its
// tokens' text, then its member; a close pass's text, then what it ends or
// yields.

import { Datum, Ev, Fail, isTreeContainer, treeTextRefusal, utf8Bytes } from '@tabnas/alchemy/shared'
import type { Flow, JsonEvent, ParserEvent, ParserSink, RulePass, Sink, TreeKey, TreeSpec } from '@tabnas/alchemy/shared'

// The library's `transduce-tree`, natively, writing to `down`.
export class TransduceTree implements ParserSink {
  // Containers open.
  open = 0
  // A close handed its container on to the rule replacing it.
  carry = false
  // The text each open `:text` rule has gathered, the innermost last, and
  // its length in UTF-8 bytes, which the bound is held to.
  readonly texts: string[] = []
  readonly bytes: number[] = []

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

  // The most bytes a text may hold.
  private get max(): number {
    return this.spec.maxTextBytes ?? Infinity
  }

  // The pass's tokens written to the innermost open text, if one is and the
  // pass consumed any, as the library's `transduce-spell` writes them: the
  // tokens' text joined, then written to the innermost, each join held to
  // the bound.
  private spell(pass: RulePass): void {
    if (0 === this.texts.length || 0 === pass.tokens.length) return
    let more = ''
    let n = 0
    for (const token of pass.tokens) {
      more += token.src
      n += utf8Bytes(token.src)
    }
    if (n > this.max) throw treeTextRefusal(n, this.max)
    this.write(more, n)
  }

  // `more`, of `n` bytes, written at the end of the innermost open text, as
  // the library's `transduce-write` joins the two, held to the bound.
  private write(more: string, n: number): void {
    const last = this.texts.length - 1
    const len = this.bytes[last] + n
    if (len > this.max) throw treeTextRefusal(len, this.max)
    this.texts[last] += more
    this.bytes[last] = len
  }

  private openPass(pass: RulePass): Flow {
    const rule = this.spec.rules.get(pass.name)
    if (undefined === rule) {
      this.spell(pass)
      return 'continue'
    }
    const key = named(rule.key, pass)
    let then: JsonEvent | undefined
    let begins = false
    if (undefined !== rule.container) {
      if (!this.carry) {
        begins = true
        then = 'object' === rule.container ? Ev.objectStart : Ev.arrayStart
      }
    } else if (undefined !== rule.scalar && 'value' !== rule.scalar) {
      const token = pass.tokens[rule.scalar.token]
      if (undefined !== token) then = scalar(token.val)
    }
    this.spell(pass)
    const member = named(rule.member, pass)
    if (undefined !== rule.container) {
      if (begins) this.open++
      else this.carry = false
    }
    if (true === rule.text) {
      this.texts.push('')
      this.bytes.push(0)
    }
    for (const ev of [
      undefined === key ? undefined : Ev.key(key),
      then,
      undefined === member ? undefined : Ev.key(member),
    ]) {
      if (undefined !== ev && 'stop' === this.down.event(ev)) return 'stop'
    }
    return 'continue'
  }

  private closePass(pass: RulePass): Flow {
    const rule = this.spec.rules.get(pass.name)
    if (undefined === rule) {
      this.spell(pass)
      return 'continue'
    }
    // A `:text` rule's text closes first, and joins the one around it
    // before this pass's own tokens do; it is yielded before anything
    // else, once the rest of the pass is known not to fail.
    let text: string | undefined
    if (true === rule.text) {
      text = this.texts.pop()
      if (undefined === text) throw typeError('top: the vector is empty; there is no last item')
      const n = this.bytes.pop() as number
      if (0 < this.texts.length) this.write(text, n)
    }
    this.spell(pass)
    let then: JsonEvent | undefined
    if (undefined === rule.container) {
      if ('value' === rule.scalar && undefined !== pass.value) then = scalar(pass.value)
    } else if (undefined !== pass.alt && isTreeContainer(this.spec, pass.alt.r)) {
      this.carry = true
    } else {
      if (0 === this.open) throw typeError('pop: the vector is empty; there is no last item to remove')
      this.open--
      this.carry = false
      then = 'object' === rule.container ? Ev.objectEnd : Ev.arrayEnd
    }
    if (undefined !== text && 'stop' === this.down.event(Ev.string(text))) return 'stop'
    return undefined === then ? 'continue' : this.down.event(then)
  }
}

// The member name a field of a rule's entry names for a pass: the string,
// or the `:val` of the pass's token at that index, nothing when the pass
// consumed no such token; the library's `key` refuses a name that is not a
// string.
function named(field: TreeKey | undefined, pass: RulePass): string | undefined {
  if (undefined === field) return undefined
  if ('name' in field) return field.name
  const token = pass.tokens[field.token]
  if (undefined === token) return undefined
  if ('string' !== token.val.type) {
    throw typeError(`key: the name must be a string, not ${kind(token.val)}`)
  }
  return token.val.value
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
