/* Copyright (c) 2026 tabnas, MIT License */

// The differential suite behind `capability.incremental`, in this runtime.
//
// The port of rs/tests/incremental_test.rs. For every grammar in the
// devDependencies and every fixture that grammar reads, the events
// incremental mode produces must equal the events materialize mode
// produces (number lexemes aside: the walk over a parsed value has none, so
// they are stripped before the comparison and checked separately to spell
// the value they accompany). Two outcomes short of identity are accepted,
// because they are the contract the incremental source documents: a
// document that repeats a member name streams every occurrence where the
// walk keeps the survivor, so the two must agree once a `last_wins` router
// has built the value; and a run the source REFUSES, with
// `STREAMABILITY_UNKNOWN` or `DUPLICATE_MEMBER`, after a protocol-valid
// prefix and before `end`. A fixture the grammar itself refuses is checked
// as well: the incremental run must fail with the same code at the same
// position (or with a documented refusal), after a protocol-valid prefix
// and before `end`. What is never accepted is a completed stream that
// disagrees with the walk. The adapter's two nets over how a grammar builds
// its values (a container opened inside a map before the member's key, a
// container streamed and never stored) count against a grammar.
//
// The TypeScript grammars build their values through their own rule
// events, so the list this suite keeps honest is this runtime's
// (`src/capability.ts`), asserted in both directions: a listed grammar that
// mismatches or trips a net fails, and an unlisted grammar that never does
// fails too.
//
// The fixtures are the Rust suite's own, read from rs/tests/fixtures, and
// four generated documents in the spec's worked-example shape (2000
// records as JSON, JSON Lines, CSV and block YAML), each offered only to
// the grammars of its family.

import { describe, it } from 'node:test'
import assert from 'node:assert'
import { readFileSync, readdirSync } from 'node:fs'
import { join } from 'node:path'

import { Tabnas } from '@tabnas/parser'

import {
  CaptureSpec,
  Code,
  Datum,
  Duplicates,
  EventRecorder,
  Fail,
  Flow,
  INCREMENTAL,
  JsonEvent,
  Limits,
  Matcher,
  Metrics,
  ParserSource,
  Prune,
  Router,
  SelectedRecorder,
  Selector,
  SourceMode,
  eventEquals,
  eventText,
  isIncremental,
  replay,
  toText,
} from '../dist/transduce'
import { GRAMMARS } from './common'
import { recordsCsv, recordsJson, recordsJsonl, recordsYaml } from './support'

const RECORDS = 2000

// Which generated documents each grammar is offered.
const GENERATED: Record<string, string[]> = {
  json: ['records.json'],
  jsonl: ['records.jsonl'],
  jsonic: ['records.json'],
  jsonc: ['records.json'],
  json5: ['records.json'],
  yaml: ['records.yaml'],
  csv: ['records.csv'],
}

const FIXTURES_DIR = join(__dirname, '..', '..', 'rs', 'tests', 'fixtures')

function fixture(name: string): string {
  return readFileSync(join(FIXTURES_DIR, name), 'utf8')
}

// Every committed fixture, by file name, in a stable order.
function committedFixtures(): [string, string][] {
  return readdirSync(FIXTURES_DIR)
    .sort()
    .map((name) => [name, fixture(name)])
}

function generated(name: string): string {
  switch (name) {
    case 'records.json':
      return recordsJson(RECORDS)
    case 'records.jsonl':
      return recordsJsonl(RECORDS)
    case 'records.csv':
      return recordsCsv(RECORDS)
    case 'records.yaml':
      return recordsYaml(RECORDS)
  }
  throw new Error('no generator for ' + name)
}

type Run = { flow?: Flow; fail?: unknown; events: JsonEvent[] }

function run(grammar: string | (() => any), text: string, mode: SourceMode): Run {
  const make = 'string' === typeof grammar ? GRAMMARS[grammar] : grammar
  const recorder = new EventRecorder()
  try {
    const flow = new ParserSource(make(), text).unverified().mode(mode).run(recorder)
    return { flow, events: recorder.events }
  } catch (fail) {
    return { fail, events: recorder.events }
  }
}

function incremental(grammar: string | (() => any), text: string): Run {
  return run(grammar, text, SourceMode.incremental(Prune.never()))
}

function materialized(grammar: string | (() => any), text: string): Run {
  return run(grammar, text, SourceMode.materialize())
}

function withoutLexemes(events: JsonEvent[]): JsonEvent[] {
  return events.map((e) =>
    'number' === e.type && null !== e.lexeme ? { type: 'number', value: e.value, lexeme: null } : e,
  )
}

function sameEvents(a: JsonEvent[], b: JsonEvent[]): boolean {
  return a.length === b.length && a.every((e, i) => eventEquals(e, b[i]))
}

function hasEnd(events: JsonEvent[]): boolean {
  return events.some((e) => 'end' === e.type)
}

// Whether a recording is a protocol-valid stream, or a prefix of one: every
// event is accepted by a matcher, which validates the sequence.
function wellFormed(events: JsonEvent[]): boolean {
  const m = new Matcher([])
  try {
    for (const e of events) m.event(e)
    return true
  } catch (_err) {
    return false
  }
}

// The document's root value as a router materializes it from a recording
// under `policy`, which is how every consumer of the stream sees repeated
// members.
function rootValue(events: JsonEvent[], policy: Duplicates): Datum {
  const out = new SelectedRecorder()
  const r = new Router(
    [CaptureSpec.materialize('root', Selector.root())],
    Limits.default(),
    policy,
    new Metrics(),
    out,
  )
  replay(events, r)
  return out.selections[0]?.value ?? Datum.null
}

function rootText(events: JsonEvent[], policy: Duplicates): string | null {
  try {
    return toText(rootValue(events, policy))
  } catch (_err) {
    return null
  }
}

function code(fail: unknown): string {
  return fail instanceof Fail ? fail.code : 'NOT_A_FAIL'
}

function show(fail: unknown): string {
  return fail instanceof Fail ? fail.toString() : String(fail)
}

// Whether a documented refusal is one of the adapter's nets over how a
// grammar builds its values, by the sentence both carry.
function unfollowed(fail: unknown): boolean {
  return (
    fail instanceof Fail &&
    'STREAMABILITY_UNKNOWN' === fail.code &&
    fail.message.includes('the incremental source cannot follow a grammar that builds')
  )
}

type Outcome =
  | { type: 'not_read'; walk: string; incremental: string }
  | { type: 'match'; events: number; lexemes: number }
  | { type: 'match_last_wins'; events: number }
  | { type: 'refused'; code: string; events: number }
  | { type: 'unfollowed'; events: number }
  | { type: 'mismatch'; why: string }

function compare(grammar: string, text: string): Outcome {
  const whole = materialized(grammar, text)
  if (undefined !== whole.fail) {
    // The grammar refuses the document: the incremental run must fail too,
    // after a protocol-valid prefix and before `end`, with the grammar's
    // own failure (code and position alike), or a documented refusal.
    const inc = incremental(grammar, text)
    if (undefined === inc.fail) {
      return {
        type: 'mismatch',
        why:
          `the walk failed with ${show(whole.fail)}; the incremental run completed with ` +
          `${inc.flow} after ${inc.events.length} events`,
      }
    }
    const clean = wellFormed(inc.events) && !hasEnd(inc.events)
    if (clean && unfollowed(inc.fail)) return { type: 'unfollowed', events: inc.events.length }
    const w = whole.fail as Fail
    const f = inc.fail as Fail
    const same = code(f) === code(w) && f.row === w.row && f.col === w.col
    const documented = ['STREAMABILITY_UNKNOWN', 'DUPLICATE_MEMBER'].includes(code(f))
    if (clean && (same || documented)) {
      return { type: 'not_read', walk: code(w), incremental: code(f) }
    }
    return {
      type: 'mismatch',
      why:
        `the walk failed with ${show(w)}; the incremental run failed with ${show(f)} after ` +
        `${inc.events.length} events (protocol-valid prefix: ${wellFormed(inc.events)})`,
    }
  }
  const inc = incremental(grammar, text)
  if (undefined !== inc.fail) {
    const documented = ['STREAMABILITY_UNKNOWN', 'DUPLICATE_MEMBER'].includes(code(inc.fail))
    if (documented && wellFormed(inc.events) && !hasEnd(inc.events)) {
      if (unfollowed(inc.fail)) return { type: 'unfollowed', events: inc.events.length }
      return { type: 'refused', code: code(inc.fail), events: inc.events.length }
    }
    return {
      type: 'mismatch',
      why: `the incremental run failed with ${show(inc.fail)} after ${inc.events.length} events`,
    }
  }
  let lexemes = 0
  for (const ev of inc.events) {
    if ('number' === ev.type && null !== ev.lexeme) {
      lexemes++
      if (Number(ev.lexeme) !== ev.value) {
        return {
          type: 'mismatch',
          why: `lexeme ${JSON.stringify(ev.lexeme)} does not spell the value ${ev.value}`,
        }
      }
    }
  }
  const stripped = withoutLexemes(inc.events)
  if (sameEvents(stripped, whole.events)) {
    return { type: 'match', events: whole.events.length, lexemes }
  }
  const lastWins = rootText(stripped, 'last_wins')
  if (wellFormed(stripped) && null !== lastWins && lastWins === rootText(whole.events, 'reject')) {
    return { type: 'match_last_wins', events: stripped.length }
  }
  let first = stripped.findIndex((e, i) => i >= whole.events.length || !eventEquals(e, whole.events[i]))
  if (first < 0) first = Math.min(stripped.length, whole.events.length)
  const around = (events: JsonEvent[]) =>
    events
      .slice(Math.max(0, first - 3), Math.min(events.length, first + 4))
      .map(eventText)
      .join(' ')
  return {
    type: 'mismatch',
    why:
      `incremental produced ${stripped.length} events, the walk ${whole.events.length}; first ` +
      `difference at event ${first}: incremental [${around(stripped)}] vs walk ` +
      `[${around(whole.events)}]`,
  }
}

// Run one grammar over every fixture it reads and check the capability
// list agrees with what happened.
function verify(name: string): void {
  const fixtures = committedFixtures()
  for (const g of GENERATED[name] ?? []) {
    fixtures.push([`generated ${g} (${RECORDS} records)`, generated(g)])
  }
  const total = fixtures.length
  let read = 0
  const mismatches: string[] = []
  const unfollowedList: string[] = []
  fixtures.forEach(([fixtureName, text], i) => {
    const started = Date.now()
    const outcome = compare(name, text)
    let verdict: string
    switch (outcome.type) {
      case 'not_read':
        verdict = `not read (${outcome.walk}; the incremental run failed with ${outcome.incremental})`
        break
      case 'match':
        read++
        verdict = `MATCH (${outcome.events} events, ${outcome.lexemes} lexemes)`
        break
      case 'match_last_wins':
        read++
        verdict = `MATCH after last_wins (${outcome.events} events; the document repeats a member name)`
        break
      case 'refused':
        read++
        verdict = `REFUSED with ${outcome.code} after ${outcome.events} events`
        break
      case 'unfollowed':
        read++
        unfollowedList.push(`${fixtureName}: after ${outcome.events} events`)
        verdict = `UNFOLLOWED: refused for how the grammar builds, after ${outcome.events} events`
        break
      case 'mismatch':
        read++
        mismatches.push(`${fixtureName}: ${outcome.why}`)
        verdict = `MISMATCH: ${outcome.why}`
        break
    }
    console.log(
      `incremental ${name}: ${i + 1} of ${total} (${Math.floor(((i + 1) * 100) / total)}%) ` +
        `${fixtureName}: ${verdict} [${((Date.now() - started) / 1000).toFixed(2)}s]`,
    )
  })
  assert.ok(read > 0, `grammar ${name} read none of the fixtures`)
  const verified = 0 === mismatches.length && 0 === unfollowedList.length
  const listed = isIncremental(name)
  const problems = [
    ...mismatches,
    ...unfollowedList.map(
      (f) =>
        `${f}: refused for how the grammar builds its values (a container opened in a map ` +
        'before its key, or streamed and never stored)',
    ),
  ]
  assert.equal(
    listed,
    verified,
    `capability.incremental(${JSON.stringify(name)}) is ${listed}, but the grammar ` +
      (verified
        ? 'never mismatched; add it to INCREMENTAL in src/capability.ts'
        : 0 === mismatches.length
          ? 'builds its values in a way the adapter refuses; remove it from INCREMENTAL, or ' +
            'fix the grammar'
          : 'mismatched; remove it from INCREMENTAL, or fix the adapter') +
      ` over ${read} fixtures it reads` +
      (0 < problems.length ? ':\n  ' + problems.join('\n  ') : ''),
  )
}

describe('incremental: the differential suite', () => {
  for (const name of Object.keys(GRAMMARS).sort()) {
    it(`${name} streams incrementally where the list says so`, () => verify(name))
  }

  // The grammar packages package.json takes as devDependencies, read at test
  // time so a grammar added there without a GRAMMARS entry fails here. A
  // grammar is a package that depends on the engine; the engine itself and
  // @tabnas/support, the fixture runner, are not grammars.
  it('every grammar in the devDependencies is verified here', () => {
    const pkg = require('../package.json')
    const grammars = Object.keys(pkg.devDependencies)
      .filter((dep) => dep.startsWith('@tabnas/'))
      .map((dep) => dep.slice('@tabnas/'.length))
      .filter((name) => 'parser' !== name && 'support' !== name)
      .filter((name) => {
        const dep = JSON.parse(
          readFileSync(join(__dirname, '..', 'node_modules', '@tabnas', name, 'package.json'), 'utf8'),
        )
        const needs = { ...dep.peerDependencies, ...dep.dependencies }
        return '@tabnas/parser' in needs
      })
      .sort()
    assert.deepEqual(grammars, Object.keys(GRAMMARS).sort())
    for (const name of INCREMENTAL) {
      assert.ok(name in GRAMMARS, `INCREMENTAL names ${name}, which this suite does not run`)
    }
  })
})

describe('incremental: documented shapes', () => {
  // A repeated member name is the one documented place the incremental
  // stream and the walk differ: the stream carries every occurrence, the
  // walk only the engine's survivor. A router's `last_wins` makes them
  // agree; `reject` sees the duplicate the walk would have hidden.
  it('a repeated scalar member streams every occurrence and last wins agrees with the walk', () => {
    const cases: [string, string][] = [
      ['json', '{"a":1,"a":2,"b":3}'],
      ['jsonl', '{"a":1,"a":2}\n{"a":3,"a":4,"b":5}\n'],
      ['yaml', 'a: 1\na: 2\nb: 3\n'],
      ['json5', '{a:1,a:2,b:3}'],
      ['jsonc', '{"a":1,"a":2,"b":3}'],
      ['jsonic', 'a:1,a:2,b:3'],
    ]
    for (const [name, text] of cases) {
      if (!isIncremental(name)) continue
      const inc = incremental(name, text)
      assert.equal(inc.fail, undefined, `${name}: ${show(inc.fail)}`)
      assert.equal(inc.flow, 'continue', name)
      assert.ok(wellFormed(inc.events), name)
      const keys = inc.events.filter((e) => 'key' === e.type && 'a' === e.key).length
      assert.ok(keys >= 2, `${name}: both occurrences of a are in the stream`)
      const walked = materialized(name, text)
      assert.equal(
        rootText(withoutLexemes(inc.events), 'last_wins'),
        rootText(walked.events, 'reject'),
        `${name}: last wins is the engine's value`,
      )
      assert.throws(() => rootValue(inc.events, 'reject'), (e: any) => 'DUPLICATE_MEMBER' === e.code)
    }
  })

  // The grammars with `map.extend` off replace the earlier value whatever
  // the shapes, so both are streamed and last wins.
  it('a repeated member a grammar replaces streams both values whatever their shapes', () => {
    for (const name of ['json', 'jsonl', 'jsonc']) {
      if (!isIncremental(name)) continue
      for (const text of [
        '{"a":{"x":1},"a":{"y":2}}',
        '{"a":[1],"a":2}',
        '{"a":1,"a":{"y":2}}',
        '{"a":1,"a":1,"a":2}',
        '{"a":{"x":1},"a":{"x":1}}',
      ]) {
        const inc = incremental(name, text)
        assert.equal(inc.fail, undefined, `${name} ${text}: ${show(inc.fail)}`)
        assert.ok(wellFormed(inc.events), `${name} ${text}`)
        const walked = materialized(name, text)
        assert.equal(
          rootText(withoutLexemes(inc.events), 'last_wins'),
          rootText(walked.events, 'reject'),
          `${name} ${text}`,
        )
      }
    }
  })

  // jsonic's `map.extend`, which yaml and json5 inherit, merges the two
  // containers of a repeated name. The first was streamed before the second
  // arrived, so the merged member cannot be, and the run fails rather than
  // emit a stream that disagrees with the walk.
  it('a repeated container member the grammar merges fails with DUPLICATE_MEMBER', () => {
    const cases: [string, string, string][] = [
      ['yaml flow', 'yaml', 'a: {x: 1}\na: {y: 2}\n'],
      ['yaml block', 'yaml', 'a:\n  x: 1\na:\n  y: 2\n'],
      ['json5', 'json5', '{a:{x:1},a:{y:2}}'],
      ['jsonic', 'jsonic', 'a:{x:1},a:{y:2}'],
    ]
    for (const [label, name, text] of cases) {
      const inc = incremental(name, text)
      assert.equal(code(inc.fail), 'DUPLICATE_MEMBER', `${label}: ${show(inc.fail)}`)
      assert.ok((inc.fail as Fail).message.includes('materialize'), label)
      assert.ok(wellFormed(inc.events), `${label}: what left is a valid prefix`)
      assert.ok(!hasEnd(inc.events), label)
      const walked = materialized(name, text)
      assert.equal(walked.fail, undefined, label)
      assert.equal(rootText(walked.events, 'reject'), '{"a":{"x":1,"y":2}}', label)
    }
  })

  // zon refuses a repeated field itself, before jsonic's assignment, so the
  // incremental run fails exactly as the walk does (the grammar's code and
  // position), after a protocol-valid prefix and before `end`, and the
  // member the grammar never stored is not streamed.
  it('every repeated-field zon fixture fails as the walk does after a protocol-valid prefix', () => {
    const k = (key: string): JsonEvent => ({ type: 'key', key })
    const n = (value: number): JsonEvent => ({ type: 'number', value, lexeme: null })
    const os: JsonEvent = { type: 'object_start' }
    const oe: JsonEvent = { type: 'object_end' }
    const as: JsonEvent = { type: 'array_start' }
    const ae: JsonEvent = { type: 'array_end' }
    const cases: [string, JsonEvent[]][] = [
      ['repeated-scalar.zon', [os, k('a'), n(1), k('a')]],
      ['repeated-scalar-then-struct.zon', [os, k('a'), n(1), k('a'), os, k('y'), n(2), oe]],
      ['repeated-struct-then-scalar.zon', [os, k('a'), os, k('x'), n(1), oe, k('a')]],
      ['repeated-structs.zon', [os, k('a'), os, k('x'), n(1), oe, k('a'), os, k('y'), n(2), oe]],
      ['repeated-scalar-then-tuple.zon', [os, k('a'), n(1), k('a'), as, n(7), n(8), ae]],
      ['repeated-after-another.zon', [os, k('a'), n(1), k('b'), n(5), k('a'), os, k('y'), n(2), oe]],
      ['repeated-nested.zon', [os, k('o'), os, k('a'), n(1), k('a'), os, k('y'), n(2), oe]],
    ]
    for (const [name, prefix] of cases) {
      const text = fixture(name)
      const whole = materialized('zon', text)
      const expected = whole.fail as Fail
      assert.equal(code(expected), 'INPUT_INVALID', `${name}: ${show(expected)}`)
      assert.ok(expected.message.includes('zon_dup_field'), `${name}: the grammar's own guard`)
      assert.equal(whole.events.length, 0, `${name}: the walk emits nothing`)
      const inc = incremental('zon', text)
      const err = inc.fail as Fail
      assert.equal(code(err), expected.code, `${name}: ${show(err)}`)
      assert.equal(err.message, expected.message, name)
      assert.deepEqual([err.row, err.col], [expected.row, expected.col], `${name}: the position`)
      assert.ok(wellFormed(inc.events), name)
      assert.ok(!hasEnd(inc.events), name)
      assert.ok(
        sameEvents(withoutLexemes(inc.events), prefix),
        `${name}: the member the grammar never stored is not streamed: ` +
          inc.events.map(eventText).join(' '),
      )
      // A router consumer sees the grammar's failure, not a protocol one.
      const out = new SelectedRecorder()
      const router = new Router(
        [CaptureSpec.materialize('root', Selector.root())],
        Limits.default(),
        'last_wins',
        new Metrics(),
        out,
      )
      assert.throws(
        () =>
          new ParserSource(GRAMMARS.zon(), text)
            .unverified()
            .mode(SourceMode.incremental())
            .run(router),
        (e: any) => 'INPUT_INVALID' === e.code,
        name,
      )
      assert.equal(out.selections.length, 0, `${name}: nothing delivered`)
    }
  })

  // YAML resolves a `<<` merge key when the mapping closes: the members the
  // adapter streamed are no longer the map's, so the run is refused.
  it('a map the grammar rewrites after streaming is refused', () => {
    let text = 'base: &b\n  x: 1\nd:\n  <<: *b\n  y: 2\n'
    const walked = materialized('yaml', text)
    assert.equal(walked.fail, undefined)
    assert.equal(rootText(walked.events, 'reject'), '{"base":{"x":1},"d":{"y":2,"x":1}}')
    const inc = incremental('yaml', text)
    assert.equal(code(inc.fail), 'STREAMABILITY_UNKNOWN', show(inc.fail))
    assert.ok((inc.fail as Fail).message.includes('merge key'), show(inc.fail))
    assert.ok(wellFormed(inc.events))
    assert.ok(!hasEnd(inc.events))

    // An alias without a merge key copies the value and streams as the walk.
    text = 'a: &r {x: 1}\nb: *r\n'
    const alias = incremental('yaml', text)
    assert.equal(alias.flow, 'continue', show(alias.fail))
    assert.ok(sameEvents(withoutLexemes(alias.events), materialized('yaml', text).events))
  })

  // jsonic parses a pair inside a list and drops it when `list.pair` is off
  // (the default): `[a:{b:1}]` reads as `[]`. The pair's value was streamed
  // before the grammar dropped it, so the run is refused where the
  // grammar's next step shows it.
  it('a container the grammar streamed and never stored is refused', () => {
    const walked = materialized('jsonic', '[a:{b:1}]')
    assert.equal(walked.fail, undefined)
    assert.equal(rootText(walked.events, 'reject'), '[]')
    for (const text of ['[a:{b:1}]', '[a:{b:1},2]', '[a:{b:1},c:{d:2}]']) {
      const inc = incremental('jsonic', text)
      assert.equal(code(inc.fail), 'STREAMABILITY_UNKNOWN', `${text}: ${show(inc.fail)}`)
      assert.ok((inc.fail as Fail).message.includes('never stored it'), text)
      assert.ok(wellFormed(inc.events), text)
      assert.ok(!hasEnd(inc.events), text)
    }
    for (const text of ['[a:1]', '[1,a:1,2]']) {
      const inc = incremental('jsonic', text)
      assert.equal(inc.flow, 'continue', `${text}: ${show(inc.fail)}`)
      assert.ok(sameEvents(withoutLexemes(inc.events), materialized('jsonic', text).events), text)
    }
  })

  // A YAML key that is itself a mapping: the value's mapping is built before
  // the key is named, so the adapter refuses it when the value opens.
  it('a yaml key that is a mapping is refused before its value streams', () => {
    let text = '- sun: yellow\n- ? earth: blue\n  : moon: white\n'
    const walked = materialized('yaml', text)
    assert.equal(walked.fail, undefined)
    assert.equal(
      rootText(walked.events, 'reject'),
      '[{"sun":"yellow"},{"earth: blue":{"moon":"white"}}]',
    )
    const inc = incremental('yaml', text)
    assert.equal(code(inc.fail), 'STREAMABILITY_UNKNOWN', show(inc.fail))
    assert.ok((inc.fail as Fail).message.includes("before announcing the member's key"))
    assert.ok(wellFormed(inc.events))
    assert.ok(!hasEnd(inc.events))
    // An explicit key whose value is a scalar streams as the walk.
    text = '? earth\n: moon\n'
    const scalar = incremental('yaml', text)
    assert.equal(scalar.flow, 'continue', show(scalar.fail))
    assert.ok(sameEvents(withoutLexemes(scalar.events), materialized('yaml', text).events))
  })

  // A grammar that builds a member's value in a rule of its own and names
  // the member only when the pair closes, never announcing the key: `[1]`
  // parses to `{"k":[1]}`. Refused when the value opens.
  it('a container opened in a map before its key is refused', () => {
    const lateKey = () => {
      const tn: any = new Tabnas({ grammar$: false } as any)
      const OS = tn.token('#OS')
      const CS = tn.token('#CS')
      const NR = tn.token('#NR')
      const ZZ = tn.token('#ZZ')
      tn.rule('val', (rs: any) => {
        rs.bo((r: any) => {
          r.node = {}
        })
        rs.open([{ s: [OS], p: 'list' }])
        rs.close([
          {
            s: [ZZ],
            a: (r: any) => {
              r.node.k = r.child.node
            },
          },
        ])
      })
      tn.rule('list', (rs: any) => {
        rs.bo((r: any) => {
          r.node = []
        })
        rs.open([
          {
            s: [NR],
            a: (r: any) => {
              r.node.push(r.o0.val)
            },
          },
        ])
        rs.close([{ s: [CS] }])
      })
      return tn
    }
    const walked = materialized(lateKey, '[1]')
    assert.equal(walked.fail, undefined)
    assert.equal(rootText(walked.events, 'reject'), '{"k":[1]}')
    const inc = incremental(lateKey, '[1]')
    assert.equal(code(inc.fail), 'STREAMABILITY_UNKNOWN', show(inc.fail))
    assert.ok((inc.fail as Fail).message.includes("before announcing the member's key"))
    assert.ok((inc.fail as Fail).message.includes('materialize'))
    assert.ok(sameEvents(inc.events, [{ type: 'object_start' }]))
  })

  // The YAML root shapes: a single document streams as the walk; a stream of
  // several documents either streams exactly the walk or is refused with
  // STREAMABILITY_UNKNOWN naming the shape, before `end`.
  it('every yaml root shape fixture streams as the walk or is refused before end', () => {
    for (const name of ['empty.yaml', 'comment.yaml', 'scalar.yaml', 'marker.yaml']) {
      const text = fixture(name)
      const walked = materialized('yaml', text)
      assert.equal(walked.fail, undefined, name)
      const inc = incremental('yaml', text)
      assert.equal(inc.flow, 'continue', `${name}: ${show(inc.fail)}`)
      assert.ok(sameEvents(withoutLexemes(inc.events), walked.events), name)
    }
    for (const name of [
      'stream.yaml',
      'stream-scalars.yaml',
      'stream-sequences.yaml',
      'stream-map-scalar.yaml',
      'stream-scalar-map.yaml',
      'stream-empty-map.yaml',
      'stream-map-empty.yaml',
    ]) {
      const text = fixture(name)
      const walked = materialized('yaml', text)
      assert.equal(walked.fail, undefined, name)
      assert.equal(walked.events[0]?.type, 'array_start', `${name}: the walk sees a list`)
      const inc = incremental('yaml', text)
      if (undefined === inc.fail) {
        assert.ok(sameEvents(withoutLexemes(inc.events), walked.events), name)
      } else {
        assert.equal(code(inc.fail), 'STREAMABILITY_UNKNOWN', `${name}: ${show(inc.fail)}`)
        assert.ok((inc.fail as Fail).message.includes('several documents'), name)
        assert.ok(wellFormed(inc.events), name)
        assert.ok(!hasEnd(inc.events), name)
      }
    }
  })

  // markdown builds nodes imperatively; richer documents than the fixture
  // are checked here, while it is listed.
  it('markdown documents stream as the walk', () => {
    if (!isIncremental('markdown')) return
    for (const text of [
      '# Title\n\nSome *emphasis* and a [link](http://x).\n\n- one\n- two\n  - nested\n\n```rust\nfn x() {}\n```\n\n| a | b |\n|---|---|\n| 1 | 2 |\n\n> quote\n\n1. first\n2. second\n',
      'para one\npara one continued\n\npara two\n',
      '',
      '***\n\n# A\n## B\n### C\n',
      'text with `code` and **bold** and ![img](u) end\n',
      '- a\n\n  b\n- c\n\n> - d\n> - e\n',
    ]) {
      const walked = materialized('markdown', text)
      assert.equal(walked.fail, undefined)
      const inc = incremental('markdown', text)
      assert.equal(inc.flow, 'continue', `${JSON.stringify(text)}: ${show(inc.fail)}`)
      assert.ok(sameEvents(withoutLexemes(inc.events), walked.events), JSON.stringify(text))
    }
  })

  // The source consults the list by the grammar's name: an unlisted grammar
  // in incremental mode is refused before the parse, with nothing emitted,
  // and a listed one runs.
  it('an unlisted grammar in incremental mode is refused before it emits anything', () => {
    const text = fixture('sample.csv')
    let refused = 0
    for (const name of Object.keys(GRAMMARS)) {
      const recorder = new EventRecorder()
      let fail: unknown
      try {
        new ParserSource(GRAMMARS[name](), text)
          .grammar(name)
          .mode(SourceMode.incremental())
          .run(recorder)
      } catch (e) {
        fail = e
      }
      if (isIncremental(name)) {
        assert.notEqual(code(fail), 'STREAMABILITY_UNKNOWN', `${name}: ${show(fail)}`)
      } else {
        assert.equal(code(fail), 'STREAMABILITY_UNKNOWN', name)
        assert.ok((fail as Fail).message.includes(name), show(fail))
        assert.equal(recorder.events.length, 0, `${name}: nothing left the source`)
        refused++
      }
    }
    assert.equal(refused, Object.keys(GRAMMARS).length - INCREMENTAL.length)
  })
})

// Code is imported for its type in the assertions above.
void Code
