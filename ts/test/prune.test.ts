/* Copyright (c) 2026 tabnas, MIT License */

// The differential suite behind `capability.prunable`, ported from
// rs/tests/prune_test.rs.
//
// For every grammar in the devDependencies and every document the suite
// has for it, a `ParserEventSource` run that prunes must relay the same
// passes as one that does not, pass for pass in the fixture encoding, and
// end the same way: both complete, or both fail with the same code,
// position and message. One difference short of identity is the contract,
// and the suite counts it apart: a token whose value is a container of the
// tree carries what the tree holds when its pass is made, so the arrays in
// it may have been emptied, while its kind and its maps' members are as
// they were (a yaml alias, which the lexer resolves to the anchored node).
// The documents are the Rust crate's fixtures, offered to every grammar,
// the generated worked-example documents of each grammar's family, and
// every document of the grammar's own corpus, the test/spec/*.tsv rows of
// the sibling checkout. A grammar is listed only when no document differs,
// and the suite asserts the list in both directions. The second half
// measures what pruning is for: ten times the records leave the pruned
// tree as it was.

import { describe, it } from 'node:test'
import assert from 'node:assert'
import { readFileSync, readdirSync } from 'node:fs'
import { join } from 'node:path'

import { loadSpecDir } from '@tabnas/support'

import {
  Datum,
  Flow,
  PRUNABLE,
  ParserEvent,
  ParserEventSource,
  ParserSink,
  RulePass,
  isPrunable,
  parserEventToJSON,
} from '../dist/transduce'
import { GRAMMARS } from './common'
import { recordsCsv, recordsJson, recordsJsonl, recordsPgn, recordsXml, recordsYaml } from './support'

// The generated documents' records in the differential: as many shapes as
// 2,000 would hold, at a tenth of the parsing.
const RECORDS = 200

// The grammars, their generated documents and the sibling checkout whose
// corpus each reads.
const SUITE: { name: string; generated: string[]; repo: string }[] = [
  { name: 'json', generated: ['records.json'], repo: 'json' },
  { name: 'jsonl', generated: ['records.jsonl'], repo: 'jsonl' },
  { name: 'jsonic', generated: ['records.json'], repo: 'jsonic' },
  { name: 'jsonc', generated: ['records.json'], repo: 'jsonc' },
  { name: 'json5', generated: ['records.json'], repo: 'json5' },
  { name: 'yaml', generated: ['records.yaml'], repo: 'yaml' },
  { name: 'toml', generated: [], repo: 'toml' },
  { name: 'ini', generated: [], repo: 'ini' },
  { name: 'csv', generated: ['records.csv'], repo: 'csv' },
  { name: 'xml', generated: ['records.xml'], repo: 'xml' },
  { name: 'zon', generated: [], repo: 'zon' },
  { name: 'markdown', generated: [], repo: 'markdown' },
  { name: 'feed', generated: [], repo: 'feed' },
  { name: 'pgn', generated: ['records.pgn'], repo: 'chess' },
]

function generated(name: string, records: number): string {
  switch (name) {
    case 'records.json':
      return recordsJson(records)
    case 'records.jsonl':
      return recordsJsonl(records)
    case 'records.csv':
      return recordsCsv(records)
    case 'records.yaml':
      return recordsYaml(records)
    case 'records.xml':
      return recordsXml(records)
    case 'records.pgn':
      return recordsPgn(records)
  }
  throw new Error('no generator for ' + name)
}

const FIXTURES_DIR = join(__dirname, '..', '..', 'rs', 'tests', 'fixtures')

// What the suite has for one grammar: the Rust crate's fixtures, its
// generated documents and its own corpus.
function documents(grammar: (typeof SUITE)[number]): [string, string][] {
  const docs: [string, string][] = readdirSync(FIXTURES_DIR)
    .sort()
    .map((name) => [name, readFileSync(join(FIXTURES_DIR, name), 'utf8')])
  for (const name of grammar.generated) docs.push([name, generated(name, RECORDS)])
  const dir = join(__dirname, '..', '..', '..', grammar.repo, 'test', 'spec')
  let specs: any[]
  try {
    specs = loadSpecDir(dir)
  } catch (err) {
    throw new Error(`${err}: clone https://github.com/tabnas/${grammar.repo} beside this repository`)
  }
  for (const spec of specs) {
    for (const row of spec.rows) docs.push([row.where(), row.unescNamed('input')])
  }
  return docs
}

// The arrays in a datum emptied, its maps' members kept.
function emptied(datum: Datum): Datum {
  if ('array' === datum.type) return { type: 'array', items: [] }
  if ('object' === datum.type) {
    const members = new Map<string, Datum>()
    for (const [name, value] of datum.members) members.set(name, emptied(value))
    return { type: 'object', members }
  }
  return datum
}

// The pass with the arrays in its tokens' container values emptied.
function shaped(ev: ParserEvent): ParserEvent {
  if ('end' === ev.type) return ev
  const pass: RulePass = {
    ...ev.pass,
    tokens: ev.pass.tokens.map((token) => ({ ...token, val: emptied(token.val) })),
  }
  return { type: ev.type, pass }
}

// The passes, each in the fixture encoding, and again shaped as pruning
// may leave them.
class Passes implements ParserSink {
  exact: string[] = []
  shaped: string[] = []
  event(ev: ParserEvent): Flow {
    this.exact.push(JSON.stringify(parserEventToJSON(ev)))
    this.shaped.push(JSON.stringify(parserEventToJSON(shaped(ev))))
    return 'continue'
  }
}

// One run: how it ended, its passes and the engine's tree afterwards.
function run(name: string, text: string, prune: boolean) {
  const source = new ParserEventSource(GRAMMARS[name](), text)
  if (prune) source.prune().unverified()
  const passes = new Passes()
  try {
    const { flow, value } = source.runWithValue(passes)
    return { ending: `${flow}`, passes, value }
  } catch (fail: any) {
    const ending = `${fail?.code} at ${fail?.row}:${fail?.column}: ${fail?.message}`
    return { ending, passes, value: undefined }
  }
}

// How the pruned run compares with the one that did not prune: '' the
// same, 'shape' the same once the arrays in the tokens' container values
// are set aside, and otherwise where they differ.
function compare(name: string, text: string): string {
  const whole = run(name, text, false)
  const pruned = run(name, text, true)
  if (whole.ending !== pruned.ending) {
    return `ended ${whole.ending} without pruning, ${pruned.ending} pruned`
  }
  if (whole.passes.exact.join('\n') === pruned.passes.exact.join('\n')) return ''
  const kept = whole.passes.shaped
  const left = pruned.passes.shaped
  for (let i = 0; i < Math.max(kept.length, left.length); i++) {
    if (kept[i] !== left[i]) {
      return (
        `pass ${i} of ${kept.length} differs: ${whole.passes.exact[i]} without pruning, ` +
        `${pruned.passes.exact[i]} pruned`
      )
    }
  }
  return 'shape'
}

// Every element of every array in `value`, and every member of every map.
function elements(value: unknown): number {
  if (Array.isArray(value)) return value.length + value.reduce((n, v) => n + elements(v), 0)
  if (null !== value && 'object' === typeof value) {
    const members = Object.values(value as Record<string, unknown>)
    return members.length + members.reduce((n: number, v) => n + elements(v), 0)
  }
  return 0
}

// What the tree holds after a run of `records` records, pruned or not.
function left(name: string, doc: string, records: number, prune: boolean): number {
  const r = run(name, generated(doc, records), prune)
  assert.strictEqual(r.ending, 'continue', `${name} over ${records} records: ${r.ending}`)
  return elements(r.value)
}

describe('pruning', () => {
  it('every grammar prunes as the list says', () => {
    const wrong: string[] = []
    for (const grammar of SUITE) {
      const docs = documents(grammar)
      const found: string[] = []
      const shapedOnly: string[] = []
      docs.forEach(([name, text], i) => {
        const why = compare(grammar.name, text)
        if ('shape' === why) shapedOnly.push(name)
        else if ('' !== why) found.push(`${name}: ${why}`)
        if ((i + 1) % 200 === 0 || i + 1 === docs.length) {
          console.log(
            `${grammar.name}: ${i + 1} of ${docs.length} (${Math.floor(((i + 1) * 100) / docs.length)}%) ` +
              `documents, ${found.length} differing`,
          )
        }
      })
      if (0 < shapedOnly.length) {
        console.log(
          `${grammar.name}: ${shapedOnly.length} the same once the arrays in their tokens' ` +
            `containers are set aside: ${shapedOnly.join(', ')}`,
        )
      }
      const listed = isPrunable(grammar.name)
      if (listed && 0 < found.length) {
        wrong.push(
          `${grammar.name} is in capability.PRUNABLE, but ${found.length} of its documents ` +
            `differ:\n    ${found.join('\n    ')}`,
        )
      }
      if (!listed && 0 === found.length) {
        wrong.push(`${grammar.name} is not in capability.PRUNABLE, but none of its documents differs`)
      }
    }
    for (const name of PRUNABLE) {
      if (!SUITE.some((g) => g.name === name)) wrong.push(`${name} is listed, and this suite never runs it`)
    }
    assert.deepStrictEqual(wrong, [])
  })

  it('ten times the records leave the pruned tree as it was', () => {
    const pairs: [string, string][] = [
      ['json', 'records.json'],
      ['jsonc', 'records.json'],
      ['json5', 'records.json'],
      ['jsonic', 'records.json'],
      ['jsonl', 'records.jsonl'],
      ['yaml', 'records.yaml'],
      ['xml', 'records.xml'],
      ['csv', 'records.csv'],
      ['pgn', 'records.pgn'],
    ]
    const wrong: string[] = []
    pairs.forEach(([name, doc], i) => {
      const few = left(name, doc, 200, true)
      const many = left(name, doc, 2000, true)
      const wholeFew = left(name, doc, 200, false)
      const wholeMany = left(name, doc, 2000, false)
      console.log(
        `retention: ${i + 1} of ${pairs.length} (${Math.floor(((i + 1) * 100) / pairs.length)}%) ` +
          `${name}: pruned ${few} then ${many} elements, not pruned ${wholeFew} then ${wholeMany}`,
      )
      if (few !== many) {
        wrong.push(`${name}: the pruned tree held ${few} elements after 200 records and ${many} after 2000`)
      }
      if (wholeMany < wholeFew + 1800) {
        wrong.push(`${name}: the tree held ${wholeFew} then ${wholeMany} elements without pruning`)
      }
    })
    assert.deepStrictEqual(wrong, [])
  })

  it('a run that prunes needs a listed grammar, and sends nothing without one', () => {
    for (const name of [null, 'chess', 'Json']) {
      const source = new ParserEventSource(GRAMMARS.json(), '[1]').prune()
      if (null !== name) source.grammar(name)
      const passes = new Passes()
      assert.throws(
        () => source.run(passes),
        (fail: any) =>
          'STREAMABILITY_UNKNOWN' === fail.code && String(fail.message).includes('capability.prunable'),
      )
      assert.strictEqual(passes.exact.length, 0)
    }
    for (const source of [
      new ParserEventSource(GRAMMARS.json(), '[1]').grammar('json').prune(),
      new ParserEventSource(GRAMMARS.json(), '[1]').prune().unverified(),
      new ParserEventSource(GRAMMARS.json(), '[1]').grammar('chess'),
    ]) {
      const passes = new Passes()
      assert.strictEqual(source.run(passes), 'continue')
      assert.strictEqual(passes.exact[passes.exact.length - 1], '["end"]')
    }
  })

  it('pruning empties the tree\'s arrays and leaves the passes as they were', () => {
    const src = '{"a":[1,2,3],"b":[[4],[5,{"c":[6]}]],"d":{"e":"f"}}'
    const whole = run('json', src, false)
    const pruned = run('json', src, true)
    // The engine's maps have no prototype; their JSON is what is compared.
    const json = (value: unknown) => JSON.parse(JSON.stringify(value))
    assert.deepStrictEqual(pruned.passes.exact, whole.passes.exact)
    assert.deepStrictEqual(json(whole.value), JSON.parse(src))
    assert.deepStrictEqual(json(pruned.value), { a: [], b: [], d: { e: 'f' } })
  })
})
