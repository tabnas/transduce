/* Copyright (c) 2026 tabnas, MIT License */

// The differential suite behind `capability.prunable` and
// `capability.prunesMaps`, ported from rs/tests/prune_test.rs.
//
// For every grammar in the devDependencies and every document the suite
// has for it, a `ParserEventSource` run that prunes must relay the same
// passes as one that does not, pass for pass in the fixture encoding, and
// end the same way: both complete, or both fail with the same code,
// position and message. The suite runs each document twice pruned, with the
// grammar's maps left and with them emptied (`unverifiedMaps`), and holds
// each list to its runs. One difference short of identity is the contract,
// and the suite counts it apart: a token whose value is a container of the
// tree carries what the tree holds when its pass is made, so the arrays in
// it may have been emptied, and its maps too in the run that empties them,
// while its kind is as it was (a yaml alias, which the lexer resolves to the
// anchored node).
// The documents are the Rust crate's fixtures, offered to every grammar,
// the generated worked-example documents of each grammar's family, and
// every document of the grammar's own corpus, the test/spec/*.tsv rows of
// the sibling checkout. A grammar is listed only when no document differs,
// and the suite asserts both lists in both directions. The second half
// measures what pruning is for: ten times the records leave the pruned
// tree as it was, and so do ten times an INI document's sections, a JSON
// object's members and a toml document's array of tables. A stylesheet's
// rules and a grammar's are not measured here: their maps are not emptied
// in this runtime, so the rules stay in the tree (DIVERGENCE.md 7).

import { describe, it } from 'node:test'
import assert from 'node:assert'
import { readFileSync, readdirSync } from 'node:fs'
import { join } from 'node:path'
import { isDeepStrictEqual } from 'node:util'

import { loadSpecDir } from '@tabnas/support'

import {
  BOUNDS_HISTORY,
  Datum,
  Flow,
  PRUNABLE,
  PRUNES_MAPS,
  PRUNE_HOLD,
  ParserEvent,
  ParserEventSource,
  ParserSink,
  RulePass,
  boundsHistory,
  isPrunable,
  parserEventToJSON,
  prunesMaps,
} from '../dist/transduce'
import { GRAMMARS } from './common'
import { recordsCsv, recordsJson, recordsJsonl, recordsPgn, recordsXml, recordsYaml } from './support'

// The generated documents' records in the differential: as many shapes as
// 2,000 would hold, at a tenth of the parsing.
const RECORDS = 200

// The grammars, their generated documents and the sibling checkout whose
// corpus each reads.
const SUITE: { name: string; generated: string[]; repo: string }[] = [
  { name: 'json', generated: ['records.json', 'members.json'], repo: 'json' },
  { name: 'jsonl', generated: ['records.jsonl'], repo: 'jsonl' },
  { name: 'jsonic', generated: ['records.json'], repo: 'jsonic' },
  { name: 'jsonc', generated: ['records.json'], repo: 'jsonc' },
  { name: 'json5', generated: ['records.json'], repo: 'json5' },
  { name: 'yaml', generated: ['records.yaml'], repo: 'yaml' },
  { name: 'toml', generated: ['tables.toml'], repo: 'toml' },
  { name: 'ini', generated: ['sections.ini'], repo: 'ini' },
  { name: 'csv', generated: ['records.csv'], repo: 'csv' },
  { name: 'xml', generated: ['records.xml'], repo: 'xml' },
  { name: 'zon', generated: [], repo: 'zon' },
  { name: 'markdown', generated: ['paragraphs.md'], repo: 'markdown' },
  { name: 'feed', generated: [], repo: 'feed' },
  { name: 'pgn', generated: ['records.pgn'], repo: 'chess' },
  { name: 'css', generated: ['rules.css'], repo: 'css' },
  { name: 'abnf', generated: ['rules.abnf'], repo: 'abnf' },
  { name: 'ebnf', generated: ['rules.ebnf'], repo: 'ebnf' },
  { name: 'gbnf', generated: ['rules.gbnf'], repo: 'gbnf' },
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
    case 'members.json':
      return membersJson(records)
    case 'sections.ini':
      return sectionsIni(records)
    case 'tables.toml':
      return tablesToml(records)
    case 'paragraphs.md':
      return paragraphsMd(records)
    case 'rules.css':
      return rulesCss(records)
    case 'rules.abnf':
      return rulesAbnf(records)
    case 'rules.ebnf':
      return rulesEbnf(records)
    case 'rules.gbnf':
      return rulesGbnf(records)
  }
  throw new Error('no generator for ' + name)
}

// A stylesheet of `records` rules, each of two declarations.
function rulesCss(records: number): string {
  let out = ''
  for (let i = 0; i < records; i++) out += `.c${i} { margin: ${i}px; content: "the text of rule ${i}"; }\n`
  return out
}

// An ABNF grammar of `records` rules, each of two alternatives, its lines
// ended as RFC 5234 ends them.
function rulesAbnf(records: number): string {
  let out = ''
  for (let i = 0; i < records; i++) out += `r${i} = "a${i} the text of rule ${i}" / "b${i}"\r\n`
  return out
}

// An EBNF grammar of `records` rules, each of two alternatives.
function rulesEbnf(records: number): string {
  let out = ''
  for (let i = 0; i < records; i++) out += `r${i} ::= "a${i} the text of rule ${i}" | "b${i}"\n`
  return out
}

// A GBNF grammar of `records` rules, each of two alternatives, after the
// `root` that starts it.
function rulesGbnf(records: number): string {
  let out = 'root ::= r0\n'
  for (let i = 0; i < records; i++) out += `r${i} ::= "a${i} the text of rule ${i}" | "b${i}"\n`
  return out
}

// A JSON object of `records` members, each an object of three.
function membersJson(records: number): string {
  const members: string[] = []
  for (let i = 0; i < records; i++) {
    members.push(`"k${i}": {"id": ${i}, "name": "n${i}", "tags": ["a", "b"]}`)
  }
  return `{\n${members.join(',\n')}\n}\n`
}

// An INI document of `records` sections, each of three keys.
function sectionsIni(records: number): string {
  let out = ''
  for (let i = 0; i < records; i++) out += `[s${i}]\nid = ${i}\nname = n${i}\ntext = the text of section ${i}\n\n`
  return out
}

// A markdown document of `records` paragraphs.
function paragraphsMd(records: number): string {
  let out = ''
  for (let i = 0; i < records; i++) out += `Paragraph ${i}. The text of paragraph ${i}, *with* some \`code\`.\n\n`
  return out
}

// A toml array of `records` tables, each of three keys.
function tablesToml(records: number): string {
  let out = ''
  for (let i = 0; i < records; i++) out += `[[items]]\nid = ${i}\nname = "n${i}"\ntext = "the text of table ${i}"\n\n`
  return out
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

// How a run prunes: not at all, the arrays, or the arrays and the maps,
// whatever the grammar.
type Pruning = 'no' | 'arrays' | 'maps'

// The arrays in a datum emptied, and the maps when `maps`; a map that is
// kept keeps its members, their own containers emptied.
function emptied(datum: Datum, maps: boolean): Datum {
  if ('array' === datum.type) return { type: 'array', items: [] }
  if ('object' === datum.type) {
    const members = new Map<string, Datum>()
    if (!maps) for (const [name, value] of datum.members) members.set(name, emptied(value, maps))
    return { type: 'object', members }
  }
  return datum
}

// The pass with the arrays in its tokens' container values emptied, and
// the maps when `maps`.
function shaped(ev: ParserEvent, maps: boolean): ParserEvent {
  // The suite asks for no token item; one is no grammar's value.
  if ('end' === ev.type || 'token' === ev.type) return ev
  const pass: RulePass = {
    ...ev.pass,
    tokens: ev.pass.tokens.map((token) => ({ ...token, val: emptied(token.val, maps) })),
  }
  return { type: ev.type, pass }
}

// The passes, each in the fixture encoding, and again shaped as pruning
// may leave them.
class Passes implements ParserSink {
  exact: string[] = []
  shaped: string[] = []
  constructor(private readonly maps = false) {}
  event(ev: ParserEvent): Flow {
    this.exact.push(JSON.stringify(parserEventToJSON(ev)))
    this.shaped.push(JSON.stringify(parserEventToJSON(shaped(ev, this.maps))))
    return 'continue'
  }
}

// One run: how it ended, its passes, shaped as a run pruning as `shape`
// would leave them, and the engine's tree afterwards. A run that prunes a
// grammar `capability.boundsHistory` lists bounds the rule history, as a
// host's does.
function run(name: string, text: string, pruning: Pruning, shape: Pruning = pruning) {
  const source = new ParserEventSource(GRAMMARS[name](), text)
  if ('arrays' === pruning) source.prune().unverified()
  if ('maps' === pruning) source.prune().unverifiedMaps()
  if ('no' !== pruning && boundsHistory(name)) source.unverifiedHistory()
  const passes = new Passes('maps' === shape)
  try {
    const { flow, value } = source.runWithValue(passes)
    return { ending: `${flow}`, passes, value }
  } catch (fail: any) {
    const ending = `${fail?.code} at ${fail?.row}:${fail?.column}: ${fail?.message}`
    return { ending, passes, value: undefined }
  }
}

// How the pruned run compares with the one that did not prune: '' the
// same, 'shape' the same once the containers in the tokens' values are set
// aside, and otherwise where they differ.
function compare(name: string, text: string, pruning: Pruning): string {
  const whole = run(name, text, 'no', pruning)
  const pruned = run(name, text, pruning)
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

// One run of the history suite: how it ended, its passes in the fixture
// encoding and the value the parse returned, with the rule history bounded
// at `STREAM_RULE_HISTORY` or left as the engine leaves it. Neither prunes.
function historyRun(name: string, text: string, bounded: boolean) {
  const source = new ParserEventSource(GRAMMARS[name](), text)
  if (bounded) source.unverifiedHistory()
  const passes = new Passes()
  try {
    const { flow, value } = source.runWithValue(passes)
    return { ending: `${flow}`, passes: passes.exact, value }
  } catch (fail: any) {
    const ending = `${fail?.code} at ${fail?.row}:${fail?.column}: ${fail?.message}`
    return { ending, passes: passes.exact, value: undefined }
  }
}

// A value as a short text, for a message.
function preview(value: unknown): string {
  let text: string
  try {
    text = JSON.stringify(value) ?? String(value)
  } catch {
    text = String(value)
  }
  return 120 < text.length ? text.slice(0, 117) + '...' : text
}

// How the bounded run compares with the unbounded one: '' alike, and
// otherwise where they differ, by the ending, a pass or the value.
function historyCompare(name: string, text: string): string {
  const unbounded = historyRun(name, text, false)
  const bounded = historyRun(name, text, true)
  if (unbounded.ending !== bounded.ending) {
    return `ended ${unbounded.ending} unbounded, ${bounded.ending} bounded`
  }
  const a = unbounded.passes
  const b = bounded.passes
  for (let i = 0; i < Math.max(a.length, b.length); i++) {
    if (a[i] !== b[i]) {
      return `pass ${i} of ${a.length} differs: ${a[i] ?? 'nothing'} unbounded, ${b[i] ?? 'nothing'} bounded`
    }
  }
  if (!isDeepStrictEqual(unbounded.value, bounded.value)) {
    return `the value differs: ${preview(unbounded.value)} unbounded, ${preview(bounded.value)} bounded`
  }
  return ''
}

// Every element of every array in `value` that holds something, and every
// member of every map: what the tree keeps reachable. An element pruning set
// to `undefined` keeps its place and holds nothing.
function elements(value: unknown): number {
  if (Array.isArray(value)) {
    return value.reduce((n: number, v) => n + (undefined === v ? 0 : 1 + elements(v)), 0)
  }
  if (null !== value && 'object' === typeof value) {
    const members = Object.values(value as Record<string, unknown>)
    return members.length + members.reduce((n: number, v) => n + elements(v), 0)
  }
  return 0
}

// What the tree holds after a run of `records` records, pruned or not:
// pruned as a host prunes, by the grammar's name and its verified lists.
function left(name: string, doc: string, records: number, prune: boolean): number {
  const source = new ParserEventSource(GRAMMARS[name](), generated(doc, records)).grammar(name)
  if (prune) source.prune()
  const { flow, value } = source.runWithValue(new Passes())
  assert.strictEqual(`${flow}`, 'continue', `${name} over ${records} records`)
  return elements(value)
}

describe('pruning', () => {
  it('every grammar prunes as the lists say', () => {
    const wrong: string[] = []
    // A grammar's documents, pruned as `pruning` says and not: the
    // differences.
    const differences = (grammar: (typeof SUITE)[number], docs: [string, string][], pruning: Pruning) => {
      const how = 'maps' === pruning ? 'with its maps emptied' : 'pruned'
      const found: string[] = []
      const shapedOnly: string[] = []
      docs.forEach(([name, text], i) => {
        const why = compare(grammar.name, text, pruning)
        if ('shape' === why) shapedOnly.push(name)
        else if ('' !== why) found.push(`${name}: ${why}`)
        if ((i + 1) % 200 === 0 || i + 1 === docs.length) {
          console.log(
            `${grammar.name} ${how}: ${i + 1} of ${docs.length} ` +
              `(${Math.floor(((i + 1) * 100) / docs.length)}%) documents, ${found.length} differing`,
          )
        }
      })
      if (0 < shapedOnly.length) {
        console.log(
          `${grammar.name} ${how}: ${shapedOnly.length} the same once the containers in their tokens' ` +
            `values are set aside: ${shapedOnly.join(', ')}`,
        )
      }
      return found
    }
    for (const grammar of SUITE) {
      const docs = documents(grammar)
      const found = differences(grammar, docs, 'arrays')
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
      if (!listed) {
        if (prunesMaps(grammar.name)) wrong.push(`${grammar.name} is in capability.PRUNES_MAPS, and not prunable`)
        continue
      }
      const foundMaps = differences(grammar, docs, 'maps')
      const listedMaps = prunesMaps(grammar.name)
      if (listedMaps && 0 < foundMaps.length) {
        wrong.push(
          `${grammar.name} is in capability.PRUNES_MAPS, but ${foundMaps.length} of its documents ` +
            `differ with its maps emptied:\n    ${foundMaps.join('\n    ')}`,
        )
      }
      if (!listedMaps && 0 === foundMaps.length) {
        wrong.push(
          `${grammar.name} is not in capability.PRUNES_MAPS, but none of its documents differs with its ` +
            'maps emptied',
        )
      }
      if (!listedMaps && 0 < foundMaps.length) {
        console.log(`${grammar.name} with its maps emptied: ${foundMaps.length} documents differ, the first ${foundMaps[0]}`)
      }
    }
    for (const name of [...PRUNABLE, ...PRUNES_MAPS]) {
      if (!SUITE.some((g) => g.name === name)) wrong.push(`${name} is listed, and this suite never runs it`)
    }
    assert.deepStrictEqual(wrong, [])
  })

  it('every grammar bounds the rule history as the list says', () => {
    const wrong: string[] = []
    for (const grammar of SUITE) {
      const docs = documents(grammar)
      const found: string[] = []
      docs.forEach(([name, text], i) => {
        const why = historyCompare(grammar.name, text)
        if ('' !== why) found.push(`${name}: ${why}`)
        if ((i + 1) % 200 === 0 || i + 1 === docs.length) {
          console.log(
            `${grammar.name} bounded: ${i + 1} of ${docs.length} ` +
              `(${Math.floor(((i + 1) * 100) / docs.length)}%) documents, ${found.length} differing`,
          )
        }
      })
      const listed = boundsHistory(grammar.name)
      if (listed && 0 < found.length) {
        wrong.push(
          `${grammar.name} is in capability.BOUNDS_HISTORY, but ${found.length} of its documents ` +
            `differ bounded:\n    ${found.join('\n    ')}`,
        )
      }
      if (!listed && 0 === found.length) {
        wrong.push(`${grammar.name} is not in capability.BOUNDS_HISTORY, but none of its documents differs bounded`)
      }
      if (!listed && 0 < found.length) {
        console.log(`${grammar.name} bounded: ${found.length} documents differ, the first ${found[0]}`)
      }
    }
    for (const name of BOUNDS_HISTORY) {
      if (!SUITE.some((g) => g.name === name)) wrong.push(`${name} is listed, and this suite never runs it`)
      if (!isPrunable(name)) wrong.push(`${name} is in capability.BOUNDS_HISTORY, and not prunable`)
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
      ['json', 'members.json'],
      ['ini', 'sections.ini'],
      ['toml', 'tables.toml'],
      ['markdown', 'paragraphs.md'],
      ['css', 'rules.css'],
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
    const whole = run('json', src, 'no')
    const pruned = run('json', src, 'arrays')
    // The engine's maps have no prototype; their JSON is what is compared.
    const json = (value: unknown) => JSON.parse(JSON.stringify(value))
    assert.deepStrictEqual(pruned.passes.exact, whole.passes.exact)
    assert.deepStrictEqual(json(whole.value), JSON.parse(src))
    assert.deepStrictEqual(json(pruned.value), { a: [], b: [], d: { e: 'f' } })
  })

  it('pruning a grammar whose maps are emptied empties them too, and leaves the passes', () => {
    const src = '{"a":[1,2,3],"b":[[4],[5,{"c":[6]}]],"d":{"e":"f"}}'
    const whole = run('json', src, 'no')
    const passes = new Passes()
    const { flow, value } = new ParserEventSource(GRAMMARS.json(), src).grammar('json').prune().runWithValue(passes)
    assert.strictEqual(`${flow}`, 'continue')
    assert.deepStrictEqual(passes.exact, whole.passes.exact)
    assert.deepStrictEqual(JSON.parse(JSON.stringify(value)), {})
    // toml reads its maps back, so its maps are left: a table keeps its keys.
    const toml = new ParserEventSource(GRAMMARS.toml(), '[a]\nb = 1\n').grammar('toml').prune()
    assert.deepStrictEqual(JSON.parse(JSON.stringify(toml.runWithValue(new Passes()).value)), { a: { b: 1 } })
  })

  // A grammar holds the pruning off with a count above zero under
  // PRUNE_HOLD in the parse context's bag, as @tabnas/yaml does while a
  // value with an anchor is open: while it is held the source empties no
  // array, and a count of zero holds nothing.
  it('a held pruning empties no array', () => {
    const src = '{"a":[1,2,3],"b":[[4],[5,{"c":[6]}]],"d":{"e":"f"}}'
    const json = (value: unknown) => JSON.parse(JSON.stringify(value))
    const held = (count: number) => {
      const parser = GRAMMARS.json()
      parser.options({ parse: { prepare: { hold: (_: unknown, ctx: any) => (ctx.u[PRUNE_HOLD] = count) } } })
      const source = new ParserEventSource(parser, src).prune().unverified()
      const passes = new Passes()
      const { flow, value } = source.runWithValue(passes)
      assert.strictEqual(`${flow}`, 'continue')
      return { passes, value: json(value) }
    }
    const whole = run('json', src, 'no')
    const one = held(1)
    const zero = held(0)
    assert.deepStrictEqual(one.passes.exact, whole.passes.exact)
    assert.deepStrictEqual(one.value, JSON.parse(src))
    assert.deepStrictEqual(zero.value, { a: [], b: [], d: { e: 'f' } })
  })
})
