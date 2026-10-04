/* Copyright (c) 2026 tabnas, MIT License */

// The grammars a line source parses with, @tabnas/json and @tabnas/csv, are
// optional peers: lines.ts loads each when a line source starts. Without
// the package, the run fails as a usage error that names it, and a failure
// inside an installed grammar is not reported as the grammar missing.

import { describe, it } from 'node:test'
import assert from 'node:assert'
import Module from 'node:module'

import { EventRecorder, LineFormat, LinesSource } from '../dist/transduce'

// Run fn with `require(name)` failing as a package that is not installed,
// naming `missing` (the package itself, or one it requires).
function without(name: string, fn: () => void, missing: string = name): void {
  const mod = Module as any
  const load = mod._load
  mod._load = function (request: string, ...rest: unknown[]) {
    if (request === name) {
      const error: any = new Error(`Cannot find module '${missing}'\nRequire stack:\n- ${name}`)
      error.code = 'MODULE_NOT_FOUND'
      throw error
    }
    return load.call(this, request, ...rest)
  }
  try {
    fn()
  } finally {
    mod._load = load
  }
}

describe('optional grammars', () => {
  it('a JSON Lines source without @tabnas/json names the package', () => {
    without('@tabnas/json', () => {
      assert.throws(
        () => new LinesSource('1\n', LineFormat.jsonl()).run(new EventRecorder()),
        (e: any) => e instanceof TypeError && /JSON Lines needs @tabnas\/json/.test(e.message),
      )
      assert.throws(
        () => new LinesSource(null, LineFormat.jsonl()).writer(new EventRecorder(), true),
        (e: any) => e instanceof TypeError && /@tabnas\/json/.test(e.message),
      )
    })
  })

  it('a CSV source without @tabnas/csv names the package', () => {
    without('@tabnas/csv', () => {
      assert.throws(
        () => new LinesSource('a,b\n1,2\n', LineFormat.csv()).run(new EventRecorder()),
        (e: any) => e instanceof TypeError && /CSV needs @tabnas\/csv/.test(e.message),
      )
    })
  })

  it('a package the grammar requires is reported as itself', () => {
    without(
      '@tabnas/csv',
      () => {
        assert.throws(
          () => new LinesSource('a,b\n1,2\n', LineFormat.csv()).run(new EventRecorder()),
          (e: any) =>
            !(e instanceof TypeError) && "Cannot find module '@tabnas/jsonic'" === e.message.split('\n')[0],
        )
      },
      '@tabnas/jsonic',
    )
  })

  it('both formats run once the grammars are installed', () => {
    const jsonl = new EventRecorder()
    assert.strictEqual(new LinesSource('1\n2\n', LineFormat.jsonl()).run(jsonl), 'continue')
    const csv = new EventRecorder()
    assert.strictEqual(new LinesSource('a,b\n1,2\n', LineFormat.csv()).run(csv), 'continue')
  })
})
