/* Copyright (c) 2026 tabnas, MIT License */

// test/spec/events.tsv, run through @tabnas/support's runner as every tabnas
// repository runs its shared fixtures. What the columns mean and how the
// result is encoded is docs/reference.md's "Shared fixtures"; the harness
// is ./common.ts.

import { events, runFixture } from './common'

runFixture('events.tsv', 'input', (input, row) => events(row, input))

// Every fixture the directory holds has a runner; a new file added without
// one fails here rather than passing unread.
import { describe, it } from 'node:test'
import assert from 'node:assert'
import { loadSpecDir } from '@tabnas/support'
import { FIXTURES, specDir } from './common'

describe('fixtures', () => {
  it('every fixture has a runner', () => {
    const files = loadSpecDir(specDir()).map((spec: any) => spec.file).sort()
    assert.deepEqual(files, FIXTURES, 'each fixture has a spec-<name>.test.ts')
  })
})
