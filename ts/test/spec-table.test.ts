/* Copyright (c) 2026 tabnas, MIT License */

// test/spec/table.tsv, run through @tabnas/support's runner as every tabnas
// repository runs its shared fixtures. What the columns mean and how the
// result is encoded is docs/reference.md's "Shared fixtures"; the harness
// is ./common.ts.

import { table, runFixture } from './common'

runFixture('table.tsv', 'input', (input, row) => table(row, input))
