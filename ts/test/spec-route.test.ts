/* Copyright (c) 2026 tabnas, MIT License */

// test/spec/route.tsv, run through @tabnas/support's runner as every tabnas
// repository runs its shared fixtures. What the columns mean and how the
// result is encoded is docs/reference.md's "Shared fixtures"; the harness
// is ./common.ts.

import { route, runFixture } from './common'

runFixture('route.tsv', 'input', (input, row) => route(row, input))
