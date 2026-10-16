/* Copyright (c) 2026 tabnas, MIT License */

// test/spec/parser-events.tsv, run through @tabnas/support's runner as every tabnas
// repository runs its shared fixtures. What the columns mean and how the
// result is encoded is docs/reference.md's "Shared fixtures"; the harness
// is ./common.ts.

import { parserEvents, runFixture } from './common'

runFixture('parser-events.tsv', 'input', (input, row) => parserEvents(row, input))
