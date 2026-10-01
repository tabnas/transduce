// test/spec/lines.tsv, run through tabnas_support's runner as every tabnas
// repository runs its shared fixtures. What the columns mean and how the
// result is encoded is docs/reference.md's "Shared fixtures"; the
// harness is tests/common.

mod common;

use tabnas_support::Runner;

#[test]
fn lines() {
    Runner::new_with_row(|input, row| {
        common::events(row, input).map_err(|fail| common::to_failure(&fail, row))
    })
    .input("input")
    .expected("expected")
    .file(common::spec_dir().join("lines.tsv"));
}
