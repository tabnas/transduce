// test/spec/scan.tsv, run through tabnas_support's runner as every tabnas
// repository runs its shared fixtures. What the columns mean and how the
// result is encoded is docs/reference.md's "Shared fixtures"; the
// harness is tests/common.

mod common;

use tabnas_support::Runner;

#[test]
fn scan() {
    Runner::new_with_row(|input, row| {
        common::scan(row, input).map_err(|fail| common::to_failure(&fail, row))
    })
    .input("script")
    .expected("expected")
    .file(common::spec_dir().join("scan.tsv"));
}
