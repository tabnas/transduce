// test/spec/route.tsv, run through tabnas_support's runner as every tabnas
// repository runs its shared fixtures. What the columns mean and how the
// result is encoded is docs/reference.md's "Shared fixtures"; the
// harness is tests/common.

mod common;

use tabnas_support::Runner;

#[test]
fn route() {
    let runner = Runner::new_with_row(|input, row| {
        common::route(row, input).map_err(|fail| common::to_failure(&fail, row))
    })
    .input("input")
    .expected("expected");
    runner.spec(&common::rust_rows("route.tsv", &runner));
}
