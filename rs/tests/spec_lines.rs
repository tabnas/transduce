// test/spec/lines.tsv, run through tabnas_support's runner as every tabnas
// repository runs its shared fixtures. What the columns mean and how the
// result is encoded is docs/reference.md's "Shared fixtures"; the
// harness is tests/common.

mod common;

use tabnas_support::Runner;

/// lines.tsv is the TypeScript and Go ports' alone since step 12 of
/// alchemy's streaming plan retired this crate's line source: every row
/// names a line source's mode, which this crate skips, so none runs here;
/// a row of another mode would have to.
#[test]
fn lines() {
    let runner = Runner::new_with_row(|input, row| {
        common::events(row, input).map_err(|fail| common::to_failure(&fail, row))
    })
    .input("input")
    .expected("expected");
    let all =
        tabnas_support::load_spec(common::spec_dir().join("lines.tsv"), runner.load_options())
            .expect("lines.tsv loads");
    assert!(!all.rows.is_empty(), "the ports' rows are there");
    let ours = common::rust_rows("lines.tsv", &runner);
    assert!(ours.rows.is_empty(), "every row is a line source's");
}
