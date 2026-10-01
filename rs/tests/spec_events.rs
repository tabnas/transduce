// test/spec/events.tsv, run through tabnas_support's runner as every tabnas
// repository runs its shared fixtures. What the columns mean and how the
// result is encoded is docs/reference.md's "Shared fixtures"; the
// harness is tests/common.

mod common;

use tabnas_support::Runner;

#[test]
fn events() {
    Runner::new_with_row(|input, row| {
        common::events(row, input).map_err(|fail| common::to_failure(&fail, row))
    })
    .input("input")
    .expected("expected")
    .file(common::spec_dir().join("events.tsv"));
}

/// Every fixture the directory holds has a runner; a new file added
/// without one fails here rather than passing unread.
#[test]
fn every_fixture_has_a_runner() {
    let files: Vec<String> = tabnas_support::load_spec_dir(common::spec_dir(), &Default::default())
        .expect("the fixtures load")
        .into_iter()
        .map(|spec| spec.file)
        .collect();
    assert_eq!(files, common::FIXTURES, "each fixture has a spec_<name>.rs");
}
