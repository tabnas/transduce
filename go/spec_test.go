// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

// The shared fixtures in ../test/spec, run through tabnas-support's
// Runner as every tabnas repository runs its fixtures; the harness is
// harness_test.go. Every row runs, except the rows specDivergences lists,
// each a measured difference between this runtime and Rust that
// DIVERGENCE.md records.

import (
	"fmt"
	"path/filepath"
	"sort"
	"testing"

	support "github.com/tabnas/support/go"
)

// specDivergences is the rows this runtime does not reproduce, by file
// and then by grammar, mode and input (the input column as written in
// the fixture, tab-separated), and why. Each must be a difference
// DIVERGENCE.md records, and each must name a row: a stale entry fails
// the run. There are none today. The yaml `<<` merge-key order and the
// csv unterminated-quote position were listed here until tabnas/yaml#109
// and tabnas/csv#86 brought the Go grammars into line with Rust.
var specDivergences = map[string]map[string]string{}

func divergenceKey(row *support.Row, inputCol string) string {
	return row.Named("grammar") + "\t" + row.Named("mode") + "\t" + row.Named(inputCol)
}

type specCount struct{ rows, passed, skipped int }

func runSpec(t *testing.T, file, inputCol string, stage func(testing.TB, *support.Row, string) (any, *failed)) {
	spec, err := support.LoadSpec(filepath.Join(specDir(t), file), nil)
	if err != nil {
		t.Fatal(err)
	}
	runner := support.Runner{
		ParseRow: func(input string, row *support.Row) (any, error) {
			got, fd := stage(t, row, input)
			if fd != nil {
				return nil, toFailure(t, fd, row)
			}
			return got, nil
		},
	}
	var count specCount
	used := map[string]bool{}
	t.Run("spec: "+file, func(t *testing.T) {
		for _, row := range spec.Rows {
			input := row.UnescNamed(inputCol)
			expected := row.Named("expected")
			count.rows++
			t.Run(fmt.Sprintf("row %d: %q", row.Line, input), func(t *testing.T) {
				key := divergenceKey(row, inputCol)
				if why, ok := specDivergences[file][key]; ok {
					used[key] = true
					count.skipped++
					t.Skipf("%s: a runtime divergence: %s", row.Where(), why)
				}
				if err := runner.CheckRow(row, input, expected); err != nil {
					t.Error(err)
					return
				}
				count.passed++
			})
		}
	})
	for key := range specDivergences[file] {
		if !used[key] {
			t.Errorf("%s: the divergence %q names no row; it is stale, remove it", file, key)
		}
	}
	t.Logf("%s: %d rows, %d passed, %d skipped", file, count.rows, count.passed, count.skipped)
	fmt.Printf("spec %s: %d rows, %d passed, %d skipped\n", file, count.rows, count.passed, count.skipped)
}

func TestSpecEvents(t *testing.T) { runSpec(t, "events.tsv", "input", eventsStage) }
func TestSpecRoute(t *testing.T)  { runSpec(t, "route.tsv", "input", routeStage) }
func TestSpecTable(t *testing.T)  { runSpec(t, "table.tsv", "input", tableStage) }
func TestSpecLines(t *testing.T)  { runSpec(t, "lines.tsv", "input", eventsStage) }
func TestSpecScan(t *testing.T)   { runSpec(t, "scan.tsv", "script", scanStage) }
func TestSpecLimits(t *testing.T) { runSpec(t, "limits.tsv", "input", stagedStage) }

// TestEveryFixtureHasARunner fails when a fixture appears without a
// runner, rather than letting it pass unread.
func TestEveryFixtureHasARunner(t *testing.T) {
	specs, err := support.LoadSpecDir(specDir(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	var files []string
	for _, s := range specs {
		files = append(files, s.Name)
	}
	sort.Strings(files)
	if fmt.Sprint(files) != fmt.Sprint(specFixtures) {
		t.Fatalf("fixtures %v, runners %v: each fixture needs a TestSpec runner", files, specFixtures)
	}
}
