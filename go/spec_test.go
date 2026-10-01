// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

// The shared fixtures in ../test/spec, run through tabnas-support's
// Runner as every tabnas repository runs its fixtures; the harness is
// harness_test.go. Every row runs, except, in a build without the
// rule-event adapter, the rows that need it (needsAdapter), which are
// skipped by name; and the rows specDivergences lists, each a measured
// difference between this runtime and Rust that DIVERGENCE.md records.

import (
	"fmt"
	"path/filepath"
	"sort"
	"testing"

	support "github.com/tabnas/support/go"
)

// specDivergences is the rows this runtime does not reproduce, by file
// and then by grammar, mode and input (the input column as written in
// the fixture, tab-separated), and why. Each is a difference in a Go
// grammar module, measured against Rust, that DIVERGENCE.md records; none
// is this package's.
var specDivergences = map[string]map[string]string{
	"events.tsv": {
		"yaml\tmaterialize\tbase: &b\\n  x: 1\\nd:\\n  <<: *b\\n  y: 2\\n": "tabnas-yaml's Go grammar " +
			"resolves a `<<` merge key with the merged members FIRST ({x:1, y:2}); Rust's puts them " +
			"after the mapping's own ({y:2, x:1})",
	},
	"lines.tsv": {
		"csv\tlines\ta,b\\n1,2\\n3,\"x\\n4,5\\n": "tabnas-csv's Go grammar reports an " +
			"unterminated quoted field at the row where the source ENDS and the field's column " +
			"(5:3 for the whole file); Rust reports the field's own row and column (3:3)",
	},
}

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
	t.Run("spec: "+file, func(t *testing.T) {
		for _, row := range spec.Rows {
			input := row.UnescNamed(inputCol)
			expected := row.Named("expected")
			count.rows++
			t.Run(fmt.Sprintf("row %d: %q", row.Line, input), func(t *testing.T) {
				if !AdapterBuilt() && needsAdapter(row) {
					count.skipped++
					t.Skipf("%s: %s %s needs the rule-event adapter (build tag tabnas_nodecell)",
						row.Where(), row.Named("grammar"), row.Named("mode"))
				}
				if why, ok := specDivergences[file][divergenceKey(row, inputCol)]; ok {
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
	t.Logf("%s: %d rows, %d passed, %d skipped (adapter built: %v)",
		file, count.rows, count.passed, count.skipped, AdapterBuilt())
	fmt.Printf("spec %s: %d rows, %d passed, %d skipped (adapter built: %v)\n",
		file, count.rows, count.passed, count.skipped, AdapterBuilt())
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
