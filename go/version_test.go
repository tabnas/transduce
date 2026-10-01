// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

// The baked-in VERSION must equal the crate's declared version. The Rust
// crate is this repository's reference runtime and its rs/Cargo.toml is
// the version every runtime of it releases under; a release that bumps
// one and forgets the other fails here instead of shipping a lie.

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

func TestVersionMatchesCargoToml(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "rs", "Cargo.toml"))
	if err != nil {
		// Deliberately fatal, never skipped: a version check that silently
		// does not run is the failure mode this test exists to prevent.
		t.Fatalf("cannot read rs/Cargo.toml, so VERSION cannot be checked: %v", err)
	}
	// The first `version` in the [package] table.
	pkg := regexp.MustCompile(`(?s)\[package\](.*?)(\n\[|$)`).FindSubmatch(raw)
	if pkg == nil {
		t.Fatal("rs/Cargo.toml has no [package] table")
	}
	m := regexp.MustCompile(`(?m)^version\s*=\s*"([^"]+)"`).FindSubmatch(pkg[1])
	if m == nil {
		t.Fatal("rs/Cargo.toml's [package] has no version")
	}
	if VERSION != string(m[1]) {
		t.Errorf("VERSION drift: go VERSION = %q but rs/Cargo.toml = %q", VERSION, m[1])
	}
}
