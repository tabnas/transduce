module github.com/tabnas/transduce/go

go 1.24.7

// The engine, jsonic, and the grammars the line sources parse with.
require (
	github.com/tabnas/csv/go v0.6.3
	github.com/tabnas/json/go v0.5.14
	github.com/tabnas/jsonic/go v0.7.5
	github.com/tabnas/parser/go v0.12.10
)

// The protocol types, declared in alchemy's shared package, which alchemy
// carries from v0.2.0.
require github.com/tabnas/alchemy/go v0.2.1

// The grammars the tests run (the differential suite reads this list), and
// the shared fixture runner.
require (
	github.com/tabnas/feed/go v0.6.13
	github.com/tabnas/ini/go v0.5.15
	github.com/tabnas/json5/go v0.5.12
	github.com/tabnas/jsonc/go v0.5.11
	github.com/tabnas/jsonl/go v0.1.13
	github.com/tabnas/markdown/go v0.7.9
	github.com/tabnas/support/go v0.3.7
	github.com/tabnas/toml/go v0.5.13
	github.com/tabnas/xml/go v0.7.13
	github.com/tabnas/yaml/go v0.5.20
	github.com/tabnas/zon/go v0.5.13
)

require github.com/tabnas/hoover/go v0.3.12 // indirect
