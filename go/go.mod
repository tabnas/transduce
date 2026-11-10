module github.com/tabnas/transduce/go

go 1.24.7

// The engine, and jsonic, csv and json, which the tests read with (the
// line sources parsed with csv and json until step 12 of alchemy's
// streaming plan retired them).
require (
	github.com/tabnas/csv/go v0.6.10
	github.com/tabnas/json/go v0.5.20
	github.com/tabnas/jsonic/go v0.7.11
	github.com/tabnas/parser/go v0.12.16
)

// The protocol types, declared in alchemy's shared package, which alchemy
// carries from v0.2.0.
require github.com/tabnas/alchemy/go v0.4.0

// The other grammars the tests run, and the shared fixture runner.
require (
	github.com/tabnas/feed/go v0.6.21
	github.com/tabnas/ini/go v0.5.22
	github.com/tabnas/json5/go v0.5.18
	github.com/tabnas/jsonc/go v0.5.17
	github.com/tabnas/jsonl/go v0.1.20
	github.com/tabnas/markdown/go v0.7.14
	github.com/tabnas/support/go v0.3.9
	github.com/tabnas/toml/go v0.5.21
	github.com/tabnas/xml/go v0.7.20
	github.com/tabnas/yaml/go v0.5.31
	github.com/tabnas/zon/go v0.5.19
)

require github.com/tabnas/hoover/go v0.3.15 // indirect
