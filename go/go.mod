module github.com/tabnas/transduce/go

go 1.24.7

// The engine, and jsonic, csv and json, which the tests read with (the
// line sources parsed with csv and json until step 12 of alchemy's
// streaming plan retired them).
require (
	github.com/tabnas/csv/go v0.6.10
	github.com/tabnas/json/go v0.5.20
	github.com/tabnas/jsonic/go v0.7.11
	github.com/tabnas/parser/go v0.12.20
)

// The protocol types, declared in alchemy's shared package, which alchemy
// carries from v0.2.0.
require github.com/tabnas/alchemy/go v0.4.0

// The other grammars the tests run, and the shared fixture runner.
require (
	github.com/tabnas/feed/go v0.6.21
	github.com/tabnas/ini/go v0.5.23
	github.com/tabnas/json5/go v0.5.19
	github.com/tabnas/jsonc/go v0.5.17
	github.com/tabnas/jsonl/go v0.1.20
	github.com/tabnas/markdown/go v0.7.15
	github.com/tabnas/support/go v0.3.9
	github.com/tabnas/toml/go v0.5.22
	github.com/tabnas/xml/go v0.7.20
	github.com/tabnas/yaml/go v0.5.32
	github.com/tabnas/zon/go v0.5.19
)

// css, the grammar notations abnf, ebnf and gbnf, and chess (the pgn
// format), which the prune suites run so that the verified lists can name
// them, taken on the maintainer's instruction of 2026-11-11 and imported by
// the tests alone. abnf, ebnf and gbnf take bnf in turn.
require (
	github.com/tabnas/abnf/go v0.5.1
	github.com/tabnas/chess/go v0.1.19
	github.com/tabnas/css/go v0.5.19
	github.com/tabnas/ebnf/go v0.2.2
	github.com/tabnas/gbnf/go v0.2.1
)

require (
	github.com/tabnas/bnf/go v0.1.33 // indirect
	github.com/tabnas/hoover/go v0.3.15 // indirect
)
