// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

// Generated inputs in the spec's worked-example shape (rs/tests/support),
// so every runtime measures the same documents.

import (
	"fmt"
	"strings"
)

// workedMetadata is the metadata the worked example carries: three
// columns by path.
const workedMetadata = `{"fields":[{"title":"Identifier","path":["id"]},{"title":"Full name","path":["person","name"]},{"title":"Balance","path":["account","balance"]}]}`

// workedRecord is one record, as compact JSON.
func workedRecord(i int) string {
	return fmt.Sprintf(`{"id":%d,"person":{"name":"Person number %d"},"account":{"balance":%d.%02d}}`,
		i, i, i*7, i%100)
}

// recordsJSON is the worked-example document with n records.
func recordsJSON(n int) string {
	var b strings.Builder
	b.WriteString(`{"response":{"metadata":` + workedMetadata + `,"payload":{"deep":{"records":[`)
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(workedRecord(i))
	}
	b.WriteString("]}}}}")
	return b.String()
}

// recordsJSONL is the records alone, one JSON document per line.
func recordsJSONL(n int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		b.WriteString(workedRecord(i))
		b.WriteByte('\n')
	}
	return b.String()
}

// recordsCSV is the records flattened to id,name,balance, with a header.
func recordsCSV(n int) string {
	var b strings.Builder
	b.WriteString("id,name,balance\n")
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "%d,Person number %d,%d.%02d\n", i, i, i*7, i%100)
	}
	return b.String()
}

// recordsYAML is the worked-example document as block YAML.
func recordsYAML(n int) string {
	var b strings.Builder
	b.WriteString("response:\n  metadata:\n    fields:\n      - title: Identifier\n        path: [id]\n" +
		"      - title: Full name\n        path: [person, name]\n      - title: Balance\n" +
		"        path: [account, balance]\n  payload:\n    deep:\n      records:\n")
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "        - id: %d\n          person:\n            name: Person number %d\n"+
			"          account:\n            balance: %d.%02d\n", i, i, i*7, i%100)
	}
	return b.String()
}
