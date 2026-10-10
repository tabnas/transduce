/* Copyright (c) 2026 tabnas, MIT License */

// Generated inputs in the spec's worked-example shape, the same documents
// rs/tests/support/mod.rs generates, so the differential suite and the
// retention test run over what the Rust runtime runs.
//
// A record is `{"id":i,"person":{"name":"Person number i"},"account":
// {"balance":D.CC}}`, the JSON document wraps them under
// `response.payload.deep.records` behind `response.metadata.fields`, the
// JSON Lines variant is one record per line, the CSV variant is the
// flattened columns `id,name,balance`, the YAML variant is the block form
// of the JSON document, and the XML variant is the same tree as elements,
// a record per line.

// The metadata the worked example carries: three columns by path.
export const METADATA =
  '{"fields":[{"title":"Identifier","path":["id"]},{"title":"Full name","path":["person","name"]},{"title":"Balance","path":["account","balance"]}]}'

function cents(i: number): string {
  return String(i % 100).padStart(2, '0')
}

// One record, as compact JSON.
export function record(i: number): string {
  return `{"id":${i},"person":{"name":"Person number ${i}"},"account":{"balance":${i * 7}.${cents(i)}}}`
}

// The worked-example document with `records` records.
export function recordsJson(records: number): string {
  const parts: string[] = []
  for (let i = 0; i < records; i++) parts.push(record(i))
  return `{"response":{"metadata":${METADATA},"payload":{"deep":{"records":[${parts.join(',')}]}}}}`
}

// The records alone, one JSON document per line.
export function recordsJsonl(records: number): string {
  let s = ''
  for (let i = 0; i < records; i++) s += record(i) + '\n'
  return s
}

// The records flattened to `id,name,balance`, with a header line.
export function recordsCsv(records: number): string {
  let s = 'id,name,balance\n'
  for (let i = 0; i < records; i++) s += `${i},Person number ${i},${i * 7}.${cents(i)}\n`
  return s
}

// The worked-example document as XML: the metadata's fields as `<field>`
// elements and each record a `<record>` element, its id an attribute and
// its name and balance child elements, one record per line.
export function recordsXml(records: number): string {
  let s =
    '<?xml version="1.0"?>\n<response>\n  <metadata>\n    <fields>\n      <field title="Identifier" path="id"/>\n      <field title="Full name" path="person name"/>\n      <field title="Balance" path="account balance"/>\n    </fields>\n  </metadata>\n  <payload><deep><records>\n'
  for (let i = 0; i < records; i++) {
    s += `    <record id="${i}"><person><name>Person number ${i}</name></person><account><balance>${i * 7}.${cents(i)}</balance></account></record>\n`
  }
  return s + '  </records></deep></payload>\n</response>\n'
}

// The worked-example document as block YAML.
export function recordsYaml(records: number): string {
  let s =
    'response:\n  metadata:\n    fields:\n      - title: Identifier\n        path: [id]\n      - title: Full name\n        path: [person, name]\n      - title: Balance\n        path: [account, balance]\n  payload:\n    deep:\n      records:\n'
  for (let i = 0; i < records; i++) {
    s += `        - id: ${i}\n          person:\n            name: Person number ${i}\n          account:\n            balance: ${i * 7}.${cents(i)}\n`
  }
  return s
}
