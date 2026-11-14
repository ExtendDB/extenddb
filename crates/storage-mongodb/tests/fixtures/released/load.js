// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//
// Load a fixture `dump.js` printed into empty databases:
//
//   mongosh --quiet --eval 'const FIXTURE = "<path>"' --file load.js
//
// Each collection is created with its options, its indexes are created as
// listed (the `_id_` index exists with the collection), and its documents
// are inserted in order. The databases must not exist.
const fixture = EJSON.parse(fs.readFileSync(FIXTURE, "utf8"), { relaxed: false });
for (const [name, database] of Object.entries(fixture.databases)) {
  const d = db.getSiblingDB(name);
  if (d.getCollectionNames().length !== 0) {
    throw new Error(`database ${name} is not empty`);
  }
  for (const c of database.collections) {
    d.createCollection(c.name, c.options);
    const indexes = c.indexes.filter((i) => i.name !== "_id_");
    if (indexes.length > 0) {
      d.runCommand({ createIndexes: c.name, indexes: indexes });
    }
    if (c.documents.length > 0) {
      d.getCollection(c.name).insertMany(c.documents, { ordered: true });
    }
  }
}
print("loaded");
