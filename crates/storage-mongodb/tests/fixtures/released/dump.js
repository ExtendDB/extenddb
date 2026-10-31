// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//
// Print both ExtendDB databases as one fixture, in canonical Extended JSON:
//
//   mongosh --quiet --file dump.js
//
// For each database: every collection with its type and options, its
// indexes as listIndexes returns them (key order kept, `v` and `ns`
// dropped), and every document. Keys keep their stored order, which
// compound `_id` lookups depend on. `load.js` loads the file back.
const out = { databases: {} };
for (const name of ["extenddb_catalog", "extenddb_data"]) {
  const d = db.getSiblingDB(name);
  const collections = [];
  for (const spec of d.getCollectionInfos({}, { nameOnly: false })) {
    if (spec.type !== "collection") {
      continue;
    }
    const coll = d.getCollection(spec.name);
    const indexes = coll.getIndexes().map((i) => {
      const copy = Object.assign({}, i);
      delete copy.v;
      delete copy.ns;
      return copy;
    });
    collections.push({
      name: spec.name,
      options: spec.options,
      indexes: indexes,
      documents: coll.find({}).sort({ _id: 1 }).toArray(),
    });
  }
  collections.sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
  out.databases[name] = { collections: collections };
}
print(EJSON.stringify(out, null, 1, { relaxed: false }));
