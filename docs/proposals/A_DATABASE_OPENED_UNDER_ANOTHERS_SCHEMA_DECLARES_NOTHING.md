# A database opened under another's schema declares nothing

## 1. Goals

1. A database can be opened under the schema another database declares, and runs with those definitions: value types, cardinalities, uniqueness, and the refusal of an unknown attribute where the opener asks for it.
2. The open writes no datom. Its cost is that of an open under no schema, whatever the number of attributes.
3. The definitions are the ones the other database declares at the open, and they stay. A later declaration on the other database does not reach it.
4. The other database may be closed first.

## 2. Tests

The tests are in `datalog/storage/open_under_anothers_schema_declares_nothing_test.go`, each over every backend it applies to.

- A database opened under another's schema holds no datom after the open, and its store states no definition after it has been written to. Its schema answers each definition the other declares, one of each cardinality and each typed. A transaction on it writes under them: two `Add`s at a vector attribute resolve to a vector of two, two at a set attribute to a set of two, and a value of another type than the declared one is refused. An attribute the other database carries datoms at and never declared has no definition in it, and a `SetSchema` on the other database after the open changes nothing in it. `TestADatabaseOpenedUnderAnothersSchemaDeclaresNothing`. Under an open that is given the other's definitions as a schema, the store holds 19 datoms for three attributes after the open.
- The other database is closed, and the database opened under its schema answers the definitions and writes under them. `TestADatabaseOpenedUnderAnothersSchemaOutlivesTheOther`.
- A schema given beside another database's is refused, and a refusing database opened under another's schema admits the attributes the other declares, refuses any other, and takes `SetSchema(nil)`. `TestADatabaseOpenedUnderAnothersSchemaTakesNoSchemaBesideIt`.
- A dump of it, text and binary, loaded into a database opened under no schema, runs with the definitions it ran under and holds what it wrote. After a `SetSchema` on it that names an ident the open took, it and its dump run with the given definition for that ident and the open's for every other, the other database's own definition is unchanged, and `SetSchema(nil)` withdraws neither. `TestADumpOfADatabaseOpenedUnderAnothersSchemaDescribesItself`.
- An import into it of a dump that declares an ident the open took: the database runs with the dump's definition for that ident. `TestADumpImportedIntoADatabaseOpenedUnderAnothersSchemaSupersedesIt`.
- A branch forked from it runs under the same definitions, holds what it held, and its store states no definition either. `TestABranchOfADatabaseOpenedUnderAnothersSchemaDeclaresNothing`.
- An open under another's schema costs what an open under no schema costs. `BenchmarkOpenUnderAnothersSchema`, on the memory-tree store under a schema of 256 attributes: 20.6 µs under no schema, 4.83 ms under the schema given, 15.4 µs under another database's.

## 3. What exists

- A schema given to a database is asserted into its store (`docs/reference/SCHEMA.md`, "Schema as Facts"). `assertSchemaFacts` declares each given definition the store does not hold word for word, and the database runs with the definitions the store then states. The running schema has one home, the store.
- On an empty store the open therefore writes every declaration. Measured from a consumer on the memory-tree store, 200 opens each: 10.5 ms under a schema of 357 attributes, 3.4 ms under 74, 0.014 ms under no schema. That is about 40 µs a declared attribute.
- A dump of a store opened under the 357 attributes, restored by `ImportBinary` into a database opened under no schema, takes 11.6 to 13 ms. The import asserts the same declarations.
- Once a database is open its schema costs nothing that measurement shows. On the memory-tree store, 200,000 each: a transaction of one `Set` and its `Commit` takes 44.0 µs under the 357 attributes and 43.6 µs under none, and `ResolveEntityAttributes` of one attribute 2.5 µs and 2.6 µs.
- A database passed as a named source answers patterns under its own schema (`docs/reference/MULTI_SOURCE.md`). Nothing of a schema is shared between sources.
- `Fork` opens its branch under the parent's given schema and asserts the parent's declared definitions into the branch, so that a dump of the branch describes itself.

## 4. The open

- A database opened under another's schema runs with the definitions the other database's store states at that moment: its declared definitions, the set `declaredSchema` answers.
- It does not take the other's effective schema. That schema also holds the definitions the other's ops inferred, at its last install, for attributes no declaration covers, and those describe the other's datoms.
- Its own effective schema is composed as any database's is: the definitions it was opened under, the engine's words, and what the ops of its own store infer for an attribute no definition covers.
- The open asserts nothing, reads its own store's schema facts as any open does, and installs once. On an empty store the inference reads no datom.
- What a database lends is read from its store by the first open under it and kept with the schema it has installed, the engine's words composed over it. A database whose own store states and infers nothing installs that composition as it is, so its open costs nothing an attribute.
- The definitions stay as they were at the open. The database holds the declarations it took and the composed schema value, both immutable, and reads nothing of the other database afterwards. So the other's `SetSchema`, import or rewind reaches no cache of this database, and the other may be closed while this one lives.
- The alternative is a live schema, the database following the other's declarations. Each install on the other would then drop this database's cached views of the attributes it changed, and a closed database would leave its followers with none.
- A second alternative is the open as it is, under the other's schema given again. It writes the declarations at each open, which is the cost in §3.
- The refusal of unknown attributes stays the opener's option. Under it the database admits the definitions it was opened under and the engine's words.

## 5. What the database holds, and what it does not

- A definition has one place: the datoms of its declaration entity and the ref its attribute's entity holds to it (`assertSchemaFacts`). The open takes the other database's declarations as those datoms, as the other's store states them at that moment, and holds them beside its own store without writing them. In every respect but being written they are definitions its store states.
- Its store holds the datoms written to it and no declaration of the ones it took. This is the one place the rule of §3 gives way.
- The schema is composed as any database's is, from the declarations it took together with those its own store states. A definition its own store states supersedes the one it took for the same ident, whole.
- A dump of it describes itself: `Export` and `ExportBinary` write the declarations it took at their entities, among the store's own datoms in the dump's order, each declaration that its own store does not supersede. A superseded one is left out, so the dump states one current definition an ident and its reader needs no order between the two databases' ElementIDs.
- A query of the schema's facts on it (`[?a :db.attribute/declaration ?d]`) reads its own store, and so finds the definitions it declared itself and none it took.
- `SetSchema` on it is allowed. It declares into the store as on any database. `SetSchema(nil)` withdraws the given schema as on any database, and the declarations the open took stay: they are the store's, not the given schema's.
- An import into it of a dump that carries declarations: the dump's declarations are stored, and they supersede what the open took.
- A branch forked from it runs under the same definitions, and holds the declarations the open took as the database does.
- A persistent store written so and opened again later is opened as any store is, under the schema given then or under none.
- The surface: an option of `DatabaseOptions` naming the other database, exclusive with `Schema`.

## 6. The uses

- One long-lived database and many short-lived ones beside it, each a source in the same queries: a store for each speculative run, holding only what the run wrote and read over the long-lived one. A consumer opens thousands of them an hour and writes each a few hundred datoms.
- Each holds the same entities under the same attributes as the long-lived database. Under its schema they take its vectors and sets as such, check each written value's type, and refuse an attribute it does not know.
- With the open as it is, the consumer either pays the declarations at each open or opens each store under the few definitions that are not one value an entity, and gives up the type check and the refusal.
