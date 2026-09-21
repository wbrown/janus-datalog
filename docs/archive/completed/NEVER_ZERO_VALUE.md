# NeverZeroValue — implementation record

**Status:** complete. `go.mod` pins `go 1.26.8`: the js/wasm storage tests trip the Go 1.26.3 runtime GC bug "found bad pointer in Go heap" (https://github.com/golang/go/issues/80472), fixed in that patch release.

## Closed design

In this store absence is no datom. For a text attribute there is no fact "the bio is the empty string": an entity carries a bio or it does not. Which attributes exclude their type's zero is a fact about each attribute, declared once, on the attribute, and enforced by the store at write; every struct that saves the attribute then honors it with a plain Go field, and every reader that tests a plain field against its zero is exact.

Per-attribute domain constraint: the Go zero of the attribute’s `ValueType` is not a value. Optional, additive, no Go-type inference.

- `AttributeDefinition.NeverZeroValue bool`; builder `NeverZeroValue()`; EDN `:db/neverZeroValue true`.
- `ValueType` is required when the flag is set (ParseSchema error, builder error, `Schema.Add` panic).
- `Add` / `Set` / `AddEntity` / `AddMap` reject the zero via `ValidateDatom`. The struct writer, on both `SaveStruct` and `StructWriter.Write` / `WriteAuto`, omits a zero-valued scalar field (same as a nil pointer).
- On many/vector the property applies per element. A non-empty slice that contains a zero element is rejected, not filtered. Nil vs empty slice on `SaveStruct` is unchanged.
- `Remove` is unconstrained (imported zeros can be tombstoned).
- No store normalization. Reads unchanged. Import still goes through `store.Assert`.
- `:db/unique-elements` → `:db/uniqueElements`. No alias, no migration.
- Unknown keys in an attribute definition map are parse errors (so the old kebab name fails closed).

Zero of each value type: `""`, `0`, `0.0`, `false`, zero `time.Time`, nil or empty `[]byte`, nil ref/keyword/symbol, zero `ElementID`. Whitespace is a value.

## Tests

| file | what it pins |
|---|---|
| `datalog/schema/never_zero_value_test.go` | `IsZeroValue` table; `ValidateDatom` reject/accept; builder + parse; parse/builder parity; `ValueType` required (builder, parse, `Schema.Add` panic); unknown schema key; kebab `:db/unique-elements` is unknown; camel `:db/uniqueElements` |
| `datalog/storage/never_zero_value_test.go` | `Add`/`Set` reject zero and buffer nothing, all declared types; whitespace is a value; `Remove("")` after import; import of a `""` datom into a declared schema |
| `datalog/reflect/never_zero_value_test.go` | `SchemaFromStruct` does not infer the flag; `SaveStruct` omits a zero field and leaves a prior value; `PullInto` absence is the zero; non-empty slice with a zero element is an error |

## Production

1. `datalog/schema/types.go` — `NeverZeroValue` on `AttributeDefinition`; `Schema.Add` panics when the flag is set with no `ValueType`.
2. `datalog/schema/builder.go` — `NeverZeroValue()`; `Add` records the missing `ValueType` as a build error.
3. `datalog/schema/parser.go` — `:db/neverZeroValue`; `:db/uniqueElements`; a non-keyword key or a key outside the six is a parse error; `:db/neverZeroValue` without `:db/valueType` is a parse error.
4. `datalog/schema/validation.go` — `IsZeroValue`; `ValidateDatom` rejects the zero after the type check. `IsZeroValue` panics on a keyword outside the value-type vocabulary; the arm is unreachable through a schema.
5. `datalog/reflect/writer.go` — `zeroOfNeverZeroValue`; `writeField` (`StructWriter.Write` / `WriteAuto`) and `updateField` (`SaveStruct`) write nothing for a zero-valued scalar field on a declared attribute. Slice and `OrderedSet` writers are unchanged: a zero element reaches `Add`/`Set` and is rejected there.
6. `datalog/schema/parser_test.go` — `TestParseSchema_UniqueElements` on `:db/uniqueElements`.
7. `docs/reference/SCHEMA.md` — `:db/neverZeroValue`, Zero-Value Rejection, EDN `:db/uniqueElements`, unknown keys are errors, `IsZeroValue` in the package reference.
8. `DATOMIC_COMPATIBILITY.md` — janus extensions list; unknown definition keys are errors; uniqueness described as read-time resolution; `SaveStruct` in place of the never-implemented `AddStruct` / `AddStructAuto`; `Remove` for every cardinality; symbol and tx in the value-type list.
9. `docs/reference/REFLECT.md` — Zero-Valued Fields; `NewStructWriter`, `Write`, `WriteAuto` in the package reference.
10. `go.mod` — `go 1.26.8`.

## Open

- `docs/bugs/resolved/EXTERNAL_REVIEW_2026_04.md` records the kebab `:db/unique-elements` as what was added then; history.
- `TestParseSchema_UniqueElements` and `TestParseSchemaUniqueElementsCamelCase` pin the same fact.
