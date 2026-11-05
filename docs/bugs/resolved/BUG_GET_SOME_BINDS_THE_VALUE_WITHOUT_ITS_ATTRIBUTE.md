# get-some binds the value without its attribute

**Status**: Resolved, as *The fix* states. `TestGetSome_BindsTheAttributeItFoundWithTheValue` (storage), `TestGetSomeWithTwoAttrs` and `TestIntegration_GetSome` (qb) pin it.

Datomic's `get-some` answers the first attribute the entity carries together with its value. Janus's answers the value alone. Goal 12 of `docs/proposals/NESTED_SCOPES_AS_QUERIES.md` says a form Datomic gives takes Datomic's meaning, so a janus meaning for this form is a gap.

## Datomic's form

Datomic's query reference: "The get-some function takes a database, an entity identifier, and one or more cardinality-one attributes, returning a tuple of the entity id and value for the first attribute possessed by the entity." Its signature is `[(get-some src-var ent attr+) [?attr ?val]]`, and its example is `[(get-some $ ?e :country/name :artist/name) [?attr ?name]]`.

## What the language does

- `GetSomeFunction` (`datalog/query/database_function.go`) documents Datomic's form, `[(get-some $ ?entity :attr1 :attr2 :attr3) [?found-attr ?value]]`.
- `GetSomeFunction.EvalWithLookup` returns a `GetSomeResult` holding the attribute it found, as a `datalog.Keyword`, and its value.
- `admitExpressionResult` (`datalog/executor/iterator_composition.go`) binds `gsr.Value` alone, so the attribute never reaches the query.
- So `[(get-some $ ?e :a :b) ?v]` binds the value, and Datomic's `[(get-some $ ?e :a :b) [?attr ?v]]` destructures that value.
- The documentation shows the one-symbol form as Datomic's:
  - `DATOMIC_COMPATIBILITY.md` marks `get-some` compatible, with the example `[(get-some $ ?e :user/nickname :user/fullname :user/email) ?display-name]`.
  - `README.md` shows the same form.
  - `docs/reference/QUERY_BUILDER.md` maps `qb.GetSome(e, a1, a2, a3).As(r)` to `[(get-some $ ?e :a1 :a2 :a3) ?r]`.

## The fix

- `admitExpressionResult` admits the pair, the attribute and its value, as the function's value. So `[?attr ?v]` binds both, under Datomic's destructuring.
- The attribute is its keyword, where Datomic answers the attribute's entity id, since janus names an attribute by its keyword.
- `qb.GetSome`'s `As` names two symbols, the attribute's and the value's.
- `DATOMIC_COMPATIBILITY.md`, `README.md` and `docs/reference/QUERY_BUILDER.md` show the two-symbol form.

## What the fix costs

A query written today binds the value at one symbol. Under the fix the function's value is the pair, so that symbol binds the pair. A query that reads it as the value answers differently, with no error. Every caller of `get-some` changes its binding, in this repository and downstream. The tests that write the one-symbol form change with it: `datalog/storage/get_some_input_test.go`, `datalog/storage/database_function_integration_test.go`, `datalog/parser/database_function_test.go` and `datalog/qb/integration_test.go`, and every other test that writes `get-some`.

## Tests

- `[(get-some $ ?e :a :b) [?attr ?v]]` binds `:a` and its value on an entity carrying both attributes, and `:b` and its value on one carrying `:b` alone.
- An entity carrying neither attribute is dropped, as today.
- A recorded `""` at `:a` binds `:a` and `""`, since existence is the datom's.
- `qb.GetSome(e, a, b).As(attr, v)` builds the same query as the EDN form.
