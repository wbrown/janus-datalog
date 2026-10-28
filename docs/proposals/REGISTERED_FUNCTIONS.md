# Proposal: Registered Functions

## Summary

Allow Go functions to be registered and called from within Datalog queries. This enables custom logic (game mechanics, business rules, domain computations) to be invoked declaratively while keeping the relational query model intact.

## Motivation

### The Gap Between Query and Logic

Datalog excels at relational queries - finding, filtering, joining data. But real applications need domain-specific computations:

- **Game mechanics**: Damage calculations, AI decisions, physics
- **Business rules**: Pricing logic, eligibility checks, compliance rules
- **Domain transforms**: Code parsing, coordinate conversion, name resolution

Currently, these require post-query processing in Go:

```go
// Query gets raw data
results, _ := db.ExecuteQuery(`
    [:find ?char ?base-dmg ?resistance
     :where [?char :character/location ?region]
            [?region :region/hazard ?hazard]
            [?hazard :hazard/damage ?base-dmg]
            [?char :character/resistance :fire ?resistance]]
`)

// Go computes derived values
for _, r := range results {
    char := r[0].(Identity)
    baseDmg := r[1].(float64)
    resist := r[2].(float64)

    // Domain logic lives here, outside the query
    actualDmg := computeFireDamage(baseDmg, resist)

    // Must manually correlate back
    applyDamage(char, actualDmg)
}
```

With registered functions:

```clojure
[:find ?char ?actual-dmg
 :where [?char :character/location ?region]
        [?region :region/hazard ?hazard]
        [?hazard :hazard/damage ?base-dmg]
        [?char :character/resistance :fire ?resist]
        [(compute-fire-damage ?base-dmg ?resist) ?actual-dmg]]
```

The computation happens _inside_ the query. Results are already correlated. The query remains declarative.

### Use Cases

**Game Rules Engine:**

```clojure
;; NPC behavior selection
[:find ?npc ?action
 :where [?npc :npc/state ?state]
        [?npc :npc/target ?target]
        [?target :character/health ?hp]
        [(select-npc-action ?state ?hp) ?action]]

;; Combat resolution
[:find ?attacker ?target ?damage
 :where [?attacker :action/attack ?target]
        [?attacker :character/weapon ?w]
        [?w :weapon/stats ?stats]
        [?target :character/armor ?armor]
        [(resolve-attack ?stats ?armor) ?damage]]
```

**Dependency Resolution:**

```clojure
[:find ?key ?subject
 :in $db $rules ?target
 :where [$rules ?r :rule/key ?target]
        [$rules ?r :rule/subject-expr ?expr]
        [$db ?e :entity/type :region]
        [(resolve-subject-expr $db ?expr ?e) ?subject]]
```

**Data Transformation:**

```clojure
;; Parse structured codes
[:find ?region ?x ?y
 :where [?region :region/code ?code]
        [(parse-grid-code ?code) ?x ?y]]

;; Normalize names
[:find ?e ?normalized
 :where [?e :entity/name ?raw]
        [(normalize-name ?raw) ?normalized]]
```

## Design

### Function Types

**Predicates** - Return `bool`, filter tuples (no output binding):

```go
db.RegisterPredicate("valid-target?", func(entity Identity) bool {
    return isValidTarget(entity)
})
```

```clojure
[:find ?target
 :where [?target :character/type :enemy]
        [(valid-target? ?target)]]  ;; Filters, no binding
```

**Functions** - Return value(s), bind to output variable(s):

```go
db.RegisterFunction("compute-damage", func(base float64, resist float64) float64 {
    return math.Max(0, base * (1 - resist))
})
```

```clojure
[:find ?char ?dmg
 :where [?char :character/health ?hp]
        [?hazard :hazard/damage ?base]
        [?char :character/resistance ?resist]
        [(compute-damage ?base ?resist) ?dmg]]  ;; Binds ?dmg
```

**Multi-return Functions** - Bind multiple output variables:

```go
db.RegisterFunction("parse-coord", func(code string) (int64, int64) {
    x, y := parseGridCode(code)
    return x, y
})
```

```clojure
[:find ?region ?x ?y
 :where [?region :region/code ?code]
        [(parse-coord ?code) ?x ?y]]  ;; Binds both ?x and ?y
```

### Registration API

```go
// Simple predicate (reflection infers signature)
db.RegisterPredicate("valid?", func(e Identity) bool {
    return validate(e)
})

// Simple function (reflection infers signature)
db.RegisterFunction("compute", func(a, b float64) float64 {
    return a * b
})

// With explicit options; the source it reads arrives as an argument
db.RegisterFunction("resolve-subject",
    func(source Source, expr Keyword, base Identity) Identity {
        return resolveSubjectExpr(source, expr, base)
    },
    datalog.WithName("resolve-subject-expr"),  // Override name
    datalog.WithDescription("Resolves a subject expression against a base entity"),
)

// Namespaced (prevents collisions)
db.RegisterFunction("myapp/compute", computeFn)
```

### Type Mapping

Registered functions use the same type coercion as `QueryInto`:

| Go Type                 | Datalog Value | Notes                      |
| ----------------------- | ------------- | -------------------------- |
| `int64`, `int`, `int32` | Integer       | Numeric coercion           |
| `float64`, `float32`    | Float         | Numeric coercion           |
| `string`                | String        | Direct                     |
| `bool`                  | Boolean       | Direct                     |
| `time.Time`             | Instant       | Direct                     |
| `datalog.Identity`      | Entity ID     | For entity references      |
| `datalog.Keyword`       | Keyword       | For attribute names, enums |
| `[]byte`                | Bytes         | Direct                     |
| `Source`                | A source      | A source variable argument |

**Entity returns are joinable:**

```clojure
[(resolve-subject $db ?expr ?base ?prop) ?subject]  ;; ?subject is Identity
[?subject :entity/name ?name]                        ;; Can join on it
```

### Reflection-Based Registration

Following the `QueryInto` pattern, registration uses reflection. A function
set is the one registration path; a database holds one, and a query-scoped
set is passed as an input:

```go
func (s *FunctionSet) Register(name string, fn any, opts ...Option) error {
    fnType := reflect.TypeOf(fn)

    // Validate: must be a function
    if fnType.Kind() != reflect.Func {
        return errors.New("expected function")
    }

    // Extract input types
    numIn := fnType.NumIn()
    argTypes := make([]reflect.Type, numIn)
    for i := 0; i < numIn; i++ {
        argTypes[i] = fnType.In(i)
    }

    // Extract output types
    numOut := fnType.NumOut()
    returnTypes := make([]reflect.Type, numOut)
    for i := 0; i < numOut; i++ {
        returnTypes[i] = fnType.Out(i)
    }

    // Build wrapper that:
    // 1. Coerces Datalog values → Go types
    // 2. Calls function via reflection
    // 3. Coerces Go return → Datalog values
    wrapper := buildWrapper(fn, argTypes, returnTypes)

    // Register in this set
    s.functions[name] = wrapper
    return nil
}

func (d *Database) RegisterFunction(name string, fn any, opts ...Option) error {
    return d.functions.Register(name, fn, opts...)
}
```

### Execution Semantics

**Pure in their arguments.** A registered function is a function clause as
Datomic defines one:

- Receives values, returns values
- Reads data only through a source passed to it as an argument, as `$` is
  passed in `[(datomic.api/entity $ ?e) ?ent]`; a source variable of the
  query, `$` or a named `$source`, is a legal argument
- Performs no side effects
- Is deterministic in its arguments, since the engine may call it any number
  of times in any order

Data a pattern can bind arrives as a value; a function is handed a source
when it must read what no pattern of the query binds:

```clojure
;; Data via patterns, transform via function
[:find ?result
 :in $db
 :where [$db ?e :entity/value ?val]
        [$db ?e :entity/factor ?factor]
        [(compute ?val ?factor) ?result]]

;; A function reads a source it is handed
[(resolve-subject $db ?expr ?base ?prop) ?subject]

;; WRONG: a function that reads a source it was not handed
[(query-and-compute ?e) ?result]  ;; reads $db without being passed it
```

**Error handling:**

```go
// Functions can return error as last return value
db.RegisterFunction("safe-divide", func(a, b float64) (float64, error) {
    if b == 0 {
        return 0, errors.New("division by zero")
    }
    return a / b, nil
})
```

Behavior options:

- **Fail mode** (default): an error from the function is the query's error.
  The iterator-error contract (`TODO.md`, "Iterator-Error Propagation
  Contract") has every error reach the result, and a dropped tuple would be
  that error turned into a smaller clean answer.
- **Filter mode** (opt-in): an error drops the tuple, as a predicate
  returning false does.

```go
db.RegisterFunction("try-parse", parseFn, datalog.OnErrorFilter())
```

### Query Syntax

Functions use the existing expression syntax `[(fn args...) outputs...]`:

```clojure
;; Predicate (no output)
[(valid? ?e)]

;; Single output
[(compute ?a ?b) ?result]

;; Multiple outputs
[(parse ?code) ?x ?y]

;; Chained
[(step1 ?input) ?mid]
[(step2 ?mid) ?output]

;; Mixed with patterns
[?e :entity/value ?val]
[(transform ?val) ?transformed]
[?other :entity/value ?transformed]  ;; Join on transformed value
```

### Registry and Scoping

A function lives on the database, or on a set passed with the query. There
is no package-level registry: the engine's settings live on the database or
the executor (`docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md` §4.2), and a
function is one.

```go
// Database-scoped: every query on db sees it
db.RegisterFunction("compute", computeFn)

// Query-scoped (passed as input)
funcs := datalog.NewFunctionSet()
funcs.Register("compute", computeFn)

db.ExecuteQueryWithInputs(
    `[:find ?r :in $ %funcs :where [(compute ?x) ?r]]`,
    funcs,
)
```

## Examples

### Example 1: Game Damage Calculation

```go
// Register game functions
db.RegisterFunction("compute-damage", func(base, resist, armor float64) float64 {
    mitigated := base * (1 - resist)
    return math.Max(0, mitigated - armor)
})

db.RegisterPredicate("is-alive?", func(hp float64) bool {
    return hp > 0
})
```

```clojure
[:find ?char ?final-damage
 :where [?char :character/health ?hp]
        [(is-alive? ?hp)]
        [?char :character/location ?loc]
        [?loc :location/hazard ?hazard]
        [?hazard :hazard/damage ?base]
        [?hazard :hazard/type ?type]
        [?char :character/resistance ?type ?resist]
        [?char :character/armor ?armor]
        [(compute-damage ?base ?resist ?armor) ?final-damage]]
```

### Example 2: Subject Expression Resolution

The world entity and the property are read through the source the function
is handed:

```go
db.RegisterFunction("resolve-subject",
    func(source Source, expr Keyword, base Identity, prop Keyword) Identity {
        switch expr {
        case Keyword(":$self"):
            return base
        case Keyword(":$world"):
            return getWorldEntity(source, base)
        default:
            if strings.HasPrefix(string(expr), ":$.") {
                // Property lookup
                return getProperty(source, base, prop)
            }
            return base
        }
    })
```

```clojure
[:find ?dep-key ?subject
 :in $db $rules ?target
 :where [$rules ?r :rule/key ?target]
        [$rules ?r :rule/depends-on ?dep]
        [$dep :dep/key ?dep-key]
        [$dep :dep/subject-expr ?expr]
        [$dep :dep/property ?prop]
        [$db ?base :entity/type :region]
        [(resolve-subject $db ?expr ?base ?prop) ?subject]]
```

### Example 3: Coordinate Parsing

```go
db.RegisterFunction("parse-grid", func(code string) (int64, int64, error) {
    // "A5" → (0, 4)
    if len(code) < 2 {
        return 0, 0, fmt.Errorf("invalid grid code: %s", code)
    }
    x := int64(code[0] - 'A')
    y, err := strconv.ParseInt(code[1:], 10, 64)
    if err != nil {
        return 0, 0, err
    }
    return x, y - 1, nil
})
```

```clojure
[:find ?region ?x ?y
 :where [?region :region/code ?code]
        [(parse-grid ?code) ?x ?y]
        [(>= ?x 0)]
        [(< ?x 10)]]
```

### Example 4: NPC Action Selection

```go
type NPCState struct {
    Health    float64
    Mana      float64
    InCombat  bool
}

db.RegisterFunction("select-action", func(state NPCState, targetHP float64) Keyword {
    if state.Health < 20 {
        return Keyword(":action/flee")
    }
    if state.Mana > 50 && targetHP > 100 {
        return Keyword(":action/cast-spell")
    }
    if state.InCombat {
        return Keyword(":action/attack")
    }
    return Keyword(":action/idle")
})
```

```clojure
[:find ?npc ?action
 :where [?npc :npc/state ?state]
        [?npc :npc/target ?target]
        [?target :character/health ?target-hp]
        [(select-action ?state ?target-hp) ?action]]
```

## Performance Considerations

### Reflection Overhead

- **Registration time**: Signature analysis happens once at registration
- **Call time**: Use cached wrapper, minimal reflection per call
- **Code generation** is the alternative for hot paths, at the cost of a build step

### Function Caching

```go
// Memoization for pure functions (optional)
db.RegisterFunction("expensive-compute", computeFn,
    datalog.WithMemoization(1000),  // Cache up to 1000 results
)
```

### Inlining Built-ins

Common patterns could be optimized:

```go
// Registered as function
db.RegisterFunction("add", func(a, b int64) int64 { return a + b })

// Could be optimized to use built-in + operator
// if detected as equivalent
```

## Relationship to Other Proposals

### Multi-Source Queries

**Orthogonal and composable.** Functions receive values from any source:

```clojure
[:find ?result
 :in $source1 $source2
 :where [$source1 ?a :attr1 ?val1]
        [$source2 ?b :attr2 ?val2]
        [(combine ?val1 ?val2) ?result]]
```

Functions don't know or care which source values came from.

### Recursive Rules

**Composable.** Functions can be used within recursive rules:

```clojure
[[(shortest-path ?from ?to ?dist)
  [?from :edge ?to ?d]
  [(identity ?d) ?dist]]
 [(shortest-path ?from ?to ?dist)
  [?from :edge ?mid ?d]
  (shortest-path ?mid ?to ?rest)
  [(+ ?d ?rest) ?dist]]]
```

### Upsert Semantics

Registered functions become powerful when combined with mutation:

```clojure
[:upsert [?char :character/health ?new-hp]
 :where [?char :character/health ?current]
        [?char :character/location ?loc]
        [?loc :location/hazard ?hazard]
        [(compute-damage ?hazard ?char) ?dmg]
        [(- ?current ?dmg) ?new-hp]]
```

Query finds affected entities, functions compute new values, upsert applies changes. This forms a complete **game rules engine**:

1. **Multi-source**: Game state from multiple stores
2. **Registered functions**: Game logic (damage, AI, physics)
3. **Upsert**: Apply computed state changes
4. **Recursive rules**: Pathfinding, propagation

## The choices

- **The Go value a source argument carries.** One alternative hands the
  function the `executor.PatternMatcher` the engine routes the source to.
  Another hands it a read view built for functions, carrying entity and
  attribute lookup and nothing else. The examples write the type as `Source`.

## References

1. Datomic Transaction Functions - https://docs.datomic.com/pro/transactions/transaction-functions.html
2. Datomic query functions - https://docs.datomic.com/query/query-data-reference.html
3. SPARQL Extension Functions - https://www.w3.org/TR/sparql11-query/#extensionFunctions
4. PostgreSQL CREATE FUNCTION - https://www.postgresql.org/docs/current/sql-createfunction.html
5. janus-datalog QueryInto - `docs/reference/QUERY_INTO.md`
