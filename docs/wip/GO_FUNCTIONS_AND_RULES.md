# Go Functions and Rules

Design document for extending the query builder with Go function integration and forward-chaining rules.

## Goals

1. Allow Go functions in queries for filtering and value computation
2. Support forward-chaining rules that transform data
3. Keep the API additive - no changes to existing `qb` functions
4. Let developers choose their tradeoffs (closures vs pure functions)

## Part 1: Go Functions in Queries

### API

```go
// Filter - predicate function, must return bool
qb.Filter(fn, args...)

// Eval - expression function, returns value, bind with .As()
qb.Eval(fn, args...).As(result)
```

### Usage

**With closures** (simple, captures state):

```go
// Rebuild query when captured values change
q := qb.Query().
    Find(e).
    Where(
        qb.Pat(e, Position, pos),
        qb.Filter(func(p Vec3) bool {
            return p.DistanceTo(playerPos) < attackRange
        }, pos),
    ).
    MustBuild()
```

**With pure functions** (reusable query):

```go
func InRange(a, b Vec3, r float64) bool {
    return a.DistanceTo(b) < r
}

playerPos := qb.NewVar("playerPos")
rangeVar := qb.NewVar("rangeVar")

q := qb.Query().
    Find(e).
    In(qb.DB, qb.Scalar(playerPos), qb.Scalar(rangeVar)).
    Where(
        qb.Pat(e, Position, pos),
        qb.Filter(InRange, pos, playerPos, rangeVar),
    ).
    MustBuild()

// Reuse query with different values each frame
results := db.ExecuteQueryWithInputs(q, player.Pos, player.Range)
```

**Computing values**:

```go
func CalculateDamage(base, armor int64, multiplier float64) int64 {
    return int64(float64(base-armor) * multiplier)
}

finalDamage := qb.NewVar("finalDamage")

q := qb.Query().
    Find(target, finalDamage).
    Where(
        qb.Pat(attack, AttackTarget, target),
        qb.Pat(attack, BaseDamage, baseDmg),
        qb.Pat(target, Armor, armor),
        qb.Pat(attack, DamageMultiplier, mult),
        qb.Eval(CalculateDamage, baseDmg, armor, mult).As(finalDamage),
    ).
    MustBuild()
```

### Implementation

```go
// GoFilter represents a Go function used as a predicate
type GoFilter struct {
    fn   reflect.Value
    args []*Var
}

func Filter(fn interface{}, args ...*Var) *GoFilter {
    rv := reflect.ValueOf(fn)
    if rv.Kind() != reflect.Func {
        panic("Filter: first argument must be a function")
    }
    // Validate returns bool
    if rv.Type().NumOut() != 1 || rv.Type().Out(0).Kind() != reflect.Bool {
        panic("Filter: function must return bool")
    }
    // Validate arg count
    if rv.Type().NumIn() != len(args) {
        panic(fmt.Sprintf("Filter: function takes %d args, got %d",
            rv.Type().NumIn(), len(args)))
    }
    return &GoFilter{fn: rv, args: args}
}

func (g *GoFilter) toClause() query.Clause {
    syms := make([]query.Symbol, len(g.args))
    for i, v := range g.args {
        syms[i] = v.Symbol()
    }
    return &query.GoFilterClause{
        Fn:   g.fn,
        Args: syms,
    }
}
```

The executor calls `fn.Call()` with bound values for each candidate tuple.

### Type Checking

- Argument count validated at Build() time
- Argument types validated at execution time (first invocation)
- Type mismatches panic with clear error message

---

## Part 2: Forward-Chaining Rules

### API

```go
qb.Rule().
    When(clauses...).
    Then(effects...).
    MustBuild()
```

**When clause** - uses same clause types as `Query().Where()`:
- `qb.Pat()` - patterns
- `qb.Filter()` - Go predicates
- `qb.Eval().As()` - computed values
- `qb.Lt()`, `qb.Gt()`, etc. - comparisons
- `qb.Not()`, `qb.Or()` - logical operators

**Then clause** - effects to apply:
- `qb.Assert(e, a, v)` - add datom
- `qb.Retract(e, a, v)` - retract specific datom
- `qb.RetractEntity(e)` - retract all datoms for entity

### Usage

**Damage application rule**:

```go
var (
    AttackTarget = qb.Kw(":attack/target")
    AttackDamage = qb.Kw(":attack/damage")
    Health       = qb.Kw(":entity/health")
)

func ApplyDamage(current, damage int64) int64 {
    if damage > current {
        return 0
    }
    return current - damage
}

attack := qb.NewVar("attack")
target := qb.NewVar("target")
damage := qb.NewVar("damage")
currentHealth := qb.NewVar("currentHealth")
newHealth := qb.NewVar("newHealth")

damageRule := qb.Rule().
    When(
        qb.Pat(attack, AttackTarget, target),
        qb.Pat(attack, AttackDamage, damage),
        qb.Pat(target, Health, currentHealth),
        qb.Eval(ApplyDamage, currentHealth, damage).As(newHealth),
    ).
    Then(
        qb.Assert(target, Health, newHealth),
        qb.RetractEntity(attack),  // consume the event
    ).
    MustBuild()
```

**Death detection rule**:

```go
entity := qb.NewVar("entity")
health := qb.NewVar("health")

deathRule := qb.Rule().
    When(
        qb.Pat(entity, Health, health),
        qb.Lte(health, 0),
        qb.Not(qb.Pat(entity, Dead, qb.V(true))),
    ).
    Then(
        qb.Assert(entity, Dead, qb.V(true)),
        qb.Assert(entity, DeathTime, qb.V(time.Now())),
    ).
    MustBuild()
```

### Firing Rules

**Single rule, manual control**:

```go
// Fire returns transaction for inspection
tx := damageRule.Fire(db)
fmt.Printf("Rule matched %d times\n", tx.DatomCount())
tx.Commit()

// Or direct commit
damageRule.FireAndCommit(db)
```

**Multiple rules, batch execution**:

```go
// Register rules
db.RegisterRule("damage", damageRule)
db.RegisterRule("death", deathRule)
db.RegisterRule("cleanup", cleanupRule)

// Fire all rules once
db.FireRulesOnce()

// Fire until fixpoint (no rule matches)
db.FireRulesToFixpoint()

// Fire with iteration limit
db.FireRules(MaxIterations(100))
```

**Event-driven game loop**:

```go
// Each frame
func (g *Game) Update() {
    // Process input -> create attack events
    g.processInput()

    // Fire combat rules
    g.db.FireRulesToFixpoint()

    // Query for rendering
    g.render()
}
```

### Implementation

```go
// RuleBuilder constructs a forward-chaining rule
type RuleBuilder struct {
    when   []interface{}  // same as Where clauses
    then   []Effect
    errors []error
}

func Rule() *RuleBuilder {
    return &RuleBuilder{}
}

func (r *RuleBuilder) When(clauses ...interface{}) *RuleBuilder {
    r.when = append(r.when, clauses...)
    return r
}

func (r *RuleBuilder) Then(effects ...Effect) *RuleBuilder {
    r.then = append(r.then, effects...)
    return r
}

func (r *RuleBuilder) Build() (*CompiledRule, error) {
    // Build internal query from When clauses
    // Compile Then effects with variable references
}

func (r *RuleBuilder) MustBuild() *CompiledRule {
    rule, err := r.Build()
    if err != nil {
        panic(err)
    }
    return rule
}
```

**Effect types**:

```go
// Effect is something that happens when a rule fires
type Effect interface {
    toEffect() ruleEffect
}

// AssertEffect adds a datom
type AssertEffect struct {
    e, a, v interface{}  // can be *Var or constant
}

func Assert(e, a, v interface{}) *AssertEffect {
    return &AssertEffect{e: e, a: a, v: v}
}

// RetractEffect removes a datom
type RetractEffect struct {
    e, a, v interface{}
}

func Retract(e, a, v interface{}) *RetractEffect {
    return &RetractEffect{e: e, a: a, v: v}
}

// RetractEntityEffect removes all datoms for an entity
type RetractEntityEffect struct {
    e interface{}
}

func RetractEntity(e interface{}) *RetractEntityEffect {
    return &RetractEntityEffect{e: e}
}
```

### CompiledRule Execution

```go
type CompiledRule struct {
    query   *query.Query      // built from When clauses
    effects []compiledEffect  // built from Then clause
}

func (r *CompiledRule) Fire(db *Database) *Transaction {
    // Execute query
    results, _ := db.ExecuteQuery(r.query)

    // Create transaction
    tx := db.NewTransaction()

    // For each result row, apply effects with bound values
    for _, row := range results {
        for _, effect := range r.effects {
            effect.apply(tx, row)
        }
    }

    return tx
}
```

---

## API Compatibility

### Existing API (unchanged)

```go
// Variables and constants
qb.NewVar()
qb.Kw(":attr/name")
qb.V(value)
qb.Blank()

// Query builder
qb.Query().Find(...).In(...).Where(...).OrderBy(...).Build()

// Patterns
qb.Pat(e, a, v)

// Predicates
qb.Lt(), qb.Lte(), qb.Gt(), qb.Gte(), qb.Eq(), qb.Ne()
qb.Range(), qb.RangeInclusive()

// Expressions
qb.Add(), qb.Sub(), qb.Mul(), qb.Div()
qb.Str(), qb.Ground(), qb.Identity()
qb.Year(), qb.Month(), qb.Day(), qb.Hour(), qb.Minute(), qb.Second()

// Aggregations
qb.Sum(), qb.Count(), qb.Avg(), qb.Min(), qb.Max()

// Logical
qb.Not(), qb.NotJoin(), qb.Or(), qb.OrJoin()

// Subqueries
qb.Subquery().BindTuple(), .BindRelation(), .BindCollection()

// Ordering
qb.Asc(), qb.Desc()
```

### New API (additive)

```go
// Go functions (Part 1)
qb.Filter(fn, args...)        // predicate
qb.Eval(fn, args...).As(v)    // expression

// Rules (Part 2)
qb.Rule().When(...).Then(...).Build()

// Effects (for Then clause)
qb.Assert(e, a, v)
qb.Retract(e, a, v)
qb.RetractEntity(e)

// Database rule registration
db.RegisterRule(name, rule)
db.FireRulesOnce()
db.FireRulesToFixpoint()
db.FireRules(opts...)
```

### Clause Reuse

The key design insight: `When()` accepts the same clause types as `Where()`.

This means `Filter` and `Eval` work in both contexts:

```go
// In a query
qb.Query().
    Where(
        qb.Pat(e, Health, hp),
        qb.Filter(IsAlive, hp),
    )

// In a rule
qb.Rule().
    When(
        qb.Pat(e, Health, hp),
        qb.Filter(IsAlive, hp),
    ).
    Then(...)
```

No API changes needed. The `Clause` interface already supports this.

---

## Future Considerations

### Rule Priorities

```go
qb.Rule().
    Priority(10).  // higher = fires first
    When(...).
    Then(...)
```

### Conditional Effects

```go
qb.Rule().
    When(...).
    Then(
        qb.If(condition,
            qb.Assert(...),
        ).Else(
            qb.Assert(...),
        ),
    )
```

### New Entity Creation

```go
newEntity := qb.NewVar("newEntity")

qb.Rule().
    When(...).
    Then(
        qb.Create(newEntity),  // generates new entity ID
        qb.Assert(newEntity, Type, qb.V("explosion")),
        qb.Assert(newEntity, Position, pos),
    )
```

### Aggregating Rules

```go
// Rule that fires once with aggregated data
qb.Rule().
    When(
        qb.Pat(e, Faction, faction),
        qb.Pat(e, Score, score),
    ).
    GroupBy(faction).
    Aggregate(qb.Sum(score).As(totalScore)).
    Then(
        qb.Assert(faction, TotalScore, totalScore),
    )
```
