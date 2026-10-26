# The storage package's js/wasm run has no stated time budget: coverage was cut to fit the ten-minute default, then the leg was given forty-five minutes

**Status**: Open.

No answer is wrong. The defect has three parts: a ten-minute bound the run
was cut to fit and has since outgrown, a guideline the gate contradicts,
and coverage removed for a bound that was then lifted.

## The history the records carry

`PERFORMANCE_STATUS.md`, "Claims Requiring Qualification": the package's
js/wasm run rose from 258 s to 613 s across the rules work, past `go test`'s
ten-minute default. The rise was one concurrent rules test, reduced to the
evaluations its contract needs, after which the run took 337.5 s. On the
round-3 tree it took 485 s, its largest test
`TestRules_BoundInvocationsOnRandomRuleSetsMatchTheFreeEvaluation` at 112 s.

To hold the run under the default on a four-core machine, that round cut
coverage:

- `ruleSetSeeds` (`rules_random_test.go`) holds a chosen subset of its seed
  range.
- `recordedGraphCaps` (`scope_contracts_graph_test.go`) holds three caps.
- `shapeKeysPerMask` (`rules_shape_differential_test.go`) is 5.
- `testOptimizationMatrix` and `testComplexQueryCheckpointCacheSteadyState`
  (`optimization_matrix_test.go`) populate 30 scenarios where they
  populated 75.

A cut of the closure-forms differential from twenty-five trials to twelve
was refused pending a comparison of the two counts.

## The gate as it stands

The `Makefile`'s `test-wasm-storage` runs the storage package alone under
`-timeout 45m`, its comment saying the package's run goes past twenty
minutes on a CI runner, and `test-wasm-others` runs every other package
with no timeout. `.github/workflows/test.yml` runs the native suite under
`-timeout 45m` and the two wasm legs as two jobs. `CLAUDE.md`'s "Test
Timeouts" says: do not add `-timeout` to `go test` commands, use the
default, no exceptions.

So the leg that was cut to fit ten minutes now runs under forty-five, the
cuts stand, and the guideline forbids the flag the gate passes. On the tree
at `8565cdd` the leg takes **705 s**, `make test-wasm-storage` under its
`-timeout 45m`, on an Apple M5.

## The fix

State one budget for the storage package's js/wasm leg, once in the
`Makefile` and once in `CLAUDE.md`'s rule as that rule's one exception.
Record the run's time in `PERFORMANCE_STATUS.md` each time a test is added
to the package. Under a forty-five-minute budget the cuts are restored: the
seeds, caps, keys per mask and scenario counts the round removed, since the
bound they bought no longer applies. The alternative keeps the default as
the budget, removes the flag from the leg, and sizes every differential to
it, which prices each new differential in coverage elsewhere. The maintainer
decides between the two.

## Tests

The leg's recorded time on the current tree is checked against the stated
budget, and each restored count's run is set beside its cut count.
