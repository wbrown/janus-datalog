package executor

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// RuleEvents is the rules tests' record of the events a handler saw.
type RuleEvents = ruleEvents

// Handle records an event.
func (r *ruleEvents) Handle(e annotations.Event) { r.handle(e) }

// Count answers how many events of one name were seen.
func (r *ruleEvents) Count(name string) int { return r.count(name) }

// Entered answers the tuples that entered a delta, summed over the iteration
// events.
func (r *ruleEvents) Entered() int { return r.entered() }

// Predicates answers every predicate an iteration event reported a delta
// for.
func (r *ruleEvents) Predicates() []query.Symbol { return r.predicates() }

// Events answers the events seen.
func (r *ruleEvents) Events() []annotations.Event { return r.events }

// Reset forgets the events seen.
func (r *ruleEvents) Reset() { r.events = nil }

// QueryExecutor answers the clause executor for one run of plan.
func (e *Executor) QueryExecutor(plan *planner.RealizedPlan) *DefaultQueryExecutor {
	return e.queryExecutor(plan)
}

// DeriveInvocation evaluates an invocation's predicate at its clause and
// answers the relation with its head.
func (e *DefaultQueryExecutor) DeriveInvocation(ctx *Context, inv *query.RuleInvocation, groups []Relation) (Relation, []query.Symbol, error) {
	return e.deriveInvocation(ctx, inv, groups)
}

// NewRuleContinuations answers a form execution's continuations, holding
// none.
var NewRuleContinuations = newRuleContinuations

// ByClause answers the evaluations the continuations hold, by the clause
// each continues under.
func (c *ruleContinuations) ByClause() map[*query.RuleInvocation]*ruleEvaluation {
	return c.byClause
}

// Complete completes every evaluation held.
func (c *ruleContinuations) Complete() { c.complete() }

// Strata answers the evaluation's strata, prepared at its first window.
func (ev *ruleEvaluation) Strata() []*stratumState { return ev.strata }

// Stratum answers the stratum a state runs.
func (s *stratumState) Stratum() query.Stratum { return s.stratum }

// RunStratum runs one window of a stratum and answers the tuples the window
// added per predicate.
func (ev *ruleEvaluation) RunStratum(ctx *Context, s *stratumState, seed Relation, lower map[query.Symbol][]Tuple) (map[query.Symbol][]Tuple, error) {
	return ev.runStratum(ctx, s, seed, lower)
}

// DefaultPlannerOptions is the planner profile NewExecutor constructs with.
var DefaultPlannerOptions = defaultPlannerOptions

// NewQueryExecutor builds a clause executor over an executor on the default
// planner profile.
var NewQueryExecutor = newQueryExecutor

// ExecuteBoundPattern runs the pattern as one body over the window of its
// subject, reading whether it has a match through exists.
func (e *DefaultQueryExecutor) ExecuteBoundPattern(ctx *Context, pattern, exists *query.DataPattern, groups Relations) (Relations, error) {
	return e.executeBoundPattern(ctx, pattern, exists, groups)
}

// SetPlanner replaces the planner the executor plans every scope through.
func (e *Executor) SetPlanner(p planner.QueryPlanner) { e.planner = p }

func (e *DefaultQueryExecutor) Options() ExecutorOptions { return e.options }

// OrClauseEvents answers, in order, the or-clause events among events.
var OrClauseEvents = orClauseEvents

// NewSliceIterator answers an iterator over tuples, before its first tuple.
func NewSliceIterator(tuples []Tuple) *sliceIterator {
	return &sliceIterator{tuples: tuples, pos: -1}
}

// Pos answers the iterator's position, -1 before its first pull.
func (it *sliceIterator) Pos() int { return it.pos }

// MustParse parses a query, failing the test on an error.
var MustParse = mustParse

// MustRules parses a rule set, failing the test on an error.
var MustRules = mustRules

// ChainDatoms links page1 through pageN by :page/next and marks every page
// with :page/kind.
var ChainDatoms = chainDatoms

// FollowsRulesText is the transitive closure of :page/next as a rule set.
const FollowsRulesText = followsRulesText

// SuccessorRulesText is a rule set computing a number's successor from its
// required argument.
const SuccessorRulesText = successorRulesText

// Page, PageNext, PageKind, IdentityPairs, ChainClosure and RequireSamePairs
// are the rules tests' fixtures: the page entities, the chain's attributes, a
// relation of identity pairs read into a set, the chain's closure computed by
// hand, and the comparison of two pair sets.
var (
	Page             = page
	PageNext         = pageNext
	PageKind         = pageKind
	IdentityPairs    = identityPairs
	ChainClosure     = chainClosure
	RequireSamePairs = requireSamePairs
)

// AllPositions answers the positions of a whole tuple of a width, its key.
var AllPositions = allPositions

// HashTuplePositions answers the hash every tuple key of the package hashes a
// tuple's positions by.
var HashTuplePositions = hashTuplePositions

// PrepareStratum answers a stratum's evaluation over the bodies planned for
// it.
func (e *Executor) PrepareStratum(rs *query.RuleSet, planned planner.PlannedStratum) *stratumEvaluation {
	return e.prepareStratum(rs, planned)
}

// Symbols answers a predicate's canonical symbols, its set's head.
func (s *stratumEvaluation) Symbols(predicate query.Symbol) []query.Symbol {
	return s.symbols[predicate]
}

// Bodies answers the stratum's planned bodies.
func (s *stratumEvaluation) Bodies() planner.Bodies { return s.bodies }

// Run executes bodies under a scope holding held, the relations by predicate,
// and answers each body's result relation by predicate.
func (s *stratumEvaluation) Run(ctx *Context, held map[query.Symbol]Relation, bodies []planner.PlannedBody) (map[query.Symbol][]Relation, error) {
	return s.run(ctx, held, bodies)
}

// DerivedSet is a predicate's tuples over its canonical symbols, each held
// once.
type DerivedSet = derivedSet

// NewDerivedSet answers an empty set over symbols.
var NewDerivedSet = newDerivedSet

// Absorb adds the tuples of rel the set does not hold and answers how many it
// added.
func (s *derivedSet) Absorb(rel Relation) (int, error) { return s.absorb(rel) }

// Relation answers the set as a relation over the tuples added so far.
func (s *derivedSet) Relation(opts ExecutorOptions) *MaterializedRelation { return s.relation(opts) }

// Len answers the number of tuples the set holds.
func (s *derivedSet) Len() int { return s.keys.Len() }

// NewRuleEvaluation answers the evaluation planned for an invocation.
var NewRuleEvaluation = newRuleEvaluation

// Extend runs one window of the evaluation, seed entering the seed
// predicate's stratum, and answers the predicate's relation as it then stands.
func (ev *ruleEvaluation) Extend(ctx *Context, seed Relation) (Relation, error) {
	return ev.extend(ctx, seed)
}

// Complete emits the complete event of every stratum that began and has not
// completed.
func (ev *ruleEvaluation) Complete() { ev.complete() }

// Derived answers the evaluation's derived relations by predicate.
func (ev *ruleEvaluation) Derived() map[query.Symbol]Relation { return ev.derived }

// Total answers the tuples the evaluation derived, every window's together.
func (ev *ruleEvaluation) Total() int { return ev.total }

// OptimizerMode is one leg of the package's optimizer mode axis.
type OptimizerMode = optimizerMode

// OptimizerModes is the package's optimizer mode axis.
var OptimizerModes = optimizerModes

// OptimizerModesWhere is the modes a predicate admits, in the table's order.
var OptimizerModesWhere = optimizerModesWhere

// LegsFor is the legs a test runs on for the reach it names.
var LegsFor = legsFor

// Option is one of the six options a reach names.
type Option = option

// AllOptions is the six options.
var AllOptions = allOptions

// The six options a reach names.
const (
	AlgebraOption     = algebraOption
	SharingOption     = sharingOption
	PrefetchOption    = prefetchOption
	SymmetricOption   = symmetricOption
	FusionOption      = fusionOption
	AggregationOption = aggregationOption
)

// Name answers the mode's name.
func (m optimizerMode) Name() string { return m.name }

func (m optimizerMode) Algebra() bool { return m.algebra }

// Sharing answers whether the mode turns scan sharing on.
func (m optimizerMode) Sharing() bool { return m.sharing }

// Prefetch answers whether the mode turns entity prefetch on.
func (m optimizerMode) Prefetch() bool { return m.prefetch }

// Fusion answers whether the mode turns attribute-fetch fusion on.
func (m optimizerMode) Fusion() bool { return m.fusion }

// Symmetric answers whether the mode turns the symmetric hash join on.
func (m optimizerMode) Symmetric() bool { return m.symmetric }

// PlannerOptions is NewExecutor's default profile with the mode's setting of
// every option.
func (m optimizerMode) PlannerOptions() planner.PlannerOptions { return m.plannerOptions() }

// ZeroPlannerOptions is the zero-value profile with the mode's setting of
// every option.
func (m optimizerMode) ZeroPlannerOptions() planner.PlannerOptions { return m.zeroPlannerOptions() }

// Recording wraps handler so the mode's reach recorder receives every event
// the handler does.
func (m optimizerMode) Recording(handler annotations.Handler) annotations.Handler {
	return m.recording(handler)
}

// ApplyFindPulls renders each pull find element of a result.
var ApplyFindPulls = applyFindPulls

// CollectTuplesInto appends every tuple of a relation to a slice, copying
// under the relation's copy answer.
var CollectTuplesInto = collectTuplesInto

// CopyTuple answers a copy of a tuple.
var CopyTuple = copyTuple

// CreateInputRelationsFromValuesWithOptions binds a query's :in values to
// its input relations.
var CreateInputRelationsFromValuesWithOptions = createInputRelationsFromValuesWithOptions

// NewMaterializedRelationFromSetWithProperties holds tuples that are a set
// under the given properties.
var NewMaterializedRelationFromSetWithProperties = newMaterializedRelationFromSet

// DeduplicatedProperties are the properties of a set keyed on its symbols.
var DeduplicatedProperties = deduplicatedProperties

// ErrInjectedIterator is the failure the failing test iterators inject.
var ErrInjectedIterator = errInjectedIterator

// NewFailingStream answers a stream over tuples whose iterator fails with
// ErrInjectedIterator after failAfter of them.
var NewFailingStream = newFailingStream

// NewFailingRelation answers rel with an iterator that fails with
// ErrInjectedIterator after failAfter tuples and whose close answers
// closeErr.
func NewFailingRelation(rel Relation, failAfter int, closeErr error) Relation {
	return failingRelation{Relation: rel, failAfter: failAfter, closeErr: closeErr}
}

// NewReusingWorkspaceStream answers a stream over tuples whose iterator
// hands back one workspace it overwrites on each pull.
var NewReusingWorkspaceStream = newReusingWorkspaceStream

// NewWorkspaceReusingIterator answers an iterator over data, its tuples of
// width, handing back one workspace it overwrites on each pull.
var NewWorkspaceReusingIterator = newWorkspaceReusingIterator

// NewFailingScanMatcher answers a matcher whose scan of failAttr fails and
// whose every other scan answers dataRel.
func NewFailingScanMatcher(failAttr datalog.Keyword, dataRel Relation) PatternMatcher {
	return &failingScanMatcher{failAttr: failAttr, dataRel: dataRel}
}

// BranchPattern, BranchGround and BranchSubquery build an or-form branch's
// clause: a pattern at attr, a ground of v, and an uncorrelated subquery
// binding value from every entity carrying attr.
var (
	BranchPattern  = branchPattern
	BranchGround   = branchGround
	BranchSubquery = branchSubquery
)

// BranchRun is a branch framed under its form's header and planned.
type BranchRun = branchRun

// FrameBranches frames each branch under a framing and plans it.
var FrameBranches = frameBranches

// BuildBranchTuples runs a branch's plan over a run and reads the answer
// whole.
var BuildBranchTuples = buildBranchTuples

// NewSubqueryFraming answers the framing of a subquery's body over its
// required run, presented under the call's substitution.
func NewSubqueryFraming(required []query.Symbol, find []query.FindElement, given, standsFor []query.Symbol, subject Relation, subst query.Substitution) framing {
	return framing{required: required, find: find, given: given, standsFor: standsFor, subject: subject, subst: subst}
}

// PlanBranches frames and plans the relation's branches.
func (r *OrFallbackRelation) PlanBranches() ([]branchRun, error) { return r.planBranches() }

// Executor answers the clause executor the relation's branches run on.
func (r *OrFallbackRelation) Executor() *DefaultQueryExecutor { return r.executor }

// Ctx answers the context the relation's branches run under.
func (r *OrFallbackRelation) Ctx() *Context { return r.ctx }

// Framing answers the relation's framing over an environment relation.
func (r *OrFallbackRelation) Framing(environment Relation) framing { return r.framing(environment) }

// Err answers the iterator's failure.
func (it *windowedIterator) Err() error { return it.err }

// NextWindow reads the next window and runs each branch over it.
func (it *windowedIterator) NextWindow() bool { return it.nextWindow() }

// BranchCache answers the index a branch's builds keep, nil before its first
// build.
func (it *windowedIterator) BranchCache(branch int) *cachedBranch { return it.branchCache[branch] }

// SetReaderWindow places a window and its ordinal on the iterator's reader,
// as the reader's next read would.
func (it *windowedIterator) SetReaderWindow(window []Tuple, ordinal int) {
	it.reader.window, it.reader.ordinal = window, ordinal
}

// RunWindow runs each branch over the reader's window.
func (it *windowedIterator) RunWindow() bool { return it.runWindow() }

// WindowIterator, CacheBuilds, GroundBranch and WindowItems are the window
// tests' fixtures: a fallback's iterator over a subject, the branches of its
// build events, a ground branch, and items carrying one attribute.
var (
	WindowIterator = windowIterator
	CacheBuilds    = cacheBuilds
	GroundBranch   = groundBranch
	WindowItems    = windowItems
)

// NewRecordingBatchEntityResolver answers a resolver serving each entity's
// attributes from values and counting its single and batch calls.
func NewRecordingBatchEntityResolver(values map[[20]byte]map[datalog.Keyword]interface{}) *recordingBatchEntityResolver {
	return &recordingBatchEntityResolver{values: values}
}

// BatchCalls answers how many batch resolutions r served.
func (r *recordingBatchEntityResolver) BatchCalls() int { return r.batchCalls }

// SingleCalls answers how many single-entity resolutions r served.
func (r *recordingBatchEntityResolver) SingleCalls() int { return r.singleCalls }

// PatternOn builds [source ?e attr ?v], a nil source naming the default.
var PatternOn = patternOn

// TryFuseAttributeFetchBundle runs a leading run of same-entity attribute
// fetches as one attach over the group binding the entity, and answers the
// replacement groups and the clauses consumed.
func (e *DefaultQueryExecutor) TryFuseAttributeFetchBundle(ctx *Context, clauses []query.Clause, groups []Relation) ([]Relation, int, error) {
	return e.tryFuseAttributeFetchBundle(ctx, clauses, groups)
}

// NewIteratorCountingRelation answers rel, counting the iterators opened on
// it.
func NewIteratorCountingRelation(rel Relation) *iteratorCountingRelation {
	return &iteratorCountingRelation{Relation: rel}
}

// IteratorCalls answers how many iterators were opened on the relation.
func (r *iteratorCountingRelation) IteratorCalls() int { return r.iteratorCalls }

// NewOrFallbackIterator answers a fallback's iterator over branches, run on
// exec, its framing keyed on key, reporting through opts, before its first
// window and with nothing prefetched.
func NewOrFallbackIterator(exec *DefaultQueryExecutor, key []query.Symbol, branches [][]query.Clause, opts ExecutorOptions) *OrFallbackIterator {
	return &OrFallbackIterator{
		windowedIterator: windowedIterator{executor: exec, framing: framing{key: key}, options: opts},
		branches:         branches,
	}
}

// SetPrefetched records a window's entities as prefetched into the EA cache.
func (it *OrFallbackIterator) SetPrefetched() { it.prefetched = true }

// SetEnvironment places the scope's environment relation in the iterator's
// framing.
func (it *OrFallbackIterator) SetEnvironment(environment Relation) {
	it.framing.environment = environment
}

// BuildBranchFromEACache builds a branch of the EA arm's shape over run
// through the matcher's lookup, and answers the tuples and their symbols.
func (it *OrFallbackIterator) BuildBranchFromEACache(branch []query.Clause, run *MaterializedRelation) ([]Tuple, []query.Symbol) {
	return it.buildBranchFromEACache(branch, run)
}

// CompareTuplesByOrder returns -1 when left precedes right under an ordering
// read at the given positions, 1 when it follows, and 0 when every sort key
// compares equal.
var CompareTuplesByOrder = compareTuplesByOrder

// CollectTypedTuples, ContainsTuplePairwise and AssertRelationPropertiesHold
// are the property tests' fixtures: a relation's tuples collected, a tuple's
// membership in a list, and the check that a relation's declared keys and
// ordering hold of its tuples.
var (
	CollectTypedTuples           = collectTypedTuples
	ContainsTuplePairwise        = containsTuplePairwise
	AssertRelationPropertiesHold = assertRelationPropertiesHold
)

// BuildFromCache is the fallback's build of a branch of data patterns alone
// over run, from the EA cache once a window is prefetched.
func (it *OrFallbackIterator) BuildFromCache(branch int, run *MaterializedRelation) ([]Tuple, []query.Symbol, error) {
	return it.buildFromCache(branch, run)
}
