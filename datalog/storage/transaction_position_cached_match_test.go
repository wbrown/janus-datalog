package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// transactionPositionFixture writes the two histories the transaction-position
// tests read. Under the plain rule first and second each assert :item/one "x"
// in their own commit. Under the unique rule holder asserts :item/code "y"
// then "x", and claimant asserts "x" after it, so the walk settles holder's
// value at "y", the older assertion, while the resolution's version is
// claimant's Tx. It returns the last commit and the ElementID of each datom,
// read through History, where every datom is read as written.
type transactionPositionFixture struct {
	last                                         datalog.ElementID
	firstX, secondX, holderY, holderX, claimantX datalog.ElementID
}

func writeTransactionPositionFixture(t *testing.T, db *Database) transactionPositionFixture {
	t.Helper()
	one := datalog.NewKeyword(":item/one")
	code := datalog.NewKeyword(":item/code")
	first := datalog.NewIdentity("item:first")
	second := datalog.NewIdentity("item:second")
	holder := datalog.NewIdentity("item:holder")
	claimant := datalog.NewIdentity("item:claimant")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(first, one, "x"))
	require.NoError(t, tx.Add(holder, code, "y"))
	_, err := tx.Commit()
	require.NoError(t, err)
	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Add(second, one, "x"))
	require.NoError(t, tx2.Add(holder, code, "x"))
	_, err = tx2.Commit()
	require.NoError(t, err)
	tx3 := db.NewTransaction()
	require.NoError(t, tx3.Add(claimant, code, "x"))
	last, err := tx3.Commit()
	require.NoError(t, err)

	txOf := func(seed, attr, value string) datalog.ElementID {
		t.Helper()
		got, err := executor.CollectTuples(db.History().Query(fmt.Sprintf(`[:find ?t :where [#id %q %s %q ?t]]`, seed, attr, value)))
		require.NoError(t, err)
		require.Len(t, got, 1, "%s %s %q is one datom", seed, attr, value)
		id, ok := datalog.DerefElementID(got[0][0])
		require.True(t, ok, "the transaction position binds an ElementID, got %T", got[0][0])
		return id
	}
	return transactionPositionFixture{
		last:      last,
		firstX:    txOf("item:first", ":item/one", "x"),
		secondX:   txOf("item:second", ":item/one", "x"),
		holderY:   txOf("item:holder", ":item/code", "y"),
		holderX:   txOf("item:holder", ":item/code", "x"),
		claimantX: txOf("item:claimant", ":item/code", "x"),
	}
}

// transactionPositionSchema declares :item/one under the plain rule and
// :item/code under the unique rule.
func transactionPositionSchema() *schema.Schema {
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{Ident: datalog.NewKeyword(":item/one"), ValueType: schema.TypeString, Cardinality: schema.CardinalityOne})
	s.Add(&schema.AttributeDefinition{Ident: datalog.NewKeyword(":item/code"), ValueType: schema.TypeString, Cardinality: schema.CardinalityOne, Unique: schema.UniqueIdentity})
	return s
}

func eidText(id datalog.ElementID) string {
	return fmt.Sprintf("#eid [%d %d]", id.Lamport, id.ReplicaID)
}

// withTxValues returns the tuples with the ElementID at position i held by
// value, so two renderings of one Tx compare equal.
func withTxValues(t *testing.T, tuples [][]interface{}, i int) [][]interface{} {
	t.Helper()
	out := make([][]interface{}, 0, len(tuples))
	for _, tuple := range tuples {
		id, ok := datalog.DerefElementID(tuple[i])
		require.True(t, ok, "position %d binds an ElementID, got %T", i, tuple[i])
		held := make([]interface{}, len(tuple))
		copy(held, tuple)
		held[i] = id
		out = append(out, held)
	}
	return out
}

// A pattern with its entity and attribute bound answers its transaction
// position alike with the EA cache on and off: the pair's resolved datom when
// that datom's Tx is the bound T, nothing otherwise, and a transaction
// variable binds the resolved datom's own Tx, under the plain rule and under
// the unique rule, whose walk settles on an older assertion than the
// resolution's version (docs/proposals/BOUND_TRANSACTION_POSITION_PROOF.md,
// T5). The entity is the pattern's constant or a binding's, in the latest
// view and as of the last commit.
func TestABoundEntityAndAttributeAnswerTheTransactionPositionAlikeWithTheCacheOnAndOff(t *testing.T) {
	first := datalog.NewIdentity("item:first")
	second := datalog.NewIdentity("item:second")
	holder := datalog.NewIdentity("item:holder")
	claimant := datalog.NewIdentity("item:claimant")

	for _, mode := range legsFor(optimizerModes) {
		for _, cacheMode := range cacheTestModes {
			t.Run(mode.name+"/"+cacheMode.name, func(t *testing.T) {
				db := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: transactionPositionSchema(), DisableCache: cacheMode.disableCache})
				f := writeTransactionPositionFixture(t, db)

				answers := func(view *Database, text string, args ...interface{}) [][]interface{} {
					t.Helper()
					got, err := executor.CollectTuples(view.Query(text, args...))
					require.NoError(t, err, text)
					return got
				}

				for _, view := range []struct {
					name string
					db   *Database
				}{{"latest", db}, {"as of the last commit", db.AsOf(f.last)}} {
					// The plain rule: first's pair resolves to its one assertion.
					assert.ElementsMatch(t, [][]interface{}{{"x"}}, answers(view.db, fmt.Sprintf(`[:find ?v :where [#id "item:first" :item/one ?v %s]]`, eidText(f.firstX))), "%s: first's value under its assertion's Tx", view.name)
					assert.Empty(t, answers(view.db, fmt.Sprintf(`[:find ?v :where [#id "item:first" :item/one ?v %s]]`, eidText(f.secondX))), "%s: first's value under second's Tx", view.name)
					assert.ElementsMatch(t, [][]interface{}{{f.firstX}}, withTxValues(t, answers(view.db, `[:find ?t :where [#id "item:first" :item/one "x" ?t]]`), 0), "%s: the Tx of first's value", view.name)
					assert.ElementsMatch(t, [][]interface{}{{first, "x"}}, answers(view.db, fmt.Sprintf(`[:find ?e ?v :in $ [?e ...] :where [?e :item/one ?v %s]]`, eidText(f.firstX)), []interface{}{first, second}), "%s: the entities bound by an input whose value first's Tx wrote", view.name)
					assert.Empty(t, answers(view.db, fmt.Sprintf(`[:find ?e :in $ ?e :where [?e :item/one "x" %s]]`, eidText(f.secondX)), first), "%s: first bound by an input under second's Tx", view.name)
					assert.ElementsMatch(t, [][]interface{}{{first}}, answers(view.db, fmt.Sprintf(`[:find ?e :in $ ?e :where [?e :item/one "x" %s]]`, eidText(f.firstX)), first), "%s: first bound by an input under its own Tx", view.name)

					// The unique rule: holder's pair resolves to its older assertion.
					assert.ElementsMatch(t, [][]interface{}{{"y", f.holderY}}, withTxValues(t, answers(view.db, `[:find ?v ?t :where [#id "item:holder" :item/code ?v ?t]]`), 1), "%s: holder's value and its Tx", view.name)
					assert.ElementsMatch(t, [][]interface{}{{"y"}}, answers(view.db, fmt.Sprintf(`[:find ?v :where [#id "item:holder" :item/code ?v %s]]`, eidText(f.holderY))), "%s: holder's value under the walk's Tx", view.name)
					assert.Empty(t, answers(view.db, fmt.Sprintf(`[:find ?v :where [#id "item:holder" :item/code ?v %s]]`, eidText(f.holderX))), "%s: holder's value under its superseded assertion's Tx", view.name)
					assert.ElementsMatch(t, [][]interface{}{{f.holderY}}, withTxValues(t, answers(view.db, `[:find ?t :where [#id "item:holder" :item/code "y" ?t]]`), 0), "%s: the Tx of holder's value", view.name)
					assert.ElementsMatch(t, [][]interface{}{{holder, f.holderY}, {claimant, f.claimantX}}, withTxValues(t, answers(view.db, `[:find ?e ?t :in $ [?e ...] :where [?e :item/code ?v ?t]]`, []interface{}{holder, claimant}), 1), "%s: the Tx of each entity's value, the entities bound by an input", view.name)
				}
			})
		}
	}
}

// A cardinality-one cache entry carries the Tx of the datom it resolves to
// beside the resolution's version. Under the plain rule the two are one
// ElementID. Under the unique rule the walk settles holder's value at its
// older assertion, while the version is the highest datom the walk read,
// claimant's claim (docs/proposals/BOUND_TRANSACTION_POSITION_PROOF.md, T5,
// F2).
func TestACardinalityOneEntryCarriesTheResolvedDatomsTx(t *testing.T) {
	first := datalog.NewIdentity("item:first")
	holder := datalog.NewIdentity("item:holder")
	one := datalog.NewKeyword(":item/one")
	code := datalog.NewKeyword(":item/code")

	for _, mode := range legsFor(optimizerModes) {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: transactionPositionSchema()})
			f := writeTransactionPositionFixture(t, db)
			matcher := db.Matcher().(*PatternMatcher)
			require.NotNil(t, matcher.cache, "the database runs with its EA cache")

			resolve := func(e datalog.Identity, a datalog.Keyword) *CacheEntry {
				t.Helper()
				entry, err := matcher.cache.GetOrResolve(CacheKey{E: e, A: a}, matcher.schema.GetAttribute(a), matcher, matcher.cache.ReadPoint(), nil, DiscardIntake)
				require.NoError(t, err)
				require.NotNil(t, entry, "the pair carries datoms")
				return entry
			}

			plain := resolve(first, one)
			assert.Equal(t, "x", plain.OneValue())
			assert.Equal(t, f.firstX, plain.OneTx(), "the plain rule resolves to first's one assertion")
			assert.Equal(t, f.firstX, plain.Version(), "under the plain rule the version is the resolved datom's Tx")

			unique := resolve(holder, code)
			assert.Equal(t, "y", unique.OneValue())
			assert.Equal(t, f.holderY, unique.OneTx(), "the walk settles on holder's older assertion")
			assert.Equal(t, f.claimantX, unique.Version(), "the version is the highest datom the walk read, claimant's claim")
		})
	}
}

// A unique value matched with the attribute free or bound and the transaction
// bound answers the claimant when the claimant's own assertion is the
// transaction, and nothing under the Tx of an assertion the walk skips
// (docs/proposals/BOUND_TRANSACTION_POSITION_PROOF.md, T4, P16). Holder's
// value is "y", its first assertion; its later "x" is superseded by
// claimant's, so "y" is selected by the first assertion's Tx and by no other,
// and "x" by claimant's Tx and not by holder's. In the latest view and as of
// the last commit, with the EA cache on and off.
func TestAUniqueValueMatchHonorsItsTransactionPosition(t *testing.T) {
	holder := datalog.NewIdentity("item:holder")
	claimant := datalog.NewIdentity("item:claimant")

	for _, mode := range legsFor(optimizerModes) {
		for _, cacheMode := range cacheTestModes {
			t.Run(mode.name+"/"+cacheMode.name, func(t *testing.T) {
				db := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: transactionPositionSchema(), DisableCache: cacheMode.disableCache})
				f := writeTransactionPositionFixture(t, db)

				answers := func(view *Database, text string) [][]interface{} {
					t.Helper()
					got, err := executor.CollectTuples(view.Query(text))
					require.NoError(t, err, text)
					return got
				}

				for _, view := range []struct {
					name string
					db   *Database
				}{{"latest", db}, {"as of the last commit", db.AsOf(f.last)}} {
					for _, c := range []struct {
						name  string
						query string
						want  [][]interface{}
					}{
						{"attribute free, the walk's value under its Tx", fmt.Sprintf(`[:find ?e :where [?e ?a "y" %s]]`, eidText(f.holderY)), [][]interface{}{{holder}}},
						{"attribute free, the walk's value under the skipped assertion's Tx", fmt.Sprintf(`[:find ?e :where [?e ?a "y" %s]]`, eidText(f.holderX)), nil},
						{"attribute bound, the walk's value under its Tx", fmt.Sprintf(`[:find ?e :where [?e :item/code "y" %s]]`, eidText(f.holderY)), [][]interface{}{{holder}}},
						{"attribute bound, the walk's value under the skipped assertion's Tx", fmt.Sprintf(`[:find ?e :where [?e :item/code "y" %s]]`, eidText(f.holderX)), nil},
						{"attribute free, the claimed value under the claimant's Tx", fmt.Sprintf(`[:find ?e :where [?e ?a "x" %s]]`, eidText(f.claimantX)), [][]interface{}{{claimant}}},
						{"attribute free, the claimed value under the superseded assertion's Tx", fmt.Sprintf(`[:find ?e :where [?e ?a "x" %s]]`, eidText(f.holderX)), nil},
						{"attribute bound, the claimed value under the superseded assertion's Tx", fmt.Sprintf(`[:find ?e :where [?e :item/code "x" %s]]`, eidText(f.holderX)), nil},
					} {
						got := answers(view.db, c.query)
						assert.ElementsMatch(t, c.want, got, "%s, %s: answered %d tuples", view.name, c.name, len(got))
					}
				}
			})
		}
	}
}
