package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// TestEmptyStringValueIsLiteralNotWildcard reproduces the suspected bug that an
// empty-string constant in a pattern — [?e :attr ""] — is treated as a wildcard
// (matching every value for the attribute) instead of as a literal that matches
// only the empty-string value.
//
// Three entities share :person/nickname; one has the empty string, two do not.
// A non-empty constant ("Bob") is the control: it must match exactly one entity,
// proving constant-value filtering works. The empty-string constant must behave
// the same way — match only the empty-string entity. If it instead returns all
// three, the empty string is acting as a wildcard.
func TestEmptyStringValueIsLiteralNotWildcard(t *testing.T) {
	dir := t.TempDir()
	db, err := NewDatabaseWithOptions(DatabaseOptions{Path: dir})
	require.NoError(t, err)
	defer db.Close()

	a := datalog.NewKeyword(":person/nickname")
	eEmpty := datalog.NewIdentity("e-empty")
	eBob := datalog.NewIdentity("e-bob")
	eCarol := datalog.NewIdentity("e-carol")

	tx := db.NewTransaction()
	require.NoError(t, tx.Set(eEmpty, a, ""))
	require.NoError(t, tx.Set(eBob, a, "Bob"))
	require.NoError(t, tx.Set(eCarol, a, "Carol"))
	_, err = tx.Commit()
	require.NoError(t, err)

	// Control: a non-empty constant filters to exactly the matching entity.
	bob, err := executor.CollectTuples(db.Query(`[:find ?e :where [?e :person/nickname "Bob"]]`))
	require.NoError(t, err)
	require.Len(t, bob, 1, "sanity: a non-empty string constant must match exactly one entity")

	// The bug: the empty-string constant must match ONLY the empty-string value,
	// not act as a wildcard over the whole attribute.
	empty, err := executor.CollectTuples(db.Query(`[:find ?e :where [?e :person/nickname ""]]`))
	require.NoError(t, err)
	require.Len(t, empty, 1, "empty-string constant must match only the empty-string value, not act as a wildcard")
	require.Equal(t, eEmpty, empty[0][0], "the single match must be the empty-string entity")
}

// TestNonEmptyValueIsExactNotPrefix locks in the exact-equality semantics chosen
// for the fix: a value constant matches by exact equality, not by prefix. With
// "Bob" and "Bobby" both present, [?e :attr "Bob"] must return only "Bob".
func TestNonEmptyValueIsExactNotPrefix(t *testing.T) {
	dir := t.TempDir()
	db, err := NewDatabaseWithOptions(DatabaseOptions{Path: dir})
	require.NoError(t, err)
	defer db.Close()

	a := datalog.NewKeyword(":person/nickname")
	eBob := datalog.NewIdentity("e-bob")
	eBobby := datalog.NewIdentity("e-bobby")

	tx := db.NewTransaction()
	require.NoError(t, tx.Set(eBob, a, "Bob"))
	require.NoError(t, tx.Set(eBobby, a, "Bobby"))
	_, err = tx.Commit()
	require.NoError(t, err)

	got, err := executor.CollectTuples(db.Query(`[:find ?e :where [?e :person/nickname "Bob"]]`))
	require.NoError(t, err)
	require.Len(t, got, 1, "a value constant must match exactly, not as a prefix")
	require.Equal(t, eBob, got[0][0])
}

// TestEmptyStringValueBoundEntity_IsLiteralNotWildcard exercises the E-bound path
// (E supplied via :in). With ?e = e-bob (whose value is "Bob"), [?e :attr ""]
// must NOT match (its value is not the empty string).
func TestEmptyStringValueBoundEntity_IsLiteralNotWildcard(t *testing.T) {
	dir := t.TempDir()
	db, err := NewDatabaseWithOptions(DatabaseOptions{Path: dir})
	require.NoError(t, err)
	defer db.Close()

	a := datalog.NewKeyword(":person/nickname")
	eEmpty := datalog.NewIdentity("e-empty")
	eBob := datalog.NewIdentity("e-bob")

	tx := db.NewTransaction()
	require.NoError(t, tx.Set(eEmpty, a, ""))
	require.NoError(t, tx.Set(eBob, a, "Bob"))
	_, err = tx.Commit()
	require.NoError(t, err)

	// ?e bound to e-bob, whose value is "Bob": the empty-string pattern must not match.
	got, err := executor.CollectTuples(db.Query(`[:find ?e :in $ ?e :where [?e :person/nickname ""]]`, eBob))
	require.NoError(t, err)
	require.Len(t, got, 0, "empty-string constant must not match a bound entity whose value is non-empty")

	// ?e bound to e-empty: the empty-string pattern must match.
	hit, err := executor.CollectTuples(db.Query(`[:find ?e :in $ ?e :where [?e :person/nickname ""]]`, eEmpty))
	require.NoError(t, err)
	require.Len(t, hit, 1, "empty-string constant must match the bound entity whose value is empty")
}

// TestEmptyStringValueJoinBoundEntity_IsLiteralNotWildcard exercises the path
// where E is bound by a prior clause (join), then a V-bound clause filters on "".
func TestEmptyStringValueJoinBoundEntity_IsLiteralNotWildcard(t *testing.T) {
	dir := t.TempDir()
	db, err := NewDatabaseWithOptions(DatabaseOptions{Path: dir})
	require.NoError(t, err)
	defer db.Close()

	nick := datalog.NewKeyword(":person/nickname")
	dept := datalog.NewKeyword(":person/dept")
	eEmpty := datalog.NewIdentity("e-empty")
	eBob := datalog.NewIdentity("e-bob")

	tx := db.NewTransaction()
	require.NoError(t, tx.Set(eEmpty, nick, ""))
	require.NoError(t, tx.Set(eEmpty, dept, "eng"))
	require.NoError(t, tx.Set(eBob, nick, "Bob"))
	require.NoError(t, tx.Set(eBob, dept, "eng"))
	_, err = tx.Commit()
	require.NoError(t, err)

	// First clause binds both (same dept); the "" clause must filter to e-empty only.
	got, err := executor.CollectTuples(db.Query(
		`[:find ?e :where [?e :person/dept "eng"] [?e :person/nickname ""]]`))
	require.NoError(t, err)
	require.Len(t, got, 1, "empty-string constant must filter a join-bound entity set to exact matches")
	require.Equal(t, eEmpty, got[0][0])
}
