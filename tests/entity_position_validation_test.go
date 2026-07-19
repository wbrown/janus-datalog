package tests

import (
	"os"
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// The entity position of a data pattern is inhabited only by Identity. A
// string constant there is a query defect, not a value that happens never to
// match: it must fail loudly instead of silently returning empty results.
// In value position a string against an Identity-valued attribute is an
// ordinary typed non-match, like a string constant against an int64 value.

func TestStringConstantInEntityPositionIsError(t *testing.T) {
	dir, err := os.MkdirTemp("", "entity-position-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := storage.NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	alice := datalog.NewIdentity("user:alice")
	tx := db.NewTransaction()
	tx.Add(alice, datalog.NewKeyword(":user/name"), "Alice")
	if _, err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	_, err = executor.CollectTuples(db.Query(
		`[:find ?v :where ["user:alice" :user/name ?v]]`,
	))
	if err == nil {
		t.Fatal("expected an error for a string constant in entity position, got none")
	}
	if !strings.Contains(err.Error(), "entity position") {
		t.Errorf("error should name the entity position; got: %v", err)
	}
}

// TestStringInputBoundToEntityPositionIsError pins the bound-value side of
// the invariant: a string :in input joined into entity position fails loudly
// on every execution path. Cache enabled and disabled route through different
// match strategies (cache lookup vs join strategies); both must error, not
// silently return empty results.
func TestStringInputBoundToEntityPositionIsError(t *testing.T) {
	for _, mode := range []struct {
		name         string
		disableCache bool
	}{
		{"cache_enabled", false},
		{"cache_disabled", true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "entity-binding-*")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(dir)

			db, err := storage.NewDatabaseWithOptions(storage.DatabaseOptions{
				Path:         dir,
				DisableCache: mode.disableCache,
			})
			if err != nil {
				t.Fatalf("Failed to create database: %v", err)
			}
			defer db.Close()

			alice := datalog.NewIdentity("user:alice")
			tx := db.NewTransaction()
			tx.Add(alice, datalog.NewKeyword(":user/name"), "Alice")
			if _, err := tx.Commit(); err != nil {
				t.Fatal(err)
			}

			_, err = executor.CollectTuples(db.Query(
				`[:find ?n :in $ ?e :where [?e :user/name ?n]]`,
				"user:alice",
			))
			if err == nil {
				t.Fatal("expected an error for a string input bound to entity position, got none")
			}
			if !strings.Contains(err.Error(), "entity position") {
				t.Errorf("error should name the entity position; got: %v", err)
			}
		})
	}
}

// TestStringInputBoundToEntityPositionVectorAttributeIsError covers the
// cardinality-vector match path, which resolves RGA state per bound entity
// and previously skipped non-Identity bindings silently.
func TestStringInputBoundToEntityPositionVectorAttributeIsError(t *testing.T) {
	dir, err := os.MkdirTemp("", "entity-binding-vector-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":product/tags"),
		ValueType:   schema.TypeString,
		Cardinality: schema.CardinalityVector,
	})

	db, err := storage.NewDatabaseWithOptions(storage.DatabaseOptions{
		Path:   dir,
		Schema: s,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	widget := datalog.NewIdentity("product:widget")
	tx := db.NewTransaction()
	tx.Add(widget, datalog.NewKeyword(":product/tags"), "electronics")
	tx.Add(widget, datalog.NewKeyword(":product/tags"), "sale")
	if _, err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	_, err = executor.CollectTuples(db.Query(
		`[:find ?tags :in $ ?e :where [?e :product/tags ?tags]]`,
		"product:widget",
	))
	if err == nil {
		t.Fatal("expected an error for a string input bound to entity position on a vector attribute, got none")
	}
	if !strings.Contains(err.Error(), "entity position") {
		t.Errorf("error should name the entity position; got: %v", err)
	}
}

// The attribute position is inhabited only by Keyword — the same boundary
// rules as the entity position: constants and :in inputs fail loudly,
// interior data flow is a typed non-match.

func TestStringConstantInAttributePositionIsError(t *testing.T) {
	dir, err := os.MkdirTemp("", "attr-position-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := storage.NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	alice := datalog.NewIdentity("user:alice")
	tx := db.NewTransaction()
	tx.Add(alice, datalog.NewKeyword(":user/name"), "Alice")
	if _, err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	_, err = executor.CollectTuples(db.Query(
		`[:find ?v :where [?e ":user/name" ?v]]`,
	))
	if err == nil {
		t.Fatal("expected an error for a string constant in attribute position, got none")
	}
	if !strings.Contains(err.Error(), "attribute position") {
		t.Errorf("error should name the attribute position; got: %v", err)
	}
}

func TestStringInputBoundToAttributePositionIsError(t *testing.T) {
	dir, err := os.MkdirTemp("", "attr-input-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := storage.NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	alice := datalog.NewIdentity("user:alice")
	nameAttr := datalog.NewKeyword(":user/name")
	tx := db.NewTransaction()
	tx.Add(alice, nameAttr, "Alice")
	if _, err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	_, err = executor.CollectTuples(db.Query(
		`[:find ?v :in $ ?a :where [?e ?a ?v]]`,
		":user/name",
	))
	if err == nil {
		t.Fatal("expected an error for a string input bound to attribute position, got none")
	}
	if !strings.Contains(err.Error(), "attribute position") {
		t.Errorf("error should name the attribute position; got: %v", err)
	}

	// The sanctioned path: a Keyword input works.
	tuples, err := executor.CollectTuples(db.Query(
		`[:find ?v :in $ ?a :where [?e ?a ?v]]`,
		nameAttr,
	))
	if err != nil {
		t.Fatalf("Keyword input must work: %v", err)
	}
	if len(tuples) != 1 {
		t.Fatalf("expected 1 result, got %d", len(tuples))
	}
}

// TestVAJoinOverMixedDataMatchesOnlyKeywords pins the interior shape for the
// attribute position: a value joined from V position into A position over
// data holding both keywords and strings keeps keyword-partnered rows and
// drops the rest; no error.
func TestVAJoinOverMixedDataMatchesOnlyKeywords(t *testing.T) {
	dir, err := os.MkdirTemp("", "va-join-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := storage.NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	alice := datalog.NewIdentity("user:alice")
	meta1 := datalog.NewIdentity("meta:1")
	meta2 := datalog.NewIdentity("meta:2")
	nameAttr := datalog.NewKeyword(":user/name")

	tx := db.NewTransaction()
	tx.Add(alice, nameAttr, "Alice")
	tx.Add(meta1, datalog.NewKeyword(":meta/attr"), nameAttr)     // a real keyword
	tx.Add(meta2, datalog.NewKeyword(":meta/attr"), ":user/name") // its text
	if _, err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	tuples, err := executor.CollectTuples(db.Query(
		`[:find ?v :where [?m :meta/attr ?a] [?e ?a ?v]]`,
	))
	if err != nil {
		t.Fatalf("mixed V→A join must not error: %v", err)
	}
	if len(tuples) != 1 {
		t.Fatalf("expected 1 row (only the keyword joins), got %d: %v", len(tuples), tuples)
	}
	if v, ok := tuples[0][0].(string); !ok || v != "Alice" {
		t.Errorf("expected \"Alice\", got %v", tuples[0][0])
	}
}

func TestStringConstantInValuePositionIsTypedNonMatch(t *testing.T) {
	dir, err := os.MkdirTemp("", "value-position-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := storage.NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	alice := datalog.NewIdentity("user:alice")
	group := datalog.NewIdentity("group:admins")
	tx := db.NewTransaction()
	tx.Add(alice, datalog.NewKeyword(":user/group"), group)
	if _, err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Neither the seed string nor the L85 text of the referenced identity may
	// match the Identity value; both are ordinary typed non-matches.
	for _, constant := range []string{"group:admins", group.L85()} {
		tuples, err := executor.CollectTuples(db.Query(
			`[:find ?e :in $ ?s :where [?e :user/group ?s]]`,
			constant,
		))
		if err != nil {
			t.Fatalf("string in value position must be a non-match, not an error; got: %v", err)
		}
		if len(tuples) != 0 {
			t.Errorf("string constant %q matched an Identity value; comparison-time coercion must not exist", constant)
		}
	}

	// The sanctioned path: the Identity itself matches.
	tuples, err := executor.CollectTuples(db.Query(
		`[:find ?e :in $ ?g :where [?e :user/group ?g]]`,
		group,
	))
	if err != nil {
		t.Fatal(err)
	}
	if len(tuples) != 1 {
		t.Errorf("Identity input should match exactly one entity, got %d", len(tuples))
	}
}
