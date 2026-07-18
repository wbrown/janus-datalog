package tests

import (
	"os"
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
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
