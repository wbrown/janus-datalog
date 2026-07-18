package parser

import (
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// #id "seed" is NewIdentity in literal form: the parser hashes the seed at
// parse time and produces an Identity constant. It is input-only sugar for
// referencing an entity by its seed string; #identity "L85" remains the
// canonical form and the only one the formatter emits (seed→hash is one-way).

func TestParseIDTaggedLiteral(t *testing.T) {
	q, err := ParseQuery(`[:find ?n :where [#id "user:alice" :user/name ?n]]`)
	if err != nil {
		t.Fatalf("ParseQuery failed: %v", err)
	}

	pattern, ok := q.Where[0].(*query.DataPattern)
	if !ok {
		t.Fatalf("expected DataPattern, got %T", q.Where[0])
	}
	constant, ok := pattern.GetE().(query.Constant)
	if !ok {
		t.Fatalf("expected Constant in entity position, got %T", pattern.GetE())
	}
	id, ok := constant.Value.(datalog.Identity)
	if !ok {
		t.Fatalf("expected Identity constant, got %T", constant.Value)
	}
	if !id.Equal(datalog.NewIdentity("user:alice")) {
		t.Errorf("#id %q parsed to %s, want NewIdentity(%q)", "user:alice", id, "user:alice")
	}
}

func TestParseIDTaggedLiteralEquivalentToIdentityLiteral(t *testing.T) {
	seed := "user:alice"
	l85 := datalog.NewIdentity(seed).L85()

	qSeed, err := ParseQuery(`[:find ?n :where [#id "` + seed + `" :user/name ?n]]`)
	if err != nil {
		t.Fatalf("ParseQuery(#id) failed: %v", err)
	}
	qHash, err := ParseQuery(`[:find ?n :where [#identity "` + l85 + `" :user/name ?n]]`)
	if err != nil {
		t.Fatalf("ParseQuery(#identity) failed: %v", err)
	}

	idSeed := qSeed.Where[0].(*query.DataPattern).GetE().(query.Constant).Value.(datalog.Identity)
	idHash := qHash.Where[0].(*query.DataPattern).GetE().(query.Constant).Value.(datalog.Identity)
	if !idSeed.Equal(idHash) {
		t.Errorf("#id %q and #identity %q parsed to different identities", seed, l85)
	}
}

func TestParseIDTaggedLiteralRequiresString(t *testing.T) {
	_, err := ParseQuery(`[:find ?n :where [#id 42 :user/name ?n]]`)
	if err == nil {
		t.Fatal("expected an error for #id with non-string value, got none")
	}
	if !strings.Contains(err.Error(), "#id") {
		t.Errorf("error should name the #id literal; got: %v", err)
	}
}
