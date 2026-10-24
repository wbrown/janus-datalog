package schema

import (
	"fmt"
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// overlongIdent is an ident one byte longer than storage holds.
func overlongIdent() string {
	return ":t/" + strings.Repeat("x", datalog.MaxAttributeBytes-len(":t/")+1)
}

// Add panics on an ident longer than storage holds, as it panics on a keyword
// from the wrong vocabulary: a definition is constructed there, and a longer
// ident would alias every attribute sharing its first bytes.
func TestAddPanicsOnAnIdentLongerThanStorable(t *testing.T) {
	ident := overlongIdent()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("Add accepted an ident longer than storage holds")
		}
		if !strings.Contains(fmt.Sprint(r), ident) {
			t.Fatalf("the panic names the ident; got %v", r)
		}
	}()
	NewSchema().Add(&AttributeDefinition{Ident: datalog.NewKeyword(ident), ValueType: TypeString})
}

// ParseSchema returns an error naming an ident longer than storage holds.
func TestParseSchemaRefusesAnIdentLongerThanStorable(t *testing.T) {
	ident := overlongIdent()
	_, err := ParseSchema("{" + ident + " {:db/valueType :db.type/string}}")
	if err == nil {
		t.Fatal("ParseSchema accepted an ident longer than storage holds")
	}
	if !strings.Contains(err.Error(), ident) {
		t.Fatalf("the error names the ident; got %v", err)
	}
}
