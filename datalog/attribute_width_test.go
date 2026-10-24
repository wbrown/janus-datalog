package datalog

import (
	"fmt"
	"strings"
	"testing"
)

// An attribute keyword fits storage when its string form is at most
// MaxAttributeBytes long. One byte past it is refused with the one message
// every boundary states, naming the keyword, its length and the limit.
func TestValidateAttributeWidth_RefusesPastTheLimitWithOneMessage(t *testing.T) {
	atLimit := NewKeyword(":t/" + strings.Repeat("x", MaxAttributeBytes-len(":t/")))
	if n := len(atLimit.String()); n != MaxAttributeBytes {
		t.Fatalf("the fixture is %d bytes, not %d", n, MaxAttributeBytes)
	}
	if err := ValidateAttributeWidth(atLimit); err != nil {
		t.Fatalf("a keyword at the limit fits; got %v", err)
	}

	past := NewKeyword(atLimit.String() + "y")
	err := ValidateAttributeWidth(past)
	if err == nil {
		t.Fatal("a keyword one byte past the limit is refused")
	}
	want := fmt.Sprintf("attribute %s is %d bytes, exceeds the %d-byte storage limit", past, MaxAttributeBytes+1, MaxAttributeBytes)
	if err.Error() != want {
		t.Fatalf("the refusal is the one message\n got: %s\nwant: %s", err, want)
	}
}
