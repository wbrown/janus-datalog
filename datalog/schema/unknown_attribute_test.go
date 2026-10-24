package schema

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// An UnknownAttributeError carries the attribute as a value, and its text
// states the condition it reports.
func TestUnknownAttributeErrorText(t *testing.T) {
	err := &UnknownAttributeError{Attribute: kw(":person/naem")}
	require.Equal(t,
		"unknown attribute :person/naem: the schema does not declare it",
		err.Error())

	wrapped := fmt.Errorf("query execution failed: %w", err)
	var got *UnknownAttributeError
	require.True(t, errors.As(wrapped, &got))
	require.Same(t, kw(":person/naem"), got.Attribute)
}
