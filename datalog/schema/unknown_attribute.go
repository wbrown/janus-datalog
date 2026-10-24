package schema

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
)

// UnknownAttributeError reports a query naming an attribute the schema does
// not declare. It carries the attribute as a value, so a caller can repair the
// query from the error's value.
type UnknownAttributeError struct {
	Attribute datalog.Keyword
}

func (e *UnknownAttributeError) Error() string {
	return fmt.Sprintf("unknown attribute %s: the schema does not declare it", e.Attribute)
}
