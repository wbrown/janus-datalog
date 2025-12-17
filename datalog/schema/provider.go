package schema

import (
	"github.com/wbrown/janus-datalog/datalog"
)

// SchemaProvider provides read-only access to schema information
// This interface is consumed by Transaction validation, Pull API, etc.
type SchemaProvider interface {
	// GetAttribute returns the definition for an attribute, or nil if unknown
	GetAttribute(attr datalog.Keyword) *AttributeDefinition

	// HasSchema returns true if a schema is available
	HasSchema() bool

	// IsRef returns true if the attribute is a reference type
	IsRef(attr datalog.Keyword) bool

	// IsMany returns true if the attribute has cardinality many
	IsMany(attr datalog.Keyword) bool
}

// Verify Schema implements SchemaProvider at compile time
var _ SchemaProvider = (*Schema)(nil)
