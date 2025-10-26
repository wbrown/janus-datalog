package datalog

import (
	"time"
)

// Value represents any value that can be stored in a Datom
// Just like C++ uses boost::variant with direct types,
// we use interface{} with direct Go types
type Value interface{}

// Valid value types:
// - string
// - int64
// - float64
// - bool
// - time.Time
// - []byte
// - Identity (for references to other entities)
// - Keyword (when used as a value, e.g., storing :status/active)

// Reference is an alias for Identity when used as a value
// This makes it clear when we're storing an entity reference
type Reference = Identity

// Helper functions for creating typed values
func String(s string) Value        { return s }
func Int(i int64) Value            { return i }
func Float(f float64) Value        { return f }
func Bool(b bool) Value            { return b }
func Time(t time.Time) Value       { return t }
func Bytes(b []byte) Value         { return b }
func Ref(id Identity) Value        { return Reference(id) }
func KeywordValue(k Keyword) Value { return k }
