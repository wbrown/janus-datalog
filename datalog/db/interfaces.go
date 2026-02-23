package db

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// EntityReader provides typed attribute access for entities.
type EntityReader interface {
	GetString(e datalog.Identity, a datalog.Keyword) (string, bool, error)
	GetInt(e datalog.Identity, a datalog.Keyword) (int64, bool, error)
	GetFloat(e datalog.Identity, a datalog.Keyword) (float64, bool, error)
	GetBool(e datalog.Identity, a datalog.Keyword) (bool, bool, error)
	GetRef(e datalog.Identity, a datalog.Keyword) (datalog.Identity, bool, error)
	GetStrings(e datalog.Identity, a datalog.Keyword) ([]string, error)
}

// Querier provides the core query and entity access operations.
type Querier interface {
	Query(q any, inputs ...any) (executor.Relation, error)
	QueryInto(dest any, q any, inputs ...any) error
	QueryOneInto(dest any, q any, inputs ...any) (bool, error)
	PullInto(entityID datalog.Identity, v any) error
	NewTransaction() *Transaction
	EntityReader
}

var _ Querier = (*DB)(nil)
