package executor

// Re-export constraint types from the constraints package for backward compatibility
import "github.com/wbrown/janus-datalog/datalog/constraints"

type TimeRangeConstraint = constraints.TimeRangeConstraint

var ComposeTimeConstraint = constraints.ComposeTimeConstraint
