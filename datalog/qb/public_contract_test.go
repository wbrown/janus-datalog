package qb_test

import "github.com/wbrown/janus-datalog/datalog/qb"

var (
	_ func(interface{}, ...interface{}) *qb.ArithBuilder = qb.Add
	_ func(interface{}, ...interface{}) *qb.ArithBuilder = qb.Sub
	_ func(interface{}, ...interface{}) *qb.ArithBuilder = qb.Mul
	_ func(interface{}, ...interface{}) *qb.ArithBuilder = qb.Div

	_ = qb.Add(int64(1))
	_ = qb.Sub(int64(1), int64(2))
	_ = qb.Mul(int64(1), int64(2), int64(3))
	_ = qb.Div(int64(8), int64(2), int64(2))
)
