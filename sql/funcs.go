package sql

import (
	"fmt"
	"math/rand"
	"strconv"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

// you can use this to sample records
type RandFilter struct{}

func (m *RandFilter) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 arg for RandFilter(sampleRate) but got %s", n)
	}

	return func(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
		if args[0] == nil || args[0].Err() || args[0].Nil() {
			return value.BoolValueFalse, true
		}

		sampleRate, err := strconv.ParseFloat(args[0].ToString(), 32)
		if err != nil {
			return value.BoolValueFalse, true
		}

		if sampleRate <= 0 || sampleRate >= 1 {
			return value.BoolValueFalse, true
		}

		if rand.Float64() < sampleRate {
			return value.BoolValueTrue, true
		}
		return value.BoolValueFalse, true
	}, nil
}
func (m *RandFilter) Type() value.ValueType { return value.BoolType }
