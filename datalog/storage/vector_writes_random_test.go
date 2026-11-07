package storage

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// vectorWritesValues are the values generated writes draw from, few enough
// that a vector often holds a value more than once.
var vectorWritesValues = []string{"a", "b", "c", "d"}

// vectorRandomTargets are the targets a generated run writes: both vector
// attributes of one entity, and the plain vector of a second entity, so a
// transaction interleaves writes of equal values to different (E, A).
func vectorRandomTargets() []vectorTarget {
	first := datalog.NewIdentity("vector-random:first")
	second := datalog.NewIdentity("vector-random:second")
	return []vectorTarget{
		{e: first, attr: vectorWritesCells},
		{e: first, attr: vectorWritesSet},
		{e: second, attr: vectorWritesCells},
	}
}

// generateVectorRun draws one to four transactions of one to eight writes,
// each to a target drawn from targets. It follows each target's vector
// through the reference model, so a Remove can name a value the vector holds
// and a Set can edit the vector it replaces.
func generateVectorRun(r *rand.Rand, targets []vectorTarget) [][]vectorWrite {
	models := make([]vectorModel, len(targets))
	for i, target := range targets {
		models[i].unique = target.attr.unique
	}
	txs := make([][]vectorWrite, 1+r.Intn(4))
	for k := range txs {
		writes := make([]vectorWrite, 1+r.Intn(8))
		for i := range writes {
			target := r.Intn(len(targets))
			op := generateVectorOp(r, targets[target].attr, models[target].values())
			models[target].apply(op)
			writes[i] = vectorWrite{target: target, op: op}
		}
		txs[k] = writes
	}
	return txs
}

// generateVectorOp draws one write: an Add nine times in twenty, a Remove six
// times in twenty, naming a value the vector holds two times in three, and a
// Set or a SaveStruct otherwise.
func generateVectorOp(r *rand.Rand, attr vectorAttr, current []string) vectorOp {
	switch n := r.Intn(20); {
	case n < 9:
		return vecAdd(vectorWritesValues[r.Intn(len(vectorWritesValues))])
	case n < 15:
		if len(current) > 0 && r.Intn(3) != 0 {
			return vecRemove(current[r.Intn(len(current))])
		}
		return vecRemove(vectorWritesValues[r.Intn(len(vectorWritesValues))])
	case n < 18:
		return vecSet(generateVectorValues(r, attr, current)...)
	default:
		return vecSave(generateVectorValues(r, attr, current)...)
	}
}

// generateVectorValues draws the vector a Set or a SaveStruct writes: a prefix
// of current two times in three, then up to two drawn values. On an ordered
// set each value appears once.
func generateVectorValues(r *rand.Rand, attr vectorAttr, current []string) []string {
	var values []string
	if r.Intn(3) != 0 {
		values = append(values, current[:r.Intn(len(current)+1)]...)
	}
	for n := r.Intn(3); n > 0; n-- {
		values = append(values, vectorWritesValues[r.Intn(len(vectorWritesValues))])
	}
	if !attr.unique {
		return values
	}
	var distinct []string
	for _, v := range values {
		if !slices.Contains(distinct, v) {
			distinct = append(distinct, v)
		}
	}
	return distinct
}

// TestVectorWritesOnRandomRunsReadBackAsTheReference writes generated
// transactions to three vectors, two attributes of one entity and one of a
// second, and reads every vector back through every reader after each commit
// and as of each once all have committed, on both optimizer paths with the
// cache on and off. Every reader answers the vector the reference model
// computes.
func TestVectorWritesOnRandomRunsReadBackAsTheReference(t *testing.T) {
	s := vectorWritesSchema(t)
	targets := vectorRandomTargets()
	for seed := int64(1); seed <= 20; seed++ {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			txs := generateVectorRun(rand.New(rand.NewSource(seed)), targets)
			want := modelAnswers(targets, txs)
			t.Logf("transactions %v, vectors %q", txs, want)
			for _, mode := range optimizerModes {
				for _, cache := range vectorWritesCaches {
					t.Run(mode.name+"/"+cache.name, func(t *testing.T) {
						d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s, DisableCache: cache.disable})
						checkVectorRun(t, d, targets, txs, want)
					})
				}
			}
		})
	}
}
