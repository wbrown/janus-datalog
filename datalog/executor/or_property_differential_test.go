package executor_test

import (
	"fmt"
	"testing"
)

func TestOrPropertyClassifierRandomized(t *testing.T) {
	for _, seed := range []int64{0x5eed, 0x5eee, 0x5eef, 0x5ef0, 0x5ef1, 0x5ef2, 0x5ef3, 0x5ef4} {
		t.Run(fmt.Sprintf("seed_%x", seed), func(t *testing.T) {
			runOrPropertyDifferential(t, seed)
		})
	}
}
