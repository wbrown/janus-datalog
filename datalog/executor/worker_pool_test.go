package executor

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerPool_OrderPreserving(t *testing.T) {
	executor := NewWorkerPool(4)

	// Create inputs
	inputs := make([]interface{}, 100)
	for i := 0; i < 100; i++ {
		inputs[i] = i
	}

	// Operation: double the value
	operation := func(ctx Context, input interface{}) (interface{}, error) {
		val := input.(int)
		return val * 2, nil
	}

	ctx := NewContext(nil)
	results, err := executor.ExecuteParallel(ctx, inputs, operation)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Verify results are in correct order
	if len(results) != 100 {
		t.Fatalf("Expected 100 results, got %d", len(results))
	}

	for i, result := range results {
		expected := i * 2
		if result.(int) != expected {
			t.Errorf("Result %d: expected %d, got %d", i, expected, result.(int))
		}
	}
}

func TestWorkerPool_ErrorHandling(t *testing.T) {
	executor := NewWorkerPool(4)

	inputs := make([]interface{}, 10)
	for i := 0; i < 10; i++ {
		inputs[i] = i
	}

	// Operation that fails on index 5
	operation := func(ctx Context, input interface{}) (interface{}, error) {
		val := input.(int)
		if val == 5 {
			return nil, fmt.Errorf("intentional error at %d", val)
		}
		return val * 2, nil
	}

	ctx := NewContext(nil)
	results, err := executor.ExecuteParallel(ctx, inputs, operation)

	// Should return error
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	// Results should be nil on error
	if results != nil {
		t.Errorf("Expected nil results on error, got %v", results)
	}

	// Error should mention the failing index
	if err.Error() != "parallel execution failed at index 5: intentional error at 5" {
		t.Errorf("Unexpected error message: %v", err)
	}
}

func TestWorkerPool_EmptyInput(t *testing.T) {
	executor := NewWorkerPool(4)

	operation := func(ctx Context, input interface{}) (interface{}, error) {
		return input, nil
	}

	ctx := NewContext(nil)
	results, err := executor.ExecuteParallel(ctx, []interface{}{}, operation)

	if err != nil {
		t.Fatalf("Expected no error for empty input, got %v", err)
	}

	if len(results) != 0 {
		t.Errorf("Expected 0 results, got %d", len(results))
	}
}

func TestWorkerPool_WorkerCount(t *testing.T) {
	tests := []struct {
		name          string
		workerCount   int
		expectedCount int
	}{
		{
			name:          "explicit_count",
			workerCount:   8,
			expectedCount: 8,
		},
		{
			name:          "zero_uses_default",
			workerCount:   0,
			expectedCount: runtime.NumCPU(),
		},
		{
			name:          "negative_uses_default",
			workerCount:   -5,
			expectedCount: runtime.NumCPU(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executor := NewWorkerPool(tt.workerCount)
			if executor.GetWorkerCount() != tt.expectedCount {
				t.Errorf("Expected %d workers, got %d", tt.expectedCount, executor.GetWorkerCount())
			}
		})
	}
}

func TestWorkerPool_ConcurrentExecution(t *testing.T) {
	executor := NewWorkerPool(8)

	// Track concurrent executions
	var maxConcurrent int32
	var currentConcurrent int32

	inputs := make([]interface{}, 20)
	for i := 0; i < 20; i++ {
		inputs[i] = i
	}

	operation := func(ctx Context, input interface{}) (interface{}, error) {
		// Increment concurrent counter
		current := atomic.AddInt32(&currentConcurrent, 1)

		// Update max if needed
		for {
			max := atomic.LoadInt32(&maxConcurrent)
			if current <= max || atomic.CompareAndSwapInt32(&maxConcurrent, max, current) {
				break
			}
		}

		// Simulate work
		time.Sleep(10 * time.Millisecond)

		// Decrement concurrent counter
		atomic.AddInt32(&currentConcurrent, -1)

		return input, nil
	}

	ctx := NewContext(nil)
	_, err := executor.ExecuteParallel(ctx, inputs, operation)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Verify we had concurrent execution
	max := atomic.LoadInt32(&maxConcurrent)
	if max < 2 {
		t.Errorf("Expected concurrent execution (max >= 2), got max = %d", max)
	}

	t.Logf("Max concurrent executions: %d", max)
}

// TestWorkerPool_ContextCancellation is disabled because Context doesn't support cancellation yet
/*
func TestWorkerPool_ContextCancellation(t *testing.T) {
	executor := NewWorkerPool(4)

	inputs := make([]interface{}, 100)
	for i := 0; i < 100; i++ {
		inputs[i] = i
	}

	// Create context and cancel after processing a few
	ctx := NewContext(nil)
	processed := int32(0)

	operation := func(ctx Context, input interface{}) (interface{}, error) {
		atomic.AddInt32(&processed, 1)
		// Simulate some work
		time.Sleep(5 * time.Millisecond)
		return input, nil
	}

	// Cancel context after 50ms
	go func() {
		time.Sleep(50 * time.Millisecond)
		ctx.Cancel()
	}()

	_, err := executor.ExecuteParallel(ctx, inputs, operation)

	// Should get error due to cancellation
	if err == nil {
		t.Error("Expected error due to cancellation, got nil")
	}

	t.Logf("Processed %d items before cancellation", atomic.LoadInt32(&processed))
}
*/

func TestWorkerPool_Batched(t *testing.T) {
	executor := NewWorkerPool(4)

	// Create 50 inputs
	inputs := make([]interface{}, 50)
	for i := 0; i < 50; i++ {
		inputs[i] = i
	}

	// Operation that processes a batch and returns doubled values
	batchOp := func(ctx Context, batch []interface{}) ([]interface{}, error) {
		results := make([]interface{}, len(batch))
		for i, input := range batch {
			results[i] = input.(int) * 2
		}
		return results, nil
	}

	ctx := NewContext(nil)
	results, err := executor.ExecuteParallelBatched(ctx, inputs, 10, batchOp)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Verify all results
	if len(results) != 50 {
		t.Fatalf("Expected 50 results, got %d", len(results))
	}

	// Note: Results may not be in order with batched execution
	// Collect all results in a map
	resultMap := make(map[int]bool)
	for _, result := range results {
		resultMap[result.(int)] = true
	}

	// Verify all expected results are present
	for i := 0; i < 50; i++ {
		expected := i * 2
		if !resultMap[expected] {
			t.Errorf("Missing result %d", expected)
		}
	}
}

func TestWorkerPool_Batched_EmptyInput(t *testing.T) {
	executor := NewWorkerPool(4)

	batchOp := func(ctx Context, batch []interface{}) ([]interface{}, error) {
		return batch, nil
	}

	ctx := NewContext(nil)
	results, err := executor.ExecuteParallelBatched(ctx, []interface{}{}, 10, batchOp)

	if err != nil {
		t.Fatalf("Expected no error for empty input, got %v", err)
	}

	if len(results) != 0 {
		t.Errorf("Expected 0 results, got %d", len(results))
	}
}

func TestWorkerPool_Batched_ErrorHandling(t *testing.T) {
	executor := NewWorkerPool(4)

	inputs := make([]interface{}, 30)
	for i := 0; i < 30; i++ {
		inputs[i] = i
	}

	// Operation that fails on second batch (indices 10-19)
	batchOp := func(ctx Context, batch []interface{}) ([]interface{}, error) {
		// Check if batch contains value 15
		for _, input := range batch {
			if input.(int) == 15 {
				return nil, fmt.Errorf("intentional batch error at value 15")
			}
		}
		return batch, nil
	}

	ctx := NewContext(nil)
	results, err := executor.ExecuteParallelBatched(ctx, inputs, 10, batchOp)

	// Should return error
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	// Results should be nil on error
	if results != nil {
		t.Errorf("Expected nil results on error, got %v", results)
	}
}

func TestWorkerPool_Batched_DefaultBatchSize(t *testing.T) {
	executor := NewWorkerPool(2)

	inputs := make([]interface{}, 10)
	for i := 0; i < 10; i++ {
		inputs[i] = i
	}

	batchCount := 0
	batchOp := func(ctx Context, batch []interface{}) ([]interface{}, error) {
		batchCount++
		return batch, nil
	}

	ctx := NewContext(nil)
	_, err := executor.ExecuteParallelBatched(ctx, inputs, 0, batchOp) // 0 = use default batch size

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// With 10 inputs and default batch size 100, should be 1 batch
	if batchCount != 1 {
		t.Errorf("Expected 1 batch with default size, got %d", batchCount)
	}
}
