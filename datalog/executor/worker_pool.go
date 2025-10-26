package executor

import (
	"fmt"
	"runtime"
	"sync"
)

// WorkerPool provides generic parallel execution with worker pool
// This is intentionally generic so it can be reused for:
// - Parallel subquery execution
// - Parallel pattern matching (future)
// - Parallel aggregations (future)
// - Any embarrassingly parallel operation
type WorkerPool struct {
	workerCount int
}

// NewWorkerPool creates a new worker pool
// workerCount: number of worker goroutines (0 = use NumCPU)
func NewWorkerPool(workerCount int) *WorkerPool {
	if workerCount <= 0 {
		workerCount = runtime.NumCPU()
	}
	return &WorkerPool{
		workerCount: workerCount,
	}
}

// ExecuteParallel executes operation on all inputs using worker pool
// Results are returned in the same order as inputs (order-preserving).
//
// Parameters:
// - ctx: Execution context (can be checked for cancellation)
// - inputs: Slice of inputs to process
// - operation: Function to execute on each input
//
// Returns: Results in same order as inputs, or error from first failure
//
// Note: Uses interface{} for maximum flexibility. Callers cast as needed.
func (p *WorkerPool) ExecuteParallel(
	ctx Context,
	inputs []interface{},
	operation func(Context, interface{}) (interface{}, error),
) ([]interface{}, error) {
	if len(inputs) == 0 {
		return []interface{}{}, nil
	}

	// Create result slices with same length as inputs
	results := make([]interface{}, len(inputs))
	errors := make([]error, len(inputs))

	// Create job channel
	jobs := make(chan int, len(inputs))

	// Worker pool
	var wg sync.WaitGroup
	for w := 0; w < p.workerCount; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				// Execute operation
				result, err := operation(ctx, inputs[idx])
				results[idx] = result
				errors[idx] = err
			}
		}()
	}

	// Enqueue all jobs
	for i := range inputs {
		jobs <- i
	}
	close(jobs)

	// Wait for completion
	wg.Wait()

	// Check for errors (return first error found)
	for i, err := range errors {
		if err != nil {
			return nil, fmt.Errorf("parallel execution failed at index %d: %w", i, err)
		}
	}

	return results, nil
}

// ExecuteParallelBatched executes operation on inputs in batches
// This is useful when inputs are large and you want to process them in chunks
// to balance memory usage and parallelism.
//
// Parameters:
// - ctx: Execution context
// - inputs: Slice of inputs to process
// - batchSize: Number of inputs to process per batch
// - operation: Function to execute on each batch
//
// Returns: Results from all batches concatenated, or error from first failure
func (p *WorkerPool) ExecuteParallelBatched(
	ctx Context,
	inputs []interface{},
	batchSize int,
	operation func(Context, []interface{}) ([]interface{}, error),
) ([]interface{}, error) {
	if len(inputs) == 0 {
		return []interface{}{}, nil
	}

	if batchSize <= 0 {
		batchSize = 100 // Default batch size
	}

	// Split inputs into batches
	var batches [][]interface{}
	for i := 0; i < len(inputs); i += batchSize {
		end := i + batchSize
		if end > len(inputs) {
			end = len(inputs)
		}
		batches = append(batches, inputs[i:end])
	}

	// Execute batches in parallel
	batchResults := make([][]interface{}, len(batches))
	batchErrors := make([]error, len(batches))

	jobs := make(chan int, len(batches))
	var wg sync.WaitGroup

	for w := 0; w < p.workerCount; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				// Execute operation on batch
				result, err := operation(ctx, batches[idx])
				batchResults[idx] = result
				batchErrors[idx] = err
			}
		}()
	}

	// Enqueue all batch jobs
	for i := range batches {
		jobs <- i
	}
	close(jobs)

	// Wait for completion
	wg.Wait()

	// Check for errors
	for i, err := range batchErrors {
		if err != nil {
			return nil, fmt.Errorf("parallel batch execution failed at batch %d: %w", i, err)
		}
	}

	// Concatenate all batch results
	var allResults []interface{}
	for _, batchResult := range batchResults {
		allResults = append(allResults, batchResult...)
	}

	return allResults, nil
}

// GetWorkerCount returns the number of worker goroutines
func (p *WorkerPool) GetWorkerCount() int {
	return p.workerCount
}
