# Janus Datalog - Makefile

.PHONY: test test-fast test-storage bench bench-prebuilt profile clean-testdb build-testdb help

# Default target
help:
	@echo "Janus Datalog - Available targets:"
	@echo ""
	@echo "  make test           - Run all tests (auto-builds test DB if needed)"
	@echo "  make test-fast      - Run tests with short flag (skips slow tests)"
	@echo "  make test-storage   - Run storage tests only"
	@echo "  make bench          - Run all benchmarks"
	@echo "  make bench-prebuilt - Run pre-built database benchmarks"
	@echo "  make profile        - Profile pattern matching with pre-built DB"
	@echo "  make build-testdb   - Build test database (default size)"
	@echo "  make clean-testdb   - Remove test database"
	@echo "  make clean          - Clean all build artifacts"
	@echo ""

# Test targets
test: build-testdb
	go test ./...

test-fast:
	go test -short ./...

test-storage: build-testdb
	go test ./datalog/storage/...

# Benchmark targets
bench: build-testdb
	go test -bench=. -benchmem ./...

bench-prebuilt: build-testdb
	go test -bench=BenchmarkPrebuiltDatabase -benchmem ./datalog/storage

# Profiling targets
profile: build-testdb
	@echo "Running CPU profile on pattern matching benchmarks..."
	go test -bench=BenchmarkPrebuiltDatabase_PatternMatching \
		-cpuprofile=cpu.prof \
		-memprofile=mem.prof \
		-benchtime=5s \
		./datalog/storage
	@echo ""
	@echo "Profile complete! View with:"
	@echo "  go tool pprof -http=:8080 cpu.prof"
	@echo "  go tool pprof -http=:8080 -alloc_space mem.prof"

# Test database management
build-testdb:
	@if [ ! -d "datalog/storage/testdata/ohlc_benchmark.db" ]; then \
		echo "Building test database..."; \
		go run cmd/build-testdb/main.go -config=default; \
	else \
		echo "✅ Test database already exists"; \
	fi

build-testdb-force:
	@echo "Force rebuilding test database..."
	@rm -rf datalog/storage/testdata/ohlc_benchmark.db
	go run cmd/build-testdb/main.go -config=default

build-testdb-medium:
	@echo "Building medium test database..."
	go run cmd/build-testdb/main.go -config=medium

clean-testdb:
	@echo "Removing test database..."
	rm -rf datalog/storage/testdata/*.db

# Clean targets
clean: clean-testdb
	@echo "Cleaning build artifacts..."
	rm -f *.prof
	rm -f cpu.prof mem.prof
	rm -f datalog/storage/*.prof
	go clean -testcache
	@echo "✅ Clean complete"

# Development helpers
.PHONY: watch-tests
watch-tests:
	@echo "Watching for changes (requires entr)..."
	@find . -name '*.go' | entr -c make test-fast
