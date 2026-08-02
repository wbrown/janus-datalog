# Janus Datalog - Makefile

.PHONY: test test-fast test-storage test-wasm test-examples test-hooks bench bench-prebuilt profile clean-testdb build-testdb help

WASM_PATH := $(shell go env GOROOT)/lib/wasm

# Default target
help:
	@echo "Janus Datalog - Available targets:"
	@echo ""
	@echo "  make test           - Native suite + examples + wasm contracts (standard gate)"
	@echo "  make test-fast      - Run tests with short flag (skips slow tests)"
	@echo "  make test-storage   - Run storage tests only (native)"
	@echo "  make test-examples  - Compile-check every examples/ file under its build tag"
	@echo "  make test-wasm      - datalog and tests packages under js/wasm (node runner)"
	@echo "  make test-hooks     - .claude/hooks harness (Tier 2 runs the real claude CLI)"
	@echo "  make bench          - Run all benchmarks"
	@echo "  make bench-prebuilt - Run pre-built database benchmarks"
	@echo "  make profile        - Profile pattern matching with pre-built DB"
	@echo "  make build-testdb   - Build test database (default size)"
	@echo "  make clean-testdb   - Remove test database"
	@echo "  make clean          - Clean all build artifacts"
	@echo ""

# Test targets
test: build-testdb
	go test -count=1 ./...
	$(MAKE) test-examples
	$(MAKE) test-wasm

test-fast:
	go test -short ./...

test-storage: build-testdb
	go test ./datalog/storage/...

# js/wasm via Go's Node runner (GOROOT/lib/wasm/go_js_wasm_exec). Requires node on PATH.
# Test files are portable by default: whatever compiles into the wasm test
# binaries runs, so a misclassified file fails loudly in this leg rather than
# being silently excluded. The !(js && wasm) tag is reserved for files bound
# to native-only surface (direct Badger imports, BadgerStore APIs).
test-wasm:
	@command -v node >/dev/null 2>&1 || { echo "node is required for wasm tests (go_js_wasm_exec)"; exit 1; }
	PATH="$(WASM_PATH):$$PATH" GOOS=js GOARCH=wasm go build ./datalog/...
	PATH="$(WASM_PATH):$$PATH" GOOS=js GOARCH=wasm go test -count=1 ./datalog/... ./tests/...

# Every examples/ file is its own `package main` behind //go:build example, so
# `go vet ./examples/` reports main redeclared and no ordinary build reaches
# them. Vet each file on its own, under the tag, or nothing compiles them at all.
EXAMPLE_FILES := $(wildcard examples/*.go)

test-examples:
	for f in $(EXAMPLE_FILES); do go vet -tags example "$$f" || exit 1; done

# The .claude/hooks harness. Deliberately outside the `make test` gate: Tier 2
# drives the real claude CLI, so the project's gate would otherwise depend on a
# reachable model. run.sh fails loudly when that CLI is absent — the reviewer is
# the thing under test there, so it must not skip.
test-hooks:
	bash .claude/hooks/test/run.sh

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
