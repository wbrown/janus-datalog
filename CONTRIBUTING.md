# Contributing to Janus Datalog

Thank you for your interest in contributing to Janus Datalog! This document provides guidelines and instructions for contributing to the project.

## Table of Contents

- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Running Tests](#running-tests)
- [Running Examples](#running-examples)
- [Code Style](#code-style)
- [Testing Requirements](#testing-requirements)
- [Architectural Guidelines](#architectural-guidelines)
- [Submitting Changes](#submitting-changes)
- [Common Issues](#common-issues)

## Getting Started

### Prerequisites

- Go 1.21 or higher
- Git
- Basic understanding of Datalog queries (see README.md for introduction)

### Clone and Build

```bash
git clone https://github.com/wbrown/janus-datalog.git
cd janus-datalog
go build ./...
```

### Run Tests

```bash
# The full gate: builds the test database, then runs the native suite,
# the examples/ compile check, and the js/wasm contracts
make test

# Run specific package tests (focused iteration only)
go test ./datalog/executor/

# Run tests with verbose output
go test -v ./datalog/executor/

# Run tests with coverage
go test -cover ./...
```

`make test` is the gate. Bare `go test ./...` is not a full green: it skips the
`examples/` compile check and the js/wasm contracts, and it does not build the
test database that storage tests read.

## Development Setup

### Project Structure

```
janus-datalog/
├── datalog/           # Core engine packages
│   ├── executor/      # Query execution
│   ├── planner/       # Query planning
│   ├── parser/        # EDN and query parsing
│   ├── storage/       # BadgerDB backend
│   └── ...
├── examples/          # Example programs (build tags required)
├── cmd/               # Command-line tools
├── docs/              # Documentation
└── tests/             # Integration tests
```

### Key Documentation Files

Before making significant changes, please read:

1. **[CLAUDE.md](CLAUDE.md)** - Architectural guidance and design patterns. Its
   later sections carry the testing strategy, the catalog of historical bugs and
   the patterns that produced them, and the debugging methodology for query
   execution.
2. **[ARCHITECTURE.md](ARCHITECTURE.md)** - System architecture overview
3. **[TODO.md](TODO.md)** - Roadmap and current priorities

## Running Tests

### Unit Tests

```bash
# The full gate
make test

# Run with coverage report
go test -cover ./... -coverprofile=coverage.out
go tool cover -html=coverage.out
```

### Benchmarks

```bash
# Run all benchmarks
go test -bench=. ./...

# Run specific benchmark
go test -bench=BenchmarkOHLCQuery ./datalog/storage/

# Run with memory profiling
go test -bench=BenchmarkOHLCQuery -memprofile=mem.prof ./datalog/storage/
go tool pprof mem.prof
```

### Integration Tests

```bash
# Run integration tests (in tests/ directory)
go test ./tests/
```

## Running Examples

Examples use build tags and must be run from the repository root:

```bash
# Run an example
go run -tags example examples/getting_started.go
```

See [`examples/README.md`](examples/README.md) for the full list.

**Note:** You cannot run `go build ./examples` due to multiple main functions. Build examples individually.

## Code Style

### Go Conventions

- Follow standard Go conventions and idioms
- Run `go fmt` before committing
- Run `go vet` to catch common mistakes
- Use `goimports` to organize imports

```bash
# Format code
go fmt ./...

# Vet code
go vet ./...

# Install and run goimports
go install golang.org/x/tools/cmd/goimports@latest
goimports -w .
```

### Project-Specific Guidelines

From [CLAUDE.md](CLAUDE.md):

1. **No global configuration state** - Thread options through constructors
2. **No V2 versions** - Fix the original implementation
3. **Write idiomatic Go** - Not Java-in-Go
4. **Avoid unnecessary abstractions** - Simple functions over manager classes
5. **Return errors explicitly** - No panic for expected errors

### Naming Conventions

- **Relations**: Use `Relation` interface, avoid `Result` or `Binding`
- **Iterators**: Implement `Iterator` interface for streaming
- **Options**: Use `*Options` structs, not global flags
- **Tests**: Use `TestFunctionName` and table-driven tests

## Testing Requirements

From the Testing Strategy section of [CLAUDE.md](CLAUDE.md):

### When to Write Tests

**Required:**
- All new features must have tests
- Bug fixes must include regression tests
- Public API changes require integration tests

**Recommended:**
- Edge cases and error conditions
- Performance benchmarks for critical paths
- Examples demonstrating usage

### Test Organization

```go
func TestFeatureName(t *testing.T) {
    tests := []struct {
        name     string
        input    interface{}
        expected interface{}
    }{
        {"description", input, expected},
        // ... more cases
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            // Test implementation
        })
    }
}
```

### Test Coverage Goals

- Core packages: >80% coverage
- Critical paths (query execution, joins): >90% coverage
- New features: 100% coverage of happy path + error cases

## Architectural Guidelines

### Key Principles

From [CLAUDE.md](CLAUDE.md), these are **hard requirements**:

1. **Options over globals** - Configuration flows through constructors
2. **Relations over bindings** - Multi-value variable support
3. **Streaming over materialization** - Use iterators when possible
4. **Explicit errors** - Return error types, don't panic
5. **Simple over clever** - Readable code beats optimization

### Before Making Architectural Changes

If your change involves:
- Introducing new patterns (globals, managers, abstractions)
- Changing existing patterns (Options → globals, Relations → Bindings)
- Adding cross-cutting concerns (configuration, logging, caching)
- Deviating from established conventions

**→ Open an issue to discuss first**

### Performance Considerations

See [PERFORMANCE_STATUS.md](PERFORMANCE_STATUS.md) for:
- Active optimizations (streaming, parallel execution, predicate pushdown)
- Measured performance characteristics
- Benchmarking methodology

## Submitting Changes

### Workflow

1. **Fork the repository** on GitHub
2. **Create a feature branch** from `main`:
   ```bash
   git checkout -b feature/your-feature-name
   ```
3. **Make your changes** following the guidelines above
4. **Write tests** for your changes
5. **Ensure all tests pass**: `make test`
6. **Run code quality checks**:
   ```bash
   go fmt ./...
   go vet ./...
   ```
7. **Commit with clear messages**:
   ```bash
   git commit -m "feat: Add feature description"
   ```
8. **Push to your fork**:
   ```bash
   git push origin feature/your-feature-name
   ```
9. **Open a Pull Request** on GitHub

### Commit Message Guidelines

Use conventional commits format:

- `feat: Add new feature`
- `fix: Fix bug description`
- `docs: Update documentation`
- `test: Add tests for X`
- `perf: Improve performance of Y`
- `refactor: Restructure Z`
- `chore: Update dependencies`

### Pull Request Guidelines

- Link related issues in the PR description
- Describe what changed and why
- Include test results if applicable
- Keep PRs focused - one feature/fix per PR
- Respond to review feedback promptly

## Common Issues

### Build Errors

**Problem:** `multiple main functions` error in examples/
```bash
# Don't do this:
go build ./examples

# Do this instead:
go build -tags example examples/getting_started.go
```

**Problem:** Import cycle detected
- Check the Bug Fixes and Learnings section of [CLAUDE.md](CLAUDE.md) for
  historical import cycle fixes
- Reorganize packages to break the cycle

### Test Failures

**Problem:** Flaky tests
- Check for race conditions: `go test -race`
- Look for uninitialized state
- Ensure tests are independent

**Problem:** Benchmark variance
- Run multiple iterations: `-benchtime=10s`
- Check for background processes
- Use consistent test data

### Performance Issues

See the Debugging Query Execution Issues section of [CLAUDE.md](CLAUDE.md) for
the systematic approach: reading annotations first, examining phase structure,
tracing data flow between phases, and verifying phase invariants. Profile before
theorizing — `go test -cpuprofile` plus `go tool pprof`, never a guess about
what is slow.

## Getting Help

- **Documentation**: Start with [README.md](README.md) and [ARCHITECTURE.md](ARCHITECTURE.md)
- **Issues**: Search existing issues or open a new one
- **Discussions**: Use GitHub Discussions for questions
- **Examples**: Look at `examples/` directory for usage patterns

## License

By contributing to Janus Datalog, you agree that your contributions will be licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.

## Recognition

Contributors are recognized in:
- Git commit history
- Release notes
- Project documentation (for significant contributions)

Thank you for contributing to Janus Datalog! 🎉
