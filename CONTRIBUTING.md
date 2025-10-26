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
# Run all tests
go test ./...

# Run tests with verbose output
go test -v ./...

# Run specific package tests
go test ./datalog/executor/

# Run tests with coverage
go test -cover ./...
```

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

1. **[CLAUDE.md](CLAUDE.md)** - Architectural guidance and design patterns
2. **[CLAUDE_TESTING.md](CLAUDE_TESTING.md)** - Testing strategy and requirements
3. **[CLAUDE_BUGS.md](CLAUDE_BUGS.md)** - Historical bugs and patterns to avoid
4. **[CLAUDE_DEBUGGING.md](CLAUDE_DEBUGGING.md)** - Systematic debugging methodology
5. **[ARCHITECTURE.md](ARCHITECTURE.md)** - System architecture overview
6. **[TODO.md](TODO.md)** - Roadmap and current priorities

## Running Tests

### Unit Tests

```bash
# Run all tests
go test ./...

# Run with race detector
go test -race ./...

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

Examples use build tags to avoid `main()` function conflicts:

```bash
# Build a specific example
go build -tags example examples/aggregation_demo.go

# Run directly
go run -tags example examples/aggregation_demo.go

# List available examples
ls examples/*.go
```

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

From [CLAUDE_TESTING.md](CLAUDE_TESTING.md):

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
5. **Ensure all tests pass**: `go test ./...`
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
go build -tags example examples/aggregation_demo.go
```

**Problem:** Import cycle detected
- Check [CLAUDE_BUGS.md](CLAUDE_BUGS.md) for historical import cycle fixes
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

See [CLAUDE_DEBUGGING.md](CLAUDE_DEBUGGING.md) for:
- Systematic debugging approach
- Profiling with pprof
- Memory leak detection
- Query plan analysis

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
