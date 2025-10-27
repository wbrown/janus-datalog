# Architectural Decision Records

This directory contains records of significant architectural decisions made during the development of the Janus Datalog engine.

## What Goes Here

Documents in this directory should record:
- **Trade-offs** between different implementation approaches
- **Deferred decisions** that need future review
- **Policy choices** that affect multiple components
- **Compromises** between competing concerns (performance, correctness, usability)

## Format

Each decision record should include:
- **Date**: When the decision was made
- **Status**: Implemented, Proposed, Deferred, Superseded
- **Context**: What problem/situation led to this decision
- **Decision**: What was chosen and why
- **Implications**: What this means for the codebase
- **Future Review**: Conditions under which this should be reconsidered

## Current Decisions

- [**UNCORRELATED_SUBQUERY_CARTESIAN_PRODUCTS.md**](UNCORRELATED_SUBQUERY_CARTESIAN_PRODUCTS.md) - Decision to support uncorrelated subqueries via Cartesian products despite general policy against them (October 2025)

## Related Documentation

- `../STREAMING_ARCHITECTURE_DECISION.md` - Streaming implementation and configuration approach
- `../proposals/` - Future feature proposals
- `../archive/completed/` - Completed implementation plans
