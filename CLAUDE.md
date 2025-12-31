# pgtofu Development Guide

## Quick Reference

```bash
go build -o /tmp/pgtofu ./cmd/pgtofu    # Build
go test ./...                           # Test all
go test -short ./...                    # Skip DB tests
golangci-lint run                       # Lint
```

## Architecture

Four-phase pipeline converting database state to migrations:

```
PostgreSQL ──→ Extractor ──→ JSON Schema (current)
                                   ↓
SQL Files ──→ Parser ──→ JSON Schema (desired) ──→ Differ ──→ Changes ──→ Generator ──→ Migrations
```

## Package Structure

```
cmd/pgtofu/main.go          # CLI entry point

internal/
├── cli/                    # Cobra commands (extract, diff, generate, partition)
├── schema/                 # Data models (Database, Table, Column, etc.)
├── extractor/              # PostgreSQL → JSON (queries pg_catalog)
├── parser/                 # SQL files → JSON (lexer-based, not regex)
├── differ/                 # Schema comparison, change detection
├── generator/              # Changes → migration SQL files
├── graph/                  # Topological sort (Kahn's algorithm)
└── util/                   # Error wrapping

pkg/database/               # pgx connection pool
```

## Key Files

| File | Purpose |
|------|---------|
| `internal/schema/schema.go` | Root `Database` struct with all object types |
| `internal/differ/types.go` | `Change` struct and `ChangeType` constants |
| `internal/differ/dependency.go` | Topological sorting for change ordering |
| `internal/generator/ddl_registry.go` | Maps change types to DDL builders |
| `internal/parser/table.go` | CREATE TABLE parsing (complex) |
| `internal/parser/lexer.go` | SQL tokenization |

## Core Types

```go
// internal/schema/schema.go - Root container
type Database struct {
    Tables, Views, MaterializedViews, Functions, Triggers,
    Sequences, Extensions, CustomTypes []...
    Hypertables, ContinuousAggregates []...  // TimescaleDB
}

// internal/differ/types.go - Schema change
type Change struct {
    Type        ChangeType      // ADD_TABLE, DROP_COLUMN, etc.
    Severity    ChangeSeverity  // SAFE, POTENTIALLY_BREAKING, BREAKING
    ObjectType  string          // "table", "column", "index"
    ObjectName  string          // Qualified name (schema.name)
    Details     map[string]any
    DependsOn   []string
}
```

## Common Tasks

### Add Support for a New PostgreSQL Object Type

1. **Define model** in `internal/schema/newobject.go`
2. **Add to Database struct** in `internal/schema/schema.go`
3. **Extract from PostgreSQL** in `internal/extractor/`:
   - Query in `queries.go`
   - Extraction in `newobject.go`
   - Call from `Extract()` in `extractor.go`
4. **Parse from SQL** in `internal/parser/`:
   - Statement detection in `statement.go`
   - Parser in `newobject.go`
5. **Compare** in `internal/differ/newobject_comparator.go`
6. **Generate DDL** in `internal/generator/`:
   - Change types in `types.go`
   - Builders in `ddl_registry.go` and `ddl_newobject.go`
7. **Test** in each package's `tests/` subdirectory

### Add a New Change Type

1. Add constant in `internal/differ/types.go`
2. Set severity in `changeSeverities` map
3. Detect in comparator, create `Change` with type
4. Register builder in `internal/generator/ddl_registry.go`
5. Implement builder function

### Fix Parser Bug

1. Check `internal/parser/lexer.go` for tokenization issues
2. Check `internal/parser/splitter.go` for statement boundaries
3. Common issues: dollar-quotes, nested parentheses, quoted identifiers

### Fix Differ Bug

1. Check qualified name generation in comparator
2. Check `internal/differ/view_normalizer.go` for view comparison
3. Dependency cycles show remaining nodes in error message

### Fix Generator Bug

1. Verify builder is registered in `ddl_registry.go`
2. Check SQL template in `templates.go`
3. Check transaction mode handling

## Dependency Order

Changes are ordered by topological sort:

**Creation order**: Extensions → Types → Sequences → Tables → Columns/Constraints → Indexes → Views → Functions → Triggers → TimescaleDB

**Deletion order**: Reversed

## Testing

```bash
go test -v ./internal/parser/...              # Package tests
go test -v -run TestTableParsing ./...        # Specific test
go test -race -coverprofile=cov.out ./...     # With coverage
```

Tests use table-driven pattern with `testify/assert` and `testify/require`. Unit tests in `tests/` subdirectory of each package.

## Code Conventions

- **Errors**: Wrap with `util.WrapError("context", err)`
- **Identifiers**: Use `schema.NormalizeIdentifier()` for case normalization
- **Qualified names**: Use `schema.QualifiedName()` for "schema.name" format

## Dependencies

- `github.com/jackc/pgx/v5` - PostgreSQL driver
- `github.com/spf13/cobra` - CLI framework
- `github.com/stretchr/testify` - Testing assertions
