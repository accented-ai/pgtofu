# pgtofu

[![Go Version](https://img.shields.io/badge/go-%3E%3D1.25-blue)](https://golang.org/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

Declarative schema management for PostgreSQL and TimescaleDB. Define your schema in SQL files, and pgtofu generates safe, versioned migrations compatible with [golang-migrate](https://github.com/golang-migrate/migrate).

## Why pgtofu?

Traditional migration tools require manually writing incremental migration files. pgtofu takes a **declarative approach**: define your desired schema, and let the tool figure out the changes.

```
SQL Files (desired) ──┐
                      ├──→ Diff ──→ Generate ──→ Migration Files
Database (current) ───┘
```

## Quick Start

```bash
# Install
docker pull accented/pgtofu:latest
# Or: go install github.com/accented-ai/pgtofu/cmd/pgtofu@latest

# Extract current schema
pgtofu extract --database-url "$DATABASE_URL" --output current.json

# Define desired schema
mkdir -p schema/tables
cat > schema/tables/users.sql << 'EOF'
CREATE TABLE users (
    id BIGSERIAL PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
EOF

# Preview and generate migrations
pgtofu diff --current current.json --desired ./schema
pgtofu generate --current current.json --desired ./schema --output-dir ./migrations

# Apply with golang-migrate
migrate -path ./migrations -database "$DATABASE_URL" up
```

## Commands

| Command | Description |
|---------|-------------|
| `extract` | Extract database schema to JSON |
| `diff` | Compare current vs desired schema |
| `generate` | Generate migration files |
| `partition generate` | Generate hash partition definitions |
| `version` | Show version information |

## Features

**PostgreSQL**: Tables, views, materialized views, functions, triggers, sequences, indexes (partial, covering, expression), constraints (PK, FK, UNIQUE, CHECK, EXCLUDE), custom types (enum, composite, domain), partitioning (HASH, RANGE, LIST), generated/identity columns.

**TimescaleDB**: Hypertables, compression policies, retention policies, continuous aggregates.

**Safety**: Change severity classification (SAFE, POTENTIALLY_BREAKING, BREAKING), dependency resolution with topological sorting, idempotent DDL, preview mode.

## Documentation

Full documentation at **[pgtofu.com](https://pgtofu.com)**

## Contributing

```bash
git clone https://github.com/accented-ai/pgtofu.git
cd pgtofu
go build -o /tmp/pgtofu ./cmd/pgtofu
go test ./...
golangci-lint run
```

## License

MIT License - see [LICENSE](LICENSE) for details.
