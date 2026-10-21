package postgresparse

import (
	"fmt"
	"sync"

	pgquery "github.com/pganalyze/pg_query_go/v6"
	wasmquery "github.com/wasilibs/go-pgquery"
)

// The WASM runtime pool and its lazy global metadata require one shared lock.
var runtimeMu sync.Mutex //nolint:gochecknoglobals

func Parse(sql string) (*pgquery.ParseResult, error) {
	runtimeMu.Lock()
	defer runtimeMu.Unlock()

	tree, err := wasmquery.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("parse PostgreSQL statement: %w", err)
	}

	return tree, nil
}

func Deparse(tree *pgquery.ParseResult) (string, error) {
	runtimeMu.Lock()
	defer runtimeMu.Unlock()

	sql, err := wasmquery.Deparse(tree)
	if err != nil {
		return "", fmt.Errorf("deparse PostgreSQL statement: %w", err)
	}

	return sql, nil
}
