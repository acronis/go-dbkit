/*
Copyright © 2026 Acronis International GmbH.

Released under MIT license.
*/

package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/acronis/go-dbkit"
)

// execer is implemented by *sql.DB and *sql.Tx.
type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// metadataRecord is a row of the migrations metadata table.
type metadataRecord struct {
	ID        string
	AppliedAt time.Time // Zero if the stored value is NULL.
}

// metadataStore reads and writes applied migrations in the metadata table.
type metadataStore struct {
	db          *sql.DB
	dialect     *sqlDialect
	dialectName dbkit.Dialect
	name        string // Table name as configured.
	table       string // Quoted table name.
}

func newMetadataStore(db *sql.DB, dialect dbkit.Dialect, tableName string) (*metadataStore, error) {
	d, err := lookupSQLDialect(dialect)
	if err != nil {
		return nil, err
	}
	return &metadataStore{db: db, dialect: d, dialectName: dialect, name: tableName, table: d.quote(tableName)}, nil
}

// prepare creates the metadata table if needed and checks that an existing table is usable. Callers read the history
// with list afterwards and run no migration SQL before. Validation is not cached: every call inspects the current schema.
func (s *metadataStore) prepare(ctx context.Context) error {
	if err := s.ensureTable(ctx); err != nil {
		return err
	}
	return s.validateSchema(ctx)
}

// validateSchema checks that the manager can insert, delete and read history rows in the table.
// It reads catalogs only and never changes the table.
func (s *metadataStore) validateSchema(ctx context.Context) error {
	schema, err := s.dialect.inspect(ctx, s.db, s.name)
	if err != nil {
		return fmt.Errorf("inspect migration history table %q: %w", s.name, err)
	}
	if reasons := checkMetadataSchema(schema); len(reasons) > 0 {
		return &incompatibleSchemaError{table: s.name, dialect: s.dialectName, reasons: reasons}
	}
	return nil
}

// ensureTable creates the metadata table if it doesn't exist.
func (s *metadataStore) ensureTable(ctx context.Context) error {
	query, args := s.dialect.createTable(s.table)
	if _, err := s.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("create migrations table: %w", err)
	}
	return nil
}

// list returns all applied migrations ordered by ID.
func (s *metadataStore) list(ctx context.Context) ([]metadataRecord, error) {
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf("SELECT id, applied_at FROM %s ORDER BY id ASC", s.table))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var records []metadataRecord
	for rows.Next() {
		var id string
		var appliedAt sql.NullTime
		if err := rows.Scan(&id, &appliedAt); err != nil {
			return nil, err
		}
		records = append(records, metadataRecord{ID: id, AppliedAt: appliedAt.Time})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

// insert records an applied migration. The time is stored in UTC: columns without time zone (MSSQL DATETIME) keep the
// wall clock of the passed value and are read back as UTC.
func (s *metadataStore) insert(ctx context.Context, ex execer, id string, appliedAt time.Time) error {
	query := fmt.Sprintf("INSERT INTO %s (id, applied_at) VALUES (%s, %s)",
		s.table, s.dialect.placeholder(1), s.dialect.placeholder(2))
	_, err := ex.ExecContext(ctx, query, id, appliedAt.UTC())
	return err
}

func (s *metadataStore) delete(ctx context.Context, ex execer, id string) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE id = %s", s.table, s.dialect.placeholder(1))
	_, err := ex.ExecContext(ctx, query, id)
	return err
}
