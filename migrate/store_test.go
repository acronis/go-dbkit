/*
Copyright © 2026 Acronis International GmbH.

Released under MIT license.
*/

package migrate

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	"github.com/acronis/go-dbkit"
)

func openTestSQLite(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", "file:"+t.Name()+"?mode=memory&cache=shared")
	require.NoError(t, err)
	t.Cleanup(func() { requireNoErrOnClose(t, db) })
	return db
}

func TestMetadataStore(t *testing.T) {
	ctx := context.Background()
	db := openTestSQLite(t)

	store, err := newMetadataStore(db, dbkit.DialectSQLite, "my migrations")
	require.NoError(t, err)

	require.NoError(t, store.ensureTable(ctx))
	require.NoError(t, store.ensureTable(ctx)) // Idempotent.

	records, err := store.list(ctx)
	require.NoError(t, err)
	require.Empty(t, records)

	now := time.Now().UTC().Truncate(time.Second)
	require.NoError(t, store.insert(ctx, db, "0002", now))
	require.NoError(t, store.insert(ctx, db, "0001", now))
	require.Error(t, store.insert(ctx, db, "0001", now)) // Duplicate ID.

	records, err = store.list(ctx)
	require.NoError(t, err)
	require.Equal(t, []metadataRecord{{"0001", now}, {"0002", now}}, records)

	require.NoError(t, store.delete(ctx, db, "0001"))
	records, err = store.list(ctx)
	require.NoError(t, err)
	require.Equal(t, []metadataRecord{{"0002", now}}, records)
}

func TestMetadataStore_NullAppliedAt(t *testing.T) {
	ctx := context.Background()
	db := openTestSQLite(t)
	store, err := newMetadataStore(db, dbkit.DialectSQLite, MigrationsTableName)
	require.NoError(t, err)
	require.NoError(t, store.ensureTable(ctx))
	_, err = db.Exec(`INSERT INTO migrations (id, applied_at) VALUES ('0001', NULL)`)
	require.NoError(t, err)

	records, err := store.list(ctx)
	require.NoError(t, err)
	require.Equal(t, []metadataRecord{{ID: "0001"}}, records)
}

func TestNewMetadataStore_UnknownDialect(t *testing.T) {
	_, err := newMetadataStore(nil, "oracle", MigrationsTableName)
	require.EqualError(t, err, "unknown dialect: oracle")
}

// utcTimeArg matches a time.Time argument in UTC.
type utcTimeArg struct{}

func (utcTimeArg) Match(v driver.Value) bool {
	tm, ok := v.(time.Time)
	return ok && tm.Location() == time.UTC
}

// TestMetadataStore_insertStoresUTC guards against storing the local wall clock in columns without time zone
// (MSSQL DATETIME), which are read back as UTC.
func TestMetadataStore_insertStoresUTC(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	store, err := newMetadataStore(db, dbkit.DialectMSSQL, MigrationsTableName)
	require.NoError(t, err)

	mock.ExpectExec(`INSERT INTO \[migrations\] \(id, applied_at\) VALUES \(@p1, @p2\)`).
		WithArgs("0001", utcTimeArg{}).WillReturnResult(sqlmock.NewResult(0, 1))
	require.NoError(t, store.insert(context.Background(), db, "0001", time.Now().In(time.FixedZone("UTC+2", 2*60*60))))
	require.NoError(t, mock.ExpectationsWereMet())
}
