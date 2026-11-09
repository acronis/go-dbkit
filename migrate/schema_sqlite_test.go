/*
Copyright © 2026 Acronis International GmbH.

Released under MIT license.
*/

package migrate

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMetadataSchema_SQLiteInspector checks the SQLite inspector together with the compatibility rules on real tables.
func TestMetadataSchema_SQLiteInspector(t *testing.T) {
	tests := []struct {
		name string
		ddl  []string
		want []string // Reasons; nil for a compatible table.
	}{
		{name: "created by the package", ddl: nil},
		{
			name: "legacy DDL of the previous implementation",
			ddl:  []string{`create table if not exists "m" ("id" varchar(255) not null primary key, "applied_at" datetime)`},
		},
		{
			name: "text id with unique constraint, reordered, timestamp, optional and generated extras",
			ddl: []string{`CREATE TABLE m (note TEXT, applied_at TIMESTAMP NOT NULL, tenant TEXT NOT NULL DEFAULT 'acme',
				id TEXT NOT NULL UNIQUE, id_len INT GENERATED ALWAYS AS (length(id)) VIRTUAL,
				id_up TEXT GENERATED ALWAYS AS (upper(id)) STORED)`},
		},
		{
			name: "unique index on id and integer primary key extra",
			ddl: []string{
				"CREATE TABLE m (seq INTEGER PRIMARY KEY, id VARCHAR(64) NOT NULL, applied_at DATETIME)",
				"CREATE UNIQUE INDEX m_id ON m (id)",
			},
		},
		{
			name: "missing applied_at",
			ddl:  []string{"CREATE TABLE m (id VARCHAR(255) NOT NULL PRIMARY KEY)"},
			want: []string{`column "applied_at" is missing`},
		},
		{
			name: "wrong types",
			ddl:  []string{"CREATE TABLE m (id INT NOT NULL PRIMARY KEY, applied_at DATE)"},
			want: []string{
				`column "id" has unsupported type INT, a character string type is required`,
				`column "applied_at" has unsupported type DATE, a date and time type is required`,
			},
		},
		{
			name: "blob id",
			ddl:  []string{"CREATE TABLE m (id BLOB NOT NULL PRIMARY KEY, applied_at DATETIME)"},
			want: []string{`column "id" has unsupported type BLOB, a character string type is required`},
		},
		{
			// SQLite allows NULL in a non-integer primary key unless NOT NULL is declared.
			name: "primary key without NOT NULL",
			ddl:  []string{"CREATE TABLE m (id VARCHAR(255) PRIMARY KEY, applied_at DATETIME)"},
			want: []string{`column "id" is nullable`},
		},
		{
			name: "no unique key",
			ddl:  []string{"CREATE TABLE m (id VARCHAR(255) NOT NULL, applied_at DATETIME)", "CREATE INDEX m_id ON m (id)"},
			want: []string{`column "id" has no primary key or unique index on it alone`},
		},
		{
			name: "composite primary key",
			ddl:  []string{"CREATE TABLE m (id VARCHAR(255) NOT NULL, applied_at DATETIME NOT NULL, PRIMARY KEY (id, applied_at))"},
			want: []string{`column "id" has no primary key or unique index on it alone`},
		},
		{
			name: "partial unique index",
			ddl: []string{
				"CREATE TABLE m (id VARCHAR(255) NOT NULL, applied_at DATETIME)",
				"CREATE UNIQUE INDEX m_id ON m (id) WHERE id <> ''",
			},
			want: []string{`column "id" has no primary key or unique index on it alone`},
		},
		{
			name: "expression unique index",
			ddl: []string{
				"CREATE TABLE m (id VARCHAR(255) NOT NULL, applied_at DATETIME)",
				"CREATE UNIQUE INDEX m_id ON m (lower(id))",
			},
			want: []string{`column "id" has no primary key or unique index on it alone`},
		},
		{
			name: "generated applied_at",
			ddl: []string{`CREATE TABLE m (id VARCHAR(255) NOT NULL PRIMARY KEY,
				applied_at DATETIME GENERATED ALWAYS AS ('2020-01-01 00:00:00') VIRTUAL)`},
			want: []string{`column "applied_at" is generated and cannot be written`},
		},
		{
			name: "required extra columns, DEFAULT NULL is not a default",
			ddl: []string{`CREATE TABLE m (id VARCHAR(255) NOT NULL PRIMARY KEY, applied_at DATETIME,
				tenant_id INT NOT NULL, owner TEXT NOT NULL DEFAULT NULL)`},
			want: []string{`column "tenant_id" is NOT NULL without a default`, `column "owner" is NOT NULL without a default`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			db := openTestSQLite(t)
			for _, ddl := range tt.ddl {
				_, err := db.Exec(ddl)
				require.NoError(t, err)
			}
			store, err := newMetadataStore(db, "sqlite3", "m")
			require.NoError(t, err)
			require.NoError(t, store.ensureTable(ctx))

			schema, err := inspectSQLite(ctx, db, "m")
			require.NoError(t, err)
			require.Equal(t, tt.want, checkMetadataSchema(schema))

			err = store.validateSchema(ctx)
			if tt.want == nil {
				require.NoError(t, err)
			} else {
				var incompatible *incompatibleSchemaError
				require.ErrorAs(t, err, &incompatible)
				require.Equal(t, tt.want, incompatible.reasons)
			}
		})
	}
}

func TestMetadataSchema_SQLiteInspectorErrors(t *testing.T) {
	ctx := context.Background()
	db := openTestSQLite(t)
	_, err := inspectSQLite(ctx, db, "missing")
	require.ErrorIs(t, err, errTableNotFound)

	store, err := newMetadataStore(db, "sqlite3", "missing")
	require.NoError(t, err)
	err = store.validateSchema(ctx)
	require.ErrorIs(t, err, errTableNotFound)
	require.ErrorContains(t, err, `inspect migration history table "missing"`)
}

// TestMetadataSchema_SQLiteResolution checks that the inspected table is the one unqualified SQL uses:
// a same-name table in an attached database is ignored.
func TestMetadataSchema_SQLiteResolution(t *testing.T) {
	ctx := context.Background()
	db := openTestSQLite(t)
	db.SetMaxOpenConns(1) // ATTACH is per connection.
	other := filepath.Join(t.TempDir(), "other.db")
	_, err := db.Exec("ATTACH DATABASE ? AS other", other)
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.Remove(other) })
	_, err = db.Exec("CREATE TABLE other.m (id INT, applied_at DATE, required_col INT NOT NULL)")
	require.NoError(t, err)
	_, err = db.Exec("CREATE TABLE main.m (id VARCHAR(255) NOT NULL PRIMARY KEY, applied_at DATETIME)")
	require.NoError(t, err)

	schema, err := inspectSQLite(ctx, db, "m")
	require.NoError(t, err)
	require.Nil(t, checkMetadataSchema(schema))
}
