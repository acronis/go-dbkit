/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package migrate

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/acronis/go-dbkit"
)

func TestSQLDialect_quote(t *testing.T) {
	require.Equal(t, `"migrations"`, sqliteDialect.quote("migrations"))
	require.Equal(t, `"a""b"`, postgresDialect.quote(`a"b`))
	require.Equal(t, "`a``b`", mysqlDialect.quote("a`b"))
	require.Equal(t, "[a]]b]", mssqlDialect.quote("a]b"))
}

func TestSQLDialect_placeholder(t *testing.T) {
	require.Equal(t, "?", sqliteDialect.placeholder(2))
	require.Equal(t, "?", mysqlDialect.placeholder(2))
	require.Equal(t, "$2", postgresDialect.placeholder(2))
	require.Equal(t, "@p2", mssqlDialect.placeholder(2))
}

func TestSQLDialect_createTable(t *testing.T) {
	tests := []struct {
		dialect  *sqlDialect
		wantSQL  string
		wantArgs []any
	}{
		{sqliteDialect, `CREATE TABLE IF NOT EXISTS "t" (id VARCHAR(255) NOT NULL PRIMARY KEY, applied_at DATETIME)`, nil},
		{postgresDialect, `CREATE TABLE IF NOT EXISTS "t" (id VARCHAR(255) NOT NULL PRIMARY KEY, applied_at TIMESTAMP WITH TIME ZONE)`, nil},
		{mysqlDialect, "CREATE TABLE IF NOT EXISTS `t` (id VARCHAR(255) NOT NULL PRIMARY KEY, applied_at DATETIME)", nil},
		{
			mssqlDialect,
			"IF OBJECT_ID(@p1, N'U') IS NULL CREATE TABLE [t] (id VARCHAR(255) NOT NULL PRIMARY KEY, applied_at DATETIME)",
			[]any{"[t]"},
		},
	}
	for _, tt := range tests {
		quoted := tt.dialect.quote("t")
		gotSQL, gotArgs := tt.dialect.createTable(quoted)
		require.Equal(t, tt.wantSQL, gotSQL)
		require.Equal(t, tt.wantArgs, gotArgs)
	}
}

func TestLookupSQLDialect(t *testing.T) {
	for dialect, want := range map[dbkit.Dialect]*sqlDialect{
		dbkit.DialectSQLite:   sqliteDialect,
		dbkit.DialectPostgres: postgresDialect,
		dbkit.DialectPgx:      postgresDialect,
		dbkit.DialectMySQL:    mysqlDialect,
		dbkit.DialectMSSQL:    mssqlDialect,
	} {
		got, err := lookupSQLDialect(dialect)
		require.NoError(t, err)
		require.Same(t, want, got)
	}
	_, err := lookupSQLDialect("oracle")
	require.EqualError(t, err, "unknown dialect: oracle")
}
