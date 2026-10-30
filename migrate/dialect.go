/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package migrate

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/acronis/go-dbkit"
)

// sqlDialect holds the SQL differences between supported databases that matter for the metadata table.
type sqlDialect struct {
	openQuote   string
	closeQuote  string
	placeholder func(n int) string
	// createTable returns a statement that creates the metadata table if it doesn't exist.
	// quotedTable is the already quoted table name.
	createTable func(quotedTable string) (query string, args []any)
}

// quote quotes an identifier, escaping embedded closing quote characters.
func (d *sqlDialect) quote(name string) string {
	return d.openQuote + strings.ReplaceAll(name, d.closeQuote, d.closeQuote+d.closeQuote) + d.closeQuote
}

// The metadata table keeps the schema used by earlier versions of the package: "id" primary key and nullable "applied_at".
func createTableIfNotExists(appliedAtType string) func(string) (string, []any) {
	return func(quotedTable string) (string, []any) {
		return fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s (id VARCHAR(255) NOT NULL PRIMARY KEY, applied_at %s)", quotedTable, appliedAtType), nil
	}
}

var (
	questionPlaceholder = func(int) string { return "?" }

	sqliteDialect = &sqlDialect{
		openQuote:   `"`,
		closeQuote:  `"`,
		placeholder: questionPlaceholder,
		createTable: createTableIfNotExists("DATETIME"),
	}
	postgresDialect = &sqlDialect{
		openQuote:   `"`,
		closeQuote:  `"`,
		placeholder: func(n int) string { return "$" + strconv.Itoa(n) },
		createTable: createTableIfNotExists("TIMESTAMP WITH TIME ZONE"),
	}
	mysqlDialect = &sqlDialect{
		openQuote:   "`",
		closeQuote:  "`",
		placeholder: questionPlaceholder,
		createTable: createTableIfNotExists("DATETIME"),
	}
	mssqlDialect = &sqlDialect{
		openQuote:   "[",
		closeQuote:  "]",
		placeholder: func(n int) string { return "@p" + strconv.Itoa(n) },
		createTable: func(quotedTable string) (string, []any) {
			return fmt.Sprintf(
				"IF OBJECT_ID(@p1, N'U') IS NULL CREATE TABLE %s (id VARCHAR(255) NOT NULL PRIMARY KEY, applied_at DATETIME)",
				quotedTable), []any{quotedTable}
		},
	}
)

// lookupSQLDialect returns SQL dialect for the passed one. The pgx dialect uses PostgreSQL SQL.
func lookupSQLDialect(dialect dbkit.Dialect) (*sqlDialect, error) {
	switch dialect {
	case dbkit.DialectSQLite:
		return sqliteDialect, nil
	case dbkit.DialectPostgres, dbkit.DialectPgx:
		return postgresDialect, nil
	case dbkit.DialectMySQL:
		return mysqlDialect, nil
	case dbkit.DialectMSSQL:
		return mssqlDialect, nil
	default:
		return nil, fmt.Errorf("unknown dialect: %s", dialect)
	}
}
