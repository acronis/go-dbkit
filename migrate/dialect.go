/*
Copyright © 2026 Acronis International GmbH.

Released under MIT license.
*/

package migrate

import (
	"context"
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
	// inspect describes an existing table, resolved by its unquoted name like the manager's SQL.
	inspect func(ctx context.Context, q queryer, table string) (*tableSchema, error)
}

// quote quotes an identifier, escaping embedded closing quote characters.
func (d *sqlDialect) quote(name string) string {
	return d.openQuote + strings.ReplaceAll(name, d.closeQuote, d.closeQuote+d.closeQuote) + d.closeQuote
}

// The metadata table keeps the schema created by earlier versions of the package (sql-migrate v1.0.0 with gorp v1.7.2):
// "id" primary key and nullable "applied_at"; the types and table options of each dialect are listed in
// testdata/upgrade/README.md. tableOptions is appended after the column list.
func createTableIfNotExists(idType, appliedAtType, tableOptions string) func(string) (string, []any) {
	return func(quotedTable string) (string, []any) {
		return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id %s NOT NULL PRIMARY KEY, applied_at %s)%s",
			quotedTable, idType, appliedAtType, tableOptions), nil
	}
}

var (
	questionPlaceholder = func(int) string { return "?" }

	sqliteDialect = &sqlDialect{
		openQuote:   `"`,
		closeQuote:  `"`,
		placeholder: questionPlaceholder,
		createTable: createTableIfNotExists("VARCHAR(255)", "DATETIME", ""),
		inspect:     inspectSQLite,
	}
	postgresDialect = &sqlDialect{
		openQuote:   `"`,
		closeQuote:  `"`,
		placeholder: func(n int) string { return "$" + strconv.Itoa(n) },
		createTable: createTableIfNotExists("TEXT", "TIMESTAMP WITH TIME ZONE", ""), // TEXT allows IDs longer than 255.
		inspect:     inspectPostgres,
	}
	mysqlDialect = &sqlDialect{
		openQuote:   "`",
		closeQuote:  "`",
		placeholder: questionPlaceholder,
		// utf8mb3 is the historical "UTF8" charset; it keeps VARCHAR(255) keys within InnoDB index limits.
		createTable: createTableIfNotExists("VARCHAR(255)", "DATETIME", " ENGINE=InnoDB DEFAULT CHARSET=utf8mb3"),
		inspect:     inspectMySQL,
	}
	mssqlDialect       = newMSSQLDialect(func(n int) string { return "@p" + strconv.Itoa(n) })
	mssqlLegacyDialect = newMSSQLDialect(questionPlaceholder)
)

func newMSSQLDialect(placeholder func(int) string) *sqlDialect {
	object := "OBJECT_ID(" + placeholder(1) + ", N'U')"
	return &sqlDialect{
		openQuote:   "[",
		closeQuote:  "]",
		placeholder: placeholder,
		createTable: func(quotedTable string) (string, []any) {
			return fmt.Sprintf(
				"IF "+object+" IS NULL CREATE TABLE %s (id VARCHAR(255) NOT NULL PRIMARY KEY, applied_at DATETIME)",
				quotedTable), []any{quotedTable}
		},
		inspect: func(ctx context.Context, q queryer, table string) (*tableSchema, error) {
			return inspectMSSQL(ctx, q, table, object)
		},
	}
}

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
