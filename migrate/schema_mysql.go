/*
Copyright © 2026 Acronis International GmbH.

Released under MIT license.
*/

package migrate

import (
	"context"
	"database/sql"
	"strings"
)

// MySQL and MariaDB DATA_TYPE values. BINARY/VARBINARY/BLOB are excluded; DATE and TIME are not timestamps.
var (
	mysqlStringTypes    = []string{typeVarchar, typeChar, "tinytext", typeText, "mediumtext", "longtext"}
	mysqlTimestampTypes = []string{typeDatetime, typeTimestamp}
)

// inspectMySQL reads information_schema of the current database, where the manager's unqualified table name resolves.
// It serves MySQL and MariaDB.
func inspectMySQL(ctx context.Context, q queryer, table string) (*tableSchema, error) {
	found := false
	err := collectRows(ctx, q, `SELECT 1 FROM information_schema.TABLES
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND TABLE_TYPE = 'BASE TABLE'`,
		func(*sql.Rows) error { found = true; return nil }, table)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errTableNotFound
	}

	s := &tableSchema{uniqueColumns: map[string]bool{}}
	err = collectRows(ctx, q, `SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA
FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION`,
		func(rows *sql.Rows) error {
			var name, dataType, columnType, nullable, extra string
			var dflt sql.NullString
			if err := rows.Scan(&name, &dataType, &columnType, &nullable, &dflt, &extra); err != nil {
				return err
			}
			extra = strings.ToUpper(extra)
			// "VIRTUAL GENERATED" and "STORED GENERATED"; "DEFAULT_GENERATED" marks an expression default.
			generated := strings.Contains(extra, "VIRTUAL GENERATED") || strings.Contains(extra, "STORED GENERATED") ||
				strings.Contains(extra, "PERSISTENT GENERATED")
			s.columns = append(s.columns, schemaColumn{
				name:     strings.ToLower(name),
				dataType: columnType,
				kind:     kindOf(strings.ToLower(dataType), mysqlStringTypes, mysqlTimestampTypes),
				nullable: nullable == "YES",
				// MySQL reports no default and DEFAULT NULL as NULL; MariaDB reports DEFAULT NULL as the text NULL.
				hasDefault:    dflt.Valid && dflt.String != sqlNull,
				generated:     generated,
				autoValue:     strings.Contains(extra, "AUTO_INCREMENT"),
				explicitWrite: !generated,
			})
			return nil
		}, table)
	if err != nil {
		return nil, err
	}

	if uniqueErr := mysqlUniqueColumns(ctx, q, table, s); uniqueErr != nil {
		return nil, uniqueErr
	}
	return s, nil
}

// mysqlUniqueColumns adds columns with a single-column unique index without a prefix length or expression.
func mysqlUniqueColumns(ctx context.Context, q queryer, table string, s *tableSchema) error {
	type indexInfo struct {
		columns  []sql.NullString
		prefixed bool
	}
	indexes := map[string]*indexInfo{}
	err := collectRows(ctx, q, `SELECT INDEX_NAME, COLUMN_NAME, SUB_PART FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND NON_UNIQUE = 0`, func(rows *sql.Rows) error {
		var index string
		var column sql.NullString // NULL for a functional key part.
		var subPart sql.NullInt64 // Prefix length of a prefix key part.
		if err := rows.Scan(&index, &column, &subPart); err != nil {
			return err
		}
		info := indexes[index]
		if info == nil {
			info = &indexInfo{}
			indexes[index] = info
		}
		info.columns = append(info.columns, column)
		info.prefixed = info.prefixed || subPart.Valid
		return nil
	}, table)
	if err != nil {
		return err
	}
	for _, info := range indexes {
		if len(info.columns) == 1 && info.columns[0].Valid && !info.prefixed {
			s.uniqueColumns[strings.ToLower(info.columns[0].String)] = true
		}
	}
	return nil
}
