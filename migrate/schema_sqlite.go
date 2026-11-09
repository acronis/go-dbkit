/*
Copyright © 2026 Acronis International GmbH.

Released under MIT license.
*/

package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// SQLite types are matched by declared type. Strings: types with TEXT affinity. Timestamps: declared types that
// mattn/go-sqlite3 scans into time.Time; DATE is excluded as a date-only type.
var (
	sqliteStringTypes = []string{typeText, typeVarchar, typeChar, "character", typeNVarchar, typeNChar,
		"varying character", "clob"}
	sqliteTimestampTypes = []string{typeDatetime, typeTimestamp}
)

// SQLite hidden values of pragma_table_xinfo.
const (
	sqliteHiddenVirtualTable     = 1
	sqliteHiddenGeneratedVirtual = 2
	sqliteHiddenGeneratedStored  = 3
)

// inspectSQLite reads the table through pragma table-valued functions, which resolve an unqualified name like other SQL.
func inspectSQLite(ctx context.Context, q queryer, table string) (*tableSchema, error) {
	s := &tableSchema{uniqueColumns: map[string]bool{}}
	if err := sqliteColumns(ctx, q, table, s); err != nil {
		return nil, err
	}
	if len(s.columns) == 0 {
		return nil, errTableNotFound
	}
	if err := sqliteUniqueColumns(ctx, q, table, s); err != nil {
		return nil, err
	}
	return s, nil
}

func sqliteColumns(ctx context.Context, q queryer, table string, s *tableSchema) error {
	var pkCount int
	var integerPK string
	err := collectRows(ctx, q, `SELECT name, type, "notnull", dflt_value, pk, hidden FROM pragma_table_xinfo(?) ORDER BY cid`,
		func(rows *sql.Rows) error {
			var name, declType string
			var notNull bool
			var dflt sql.NullString
			var pk, hidden int
			if err := rows.Scan(&name, &declType, &notNull, &dflt, &pk, &hidden); err != nil {
				return err
			}
			typeName := strings.ToLower(strings.TrimSpace(declType))
			if i := strings.IndexByte(typeName, '('); i >= 0 {
				typeName = strings.TrimSpace(typeName[:i])
			}
			generated := hidden == sqliteHiddenGeneratedVirtual || hidden == sqliteHiddenGeneratedStored ||
				hidden == sqliteHiddenVirtualTable
			if pk > 0 {
				pkCount++
				if typeName == "integer" {
					integerPK = strings.ToLower(name)
				}
			}
			s.columns = append(s.columns, schemaColumn{
				name:          strings.ToLower(name),
				dataType:      declType,
				kind:          kindOf(typeName, sqliteStringTypes, sqliteTimestampTypes),
				nullable:      !notNull,
				hasDefault:    dflt.Valid && !isNullDefault(dflt.String),
				generated:     generated,
				explicitWrite: !generated,
			})
			return nil
		}, table)
	if err != nil {
		return err
	}
	// A single INTEGER PRIMARY KEY column is the rowid alias: unique, filled automatically, and without an index entry.
	if pkCount == 1 && integerPK != "" {
		for i := range s.columns {
			if s.columns[i].name == integerPK {
				s.columns[i].autoValue = true
			}
		}
		s.uniqueColumns[integerPK] = true
	}
	return nil
}

// sqliteUniqueColumns adds columns with a single-column, non-partial unique index without expressions.
func sqliteUniqueColumns(ctx context.Context, q queryer, table string, s *tableSchema) error {
	var indexes []string
	err := collectRows(ctx, q, `SELECT name FROM pragma_index_list(?) WHERE "unique" = 1 AND partial = 0`,
		func(rows *sql.Rows) error {
			var name string
			if err := rows.Scan(&name); err != nil {
				return err
			}
			indexes = append(indexes, name)
			return nil
		}, table)
	if err != nil {
		return err
	}
	for _, index := range indexes {
		// Key columns only; an expression has a NULL name.
		var keyColumns []sql.NullString
		err = collectRows(ctx, q, `SELECT name FROM pragma_index_xinfo(?) WHERE "key" = 1`, func(rows *sql.Rows) error {
			var name sql.NullString
			if err := rows.Scan(&name); err != nil {
				return err
			}
			keyColumns = append(keyColumns, name)
			return nil
		}, index)
		if err != nil {
			return fmt.Errorf("index %s: %w", index, err)
		}
		if len(keyColumns) == 1 && keyColumns[0].Valid {
			s.uniqueColumns[strings.ToLower(keyColumns[0].String)] = true
		}
	}
	return nil
}
