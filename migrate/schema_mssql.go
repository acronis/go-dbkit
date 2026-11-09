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

// SQL Server system type names. Legacy TEXT/NTEXT and binary types are excluded; DATE is not a timestamp.
var (
	mssqlStringTypes    = []string{typeVarchar, typeNVarchar, typeChar, typeNChar}
	mssqlTimestampTypes = []string{typeDatetime, "datetime2", "smalldatetime", "datetimeoffset"}
)

// mssqlObject resolves the quoted table name like the manager's SQL, in the default schema of the user.
const mssqlObject = "OBJECT_ID(@p1, N'U')"

func inspectMSSQL(ctx context.Context, q queryer, table string) (*tableSchema, error) {
	quoted := "[" + strings.ReplaceAll(table, "]", "]]") + "]"
	found := false
	err := collectRows(ctx, q, "SELECT 1 WHERE "+mssqlObject+" IS NOT NULL", func(*sql.Rows) error {
		found = true
		return nil
	}, quoted)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errTableNotFound
	}

	s := &tableSchema{uniqueColumns: map[string]bool{}}
	err = collectRows(ctx, q, `SELECT c.name, TYPE_NAME(c.system_type_id), c.is_nullable, c.is_computed, c.is_identity,
	COALESCE(dc.definition, '')
FROM sys.columns c
LEFT JOIN sys.default_constraints dc ON dc.object_id = c.default_object_id
WHERE c.object_id = `+mssqlObject+` ORDER BY c.column_id`, func(rows *sql.Rows) error {
		var name, typeName, dflt string
		var nullable, computed, identity bool
		if err := rows.Scan(&name, &typeName, &nullable, &computed, &identity, &dflt); err != nil {
			return err
		}
		typeName = strings.ToLower(typeName)
		generated := computed || typeName == typeTimestamp // TIMESTAMP is ROWVERSION in SQL Server.
		s.columns = append(s.columns, schemaColumn{
			name:          strings.ToLower(name),
			dataType:      typeName,
			kind:          kindOf(typeName, mssqlStringTypes, mssqlTimestampTypes),
			nullable:      nullable,
			hasDefault:    dflt != "" && !isNullDefault(dflt),
			generated:     generated,
			autoValue:     identity,
			explicitWrite: !generated && !identity, // Explicit identity values require IDENTITY_INSERT.
		})
		return nil
	}, quoted)
	if err != nil {
		return nil, err
	}

	keyColumns := map[int][]string{}
	err = collectRows(ctx, q, `SELECT i.index_id, COL_NAME(ic.object_id, ic.column_id)
FROM sys.indexes i
JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 0
WHERE i.object_id = `+mssqlObject+` AND i.is_unique = 1 AND i.is_disabled = 0 AND i.has_filter = 0
	AND i.is_hypothetical = 0`, func(rows *sql.Rows) error {
		var indexID int
		var column string
		if err := rows.Scan(&indexID, &column); err != nil {
			return err
		}
		keyColumns[indexID] = append(keyColumns[indexID], column)
		return nil
	}, quoted)
	if err != nil {
		return nil, err
	}
	for _, cols := range keyColumns {
		if len(cols) == 1 {
			s.uniqueColumns[strings.ToLower(cols[0])] = true
		}
	}
	return s, nil
}
