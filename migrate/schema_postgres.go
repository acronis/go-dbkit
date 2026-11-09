/*
Copyright © 2026 Acronis International GmbH.

Released under MIT license.
*/

package migrate

import (
	"context"
	"database/sql"
)

// PostgreSQL type names as returned by format_type without a type modifier; domains are resolved to their base types.
var (
	postgresStringTypes    = []string{typeText, "character varying"}
	postgresTimestampTypes = []string{"timestamp with time zone", "timestamp without time zone"}
)

// postgresRelation resolves the table name the same way as the manager's unqualified SQL, through search_path.
const postgresRelation = "to_regclass(quote_ident($1))"

func inspectPostgres(ctx context.Context, q queryer, table string) (*tableSchema, error) {
	found := false
	err := collectRows(ctx, q, "SELECT 1 FROM pg_class c WHERE c.oid = "+postgresRelation+" AND c.relkind IN ('r', 'p')",
		func(*sql.Rows) error { found = true; return nil }, table)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errTableNotFound
	}

	s := &tableSchema{uniqueColumns: map[string]bool{}}
	err = collectRows(ctx, q, `SELECT a.attname,
	format_type(CASE WHEN t.typtype = 'd' THEN t.typbasetype ELSE a.atttypid END, NULL),
	NOT a.attnotnull AND NOT (t.typtype = 'd' AND t.typnotnull),
	COALESCE(pg_get_expr(d.adbin, d.adrelid), t.typdefault, ''),
	a.attgenerated, a.attidentity
FROM pg_attribute a
JOIN pg_type t ON t.oid = a.atttypid
LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
WHERE a.attrelid = `+postgresRelation+` AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum`, func(rows *sql.Rows) error {
		var name, typeName, dflt, generated, identity string
		var nullable bool
		if err := rows.Scan(&name, &typeName, &nullable, &dflt, &generated, &identity); err != nil {
			return err
		}
		c := schemaColumn{
			name:          name,
			dataType:      typeName,
			kind:          kindOf(typeName, postgresStringTypes, postgresTimestampTypes),
			nullable:      nullable,
			hasDefault:    dflt != "" && !isNullDefault(dflt),
			generated:     generated != "",
			autoValue:     identity != "",
			explicitWrite: generated == "" && identity != "a", // GENERATED ALWAYS AS IDENTITY rejects explicit values.
		}
		s.columns = append(s.columns, c)
		return nil
	}, table)
	if err != nil {
		return nil, err
	}

	err = collectRows(ctx, q, `SELECT a.attname
FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0]
WHERE i.indrelid = `+postgresRelation+` AND i.indisunique AND i.indisvalid AND i.indisready AND i.indislive
	AND i.indpred IS NULL AND i.indexprs IS NULL AND i.indnkeyatts = 1`, func(rows *sql.Rows) error {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		s.uniqueColumns[name] = true
		return nil
	}, table)
	if err != nil {
		return nil, err
	}
	return s, nil
}
