/*
Copyright © 2026 Acronis International GmbH.

Released under MIT license.
*/

package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/acronis/go-dbkit"
)

// queryer is implemented by *sql.DB and *sql.Tx.
type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// Type names shared by dialect allowlists.
const (
	typeText      = "text"
	typeChar      = "char"
	typeVarchar   = "varchar"
	typeNVarchar  = "nvarchar"
	typeNChar     = "nchar"
	typeDatetime  = "datetime"
	typeTimestamp = "timestamp"
	sqlNull       = "NULL"
)

// errTableNotFound is returned by schema inspectors when the metadata table does not exist.
var errTableNotFound = errors.New("table not found")

// columnKind classifies a column type by the values the manager writes into it.
type columnKind int

const (
	kindOther     columnKind = iota
	kindString               // Variable or fixed length character string accepted as migration ID.
	kindTimestamp            // Date and time scanned by drivers into time.Time.
)

// schemaColumn is a normalized description of a table column.
type schemaColumn struct {
	name     string // Lower case on case-insensitive dialects.
	dataType string // Type as reported by the catalog, for messages.
	kind     columnKind
	nullable bool
	// hasDefault is true for a non-NULL default value.
	hasDefault bool
	// generated is true for generated, computed and row version columns: the database computes the value and an
	// explicit value cannot be written.
	generated bool
	// autoValue is true for identity and auto-increment columns: the database supplies the value when it is omitted.
	autoValue bool
	// explicitWrite is false when an explicit value cannot be inserted (generated, MSSQL or ALWAYS identity columns).
	explicitWrite bool
}

// tableSchema is a normalized description of the metadata table.
type tableSchema struct {
	columns []schemaColumn
	// uniqueColumns contains columns protected by a valid, complete unique index or constraint (including the primary key)
	// on that column alone. Composite, partial, filtered, expression, prefix, disabled and invalid indexes are excluded.
	uniqueColumns map[string]bool
}

func (s *tableSchema) column(name string) (schemaColumn, bool) {
	for _, c := range s.columns {
		if c.name == name {
			return c, true
		}
	}
	return schemaColumn{}, false
}

// checkMetadataSchema returns the reasons why the manager cannot use the table, or nil.
// The manager inserts explicit id and applied_at values, deletes by id and selects both columns.
func checkMetadataSchema(s *tableSchema) []string {
	var reasons []string
	if reason := checkIDColumn(s); reason != "" {
		reasons = append(reasons, reason)
	}
	if reason := checkAppliedAtColumn(s); reason != "" {
		reasons = append(reasons, reason)
	}
	for _, c := range s.columns {
		if c.name == "id" || c.name == "applied_at" {
			continue
		}
		if !c.nullable && !c.hasDefault && !c.generated && !c.autoValue {
			reasons = append(reasons, fmt.Sprintf("column %q is NOT NULL without a default", c.name))
		}
	}
	return reasons
}

func checkIDColumn(s *tableSchema) string {
	id, ok := s.column("id")
	switch {
	case !ok:
		return `column "id" is missing`
	case !id.explicitWrite:
		return `column "id" is generated and cannot be written`
	case id.kind != kindString:
		return fmt.Sprintf(`column "id" has unsupported type %s, a character string type is required`, id.dataType)
	case id.nullable:
		return `column "id" is nullable`
	case !s.uniqueColumns["id"]:
		return `column "id" has no primary key or unique index on it alone`
	}
	return ""
}

func checkAppliedAtColumn(s *tableSchema) string {
	appliedAt, ok := s.column("applied_at")
	switch {
	case !ok:
		return `column "applied_at" is missing`
	case !appliedAt.explicitWrite:
		return `column "applied_at" is generated and cannot be written`
	case appliedAt.kind != kindTimestamp:
		return fmt.Sprintf(`column "applied_at" has unsupported type %s, a date and time type is required`, appliedAt.dataType)
	}
	return ""
}

// incompatibleSchemaError reports an existing migration history table that the manager cannot use.
type incompatibleSchemaError struct {
	table   string
	dialect dbkit.Dialect
	reasons []string
}

func (e *incompatibleSchemaError) Error() string {
	return fmt.Sprintf("migration history table %q (dialect %s) is incompatible: %s; "+
		"configure MigrationsManagerOpts.TableName to use a separate migration history table",
		e.table, e.dialect, strings.Join(e.reasons, "; "))
}

// kindOf returns the kind of a type by the allowlists of string and timestamp type names.
func kindOf(typeName string, stringTypes, timestampTypes []string) columnKind {
	switch {
	case slices.Contains(stringTypes, typeName):
		return kindString
	case slices.Contains(timestampTypes, typeName):
		return kindTimestamp
	default:
		return kindOther
	}
}

// isNullDefault reports whether a default expression is NULL, possibly with a cast or parentheses.
func isNullDefault(expr string) bool {
	expr = strings.TrimSpace(expr)
	for strings.HasPrefix(expr, "(") && strings.HasSuffix(expr, ")") {
		expr = strings.TrimSpace(expr[1 : len(expr)-1])
	}
	upper := strings.ToUpper(expr)
	return upper == sqlNull || strings.HasPrefix(upper, sqlNull+"::")
}

// collectRows runs a query and calls scan for every row.
func collectRows(ctx context.Context, q queryer, query string, scan func(rows *sql.Rows) error, args ...any) error {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		if scanErr := scan(rows); scanErr != nil {
			return scanErr
		}
	}
	return rows.Err()
}
