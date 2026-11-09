/*
Copyright © 2026 Acronis International GmbH.

Released under MIT license.
*/

package migrate

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func validMetadataColumns() []schemaColumn {
	return []schemaColumn{
		{name: "id", dataType: "varchar(255)", kind: kindString, explicitWrite: true},
		{name: "applied_at", dataType: "datetime", kind: kindTimestamp, nullable: true, explicitWrite: true},
	}
}

func TestCheckMetadataSchema(t *testing.T) {
	unique := map[string]bool{"id": true}
	with := func(extra ...schemaColumn) []schemaColumn { return append(validMetadataColumns(), extra...) }
	replace := func(name string, c schemaColumn) []schemaColumn {
		cols := validMetadataColumns()
		for i := range cols {
			if cols[i].name == name {
				cols[i] = c
			}
		}
		return cols
	}

	tests := []struct {
		name    string
		columns []schemaColumn
		unique  map[string]bool
		want    []string
	}{
		{name: "legacy schema", columns: validMetadataColumns(), unique: unique},
		{
			name: "reordered columns, NOT NULL applied_at and optional extras",
			columns: []schemaColumn{
				{name: "note", kind: kindString, nullable: true, explicitWrite: true},
				{name: "applied_at", dataType: "timestamp", kind: kindTimestamp, explicitWrite: true},
				{name: "tenant", kind: kindString, hasDefault: true, explicitWrite: true},
				{name: "id_len", kind: kindOther, generated: true},
				{name: "seq", kind: kindOther, autoValue: true},
				{name: "id", dataType: "text", kind: kindString, explicitWrite: true},
			},
			unique: unique,
		},
		{
			name:    "missing columns",
			columns: []schemaColumn{{name: "other", kind: kindString, nullable: true, explicitWrite: true}},
			want:    []string{`column "id" is missing`, `column "applied_at" is missing`},
		},
		{
			name:    "wrong types",
			columns: []schemaColumn{{name: "id", dataType: "int", kind: kindOther, explicitWrite: true}, {name: "applied_at", dataType: "date", explicitWrite: true}},
			unique:  unique,
			want: []string{
				`column "id" has unsupported type int, a character string type is required`,
				`column "applied_at" has unsupported type date, a date and time type is required`,
			},
		},
		{
			name:    "nullable id",
			columns: replace("id", schemaColumn{name: "id", kind: kindString, nullable: true, explicitWrite: true}),
			unique:  unique,
			want:    []string{`column "id" is nullable`},
		},
		{
			name:    "id without unique key",
			columns: validMetadataColumns(),
			unique:  map[string]bool{"applied_at": true},
			want:    []string{`column "id" has no primary key or unique index on it alone`},
		},
		{
			name: "generated metadata columns",
			columns: []schemaColumn{
				{name: "id", kind: kindString, generated: true},
				{name: "applied_at", kind: kindTimestamp, generated: true},
			},
			unique: unique,
			want: []string{
				`column "id" is generated and cannot be written`,
				`column "applied_at" is generated and cannot be written`,
			},
		},
		{
			name:    "identity id without explicit values",
			columns: replace("id", schemaColumn{name: "id", kind: kindString, autoValue: true}),
			unique:  unique,
			want:    []string{`column "id" is generated and cannot be written`},
		},
		{
			name: "required extra columns",
			columns: with(
				schemaColumn{name: "tenant_id", kind: kindOther, explicitWrite: true},
				schemaColumn{name: "owner", kind: kindString, explicitWrite: true},
			),
			unique: unique,
			want:   []string{`column "tenant_id" is NOT NULL without a default`, `column "owner" is NOT NULL without a default`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := checkMetadataSchema(&tableSchema{columns: tt.columns, uniqueColumns: tt.unique})
			require.Equal(t, tt.want, got)
		})
	}
}

func TestIncompatibleSchemaError(t *testing.T) {
	err := &incompatibleSchemaError{table: "migrations", dialect: "postgres", reasons: []string{"a", "b"}}
	require.EqualError(t, err, `migration history table "migrations" (dialect postgres) is incompatible: a; b; `+
		`configure MigrationsManagerOpts.TableName to use a separate migration history table`)
}

func TestIsNullDefault(t *testing.T) {
	for expr, want := range map[string]bool{
		"NULL": true, "null": true, "(NULL)": true, "((NULL))": true, "NULL::character varying": true,
		"'NULL'": false, "0": false, "('acme')": false, "nextval('s'::regclass)": false,
	} {
		require.Equal(t, want, isNullDefault(expr), expr)
	}
}
