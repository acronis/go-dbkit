/*
Copyright © 2026 Acronis International GmbH.

Released under MIT license.
*/

package migrate

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	"github.com/acronis/go-dbkit"
)

// inspectorStep is one catalog query of an inspector with a successful result.
type inspectorStep struct {
	query   string // Regular expression.
	arg     string
	columns []string
	row     []driver.Value
}

type inspectorCase struct {
	dialect dbkit.Dialect
	steps   []inspectorStep
}

// The table name contains characters that would break SQL if it were not bound.
const inspectedTable = "my'table"

func inspectorCases() []inspectorCase {
	return []inspectorCase{
		{dbkit.DialectSQLite, []inspectorStep{
			{`pragma_table_xinfo\(\?\)`, inspectedTable, []string{"name", "type", "notnull", "dflt_value", "pk", "hidden"},
				[]driver.Value{"id", "VARCHAR(255)", true, nil, 1, 0}},
			{`pragma_index_list\(\?\)`, inspectedTable, []string{"name"}, []driver.Value{"sqlite_autoindex_1"}},
			{`pragma_index_xinfo\(\?\)`, "sqlite_autoindex_1", []string{"name"}, []driver.Value{"id"}},
		}},
		{dbkit.DialectPostgres, []inspectorStep{
			{`FROM pg_class c WHERE c.oid = to_regclass\(quote_ident\(\$1\)\)`, inspectedTable, []string{"x"}, []driver.Value{1}},
			{`FROM pg_attribute a`, inspectedTable, []string{"attname", "type", "nullable", "default", "generated", "identity"},
				[]driver.Value{"id", "text", false, "", "", ""}},
			{`FROM pg_index i`, inspectedTable, []string{"attname"}, []driver.Value{"id"}},
		}},
		{dbkit.DialectMySQL, []inspectorStep{
			{`FROM information_schema.TABLES`, inspectedTable, []string{"x"}, []driver.Value{1}},
			{`FROM information_schema.COLUMNS`, inspectedTable,
				[]string{"COLUMN_NAME", "DATA_TYPE", "COLUMN_TYPE", "IS_NULLABLE", "COLUMN_DEFAULT", "EXTRA"},
				[]driver.Value{"id", "varchar", "varchar(255)", "NO", nil, ""}},
			{`FROM information_schema.STATISTICS`, inspectedTable, []string{"INDEX_NAME", "COLUMN_NAME", "SUB_PART"},
				[]driver.Value{"PRIMARY", "id", nil}},
		}},
		{dbkit.DialectMSSQL, []inspectorStep{
			{`SELECT 1 WHERE OBJECT_ID\(@p1, N'U'\) IS NOT NULL`, "[" + inspectedTable + "]", []string{"x"}, []driver.Value{1}},
			{`FROM sys.columns c`, "[" + inspectedTable + "]",
				[]string{"name", "type", "is_nullable", "is_computed", "is_identity", "definition"},
				[]driver.Value{"id", "varchar", false, false, false, ""}},
			{`FROM sys.indexes i`, "[" + inspectedTable + "]", []string{"index_id", "column"}, []driver.Value{1, "id"}},
		}},
	}
}

func (s inspectorStep) expect(mock sqlmock.Sqlmock) *sqlmock.ExpectedQuery {
	return mock.ExpectQuery(s.query).WithArgs(s.arg)
}

func (s inspectorStep) rows() *sqlmock.Rows {
	return sqlmock.NewRows(s.columns).AddRow(s.row...)
}

func TestMetadataSchema_InspectorCatalogs(t *testing.T) {
	errCatalog := errors.New("catalog failure")
	for _, tc := range inspectorCases() {
		d, err := lookupSQLDialect(tc.dialect)
		require.NoError(t, err)

		t.Run(string(tc.dialect)+"/success", func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			for _, step := range tc.steps {
				step.expect(mock).WillReturnRows(step.rows())
			}
			schema, err := d.inspect(context.Background(), db, inspectedTable)
			require.NoError(t, err)
			require.True(t, schema.uniqueColumns["id"])
			id, ok := schema.column("id")
			require.True(t, ok)
			require.Equal(t, kindString, id.kind)
			require.False(t, id.nullable)
			require.NoError(t, mock.ExpectationsWereMet())
		})

		for failing := range tc.steps {
			failures := map[string]func(step inspectorStep, e *sqlmock.ExpectedQuery){
				"query": func(_ inspectorStep, e *sqlmock.ExpectedQuery) { e.WillReturnError(errCatalog) },
				"scan": func(_ inspectorStep, e *sqlmock.ExpectedQuery) {
					e.WillReturnRows(sqlmock.NewRows([]string{"unexpected", "columns", "count", "x", "y", "z", "w"}).
						AddRow(1, 2, 3, 4, 5, 6, 7))
				},
				"iteration": func(step inspectorStep, e *sqlmock.ExpectedQuery) {
					e.WillReturnRows(step.rows().AddRow(step.row...).RowError(1, errCatalog))
				},
			}
			for kind, fail := range failures {
				t.Run(fmt.Sprintf("%s/%s error at step %d", tc.dialect, kind, failing+1), func(t *testing.T) {
					db, mock, err := sqlmock.New()
					require.NoError(t, err)
					t.Cleanup(func() { _ = db.Close() })
					for i, step := range tc.steps[:failing+1] {
						e := step.expect(mock)
						if i < failing {
							e.WillReturnRows(step.rows())
						} else {
							fail(step, e)
						}
					}
					_, err = d.inspect(context.Background(), db, inspectedTable)
					require.Error(t, err)
					if kind != "scan" {
						require.ErrorIs(t, err, errCatalog)
					}
					require.NoError(t, mock.ExpectationsWereMet()) // No queries after the failure.
				})
			}
		}

		t.Run(string(tc.dialect)+"/table not found", func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			tc.steps[0].expect(mock).WillReturnRows(sqlmock.NewRows(tc.steps[0].columns))
			_, err = d.inspect(context.Background(), db, inspectedTable)
			require.ErrorIs(t, err, errTableNotFound)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
