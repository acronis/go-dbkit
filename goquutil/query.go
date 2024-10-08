/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package goquutil

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exec"
	"github.com/doug-martin/goqu/v9/exp"

	"github.com/acronis/go-dbkit"
)

// QueryDurationObserverFunc is a function type to observe query related stats
type QueryDurationObserverFunc func(preparedQueryString string, ctx context.Context, startTime time.Time, err error)

// ObserveSQLQueryDuration is an actual instance of QueryDurationObserverFunc that is used
var ObserveSQLQueryDuration QueryDurationObserverFunc

// IsInsideTest when set to true enables some checks that are skipped for production code
var IsInsideTest bool

// SQLBuilderSettings is sql builder settings representation
type SQLBuilderSettings struct {
	Dialect goqu.DialectWrapper
}

// Querier is an interface to abstract details of db implementation
type Querier interface {
	QueryRow(query string, args ...interface{}) *sql.Row
	Query(query string, args ...interface{}) (*sql.Rows, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
}

// Scanner is an interface to abstract details of scanning db values
type Scanner interface {
	Scan(dest ...interface{}) error
}

type execFunc func(Querier, string, ...interface{}) (sql.Result, error)
type queryFunc func(Querier, string, ...interface{}) (*sql.Rows, error)
type queryRowFunc func(Querier, string, ...interface{}) *sql.Row

func queryDatabase(
	q Querier,
	sqlExpression exp.SQLExpression,
	execF execFunc,
	queryF queryFunc,
	queryRowF queryRowFunc,
) (sqlResult sql.Result, sqlRows *sql.Rows, sqlRow *sql.Row, err error) {
	literalQuery, params, err := sqlExpression.ToSQL()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("query builing: %w", err)
	}

	queryCouldBeObserved := false
	var currentTime time.Time
	if sqlExpression.IsPrepared() {
		queryCouldBeObserved = true
		currentTime = time.Now()
	} else if IsInsideTest {
		panic(fmt.Sprintf("non-prepared sql statement detected: %s", literalQuery))
	}

	var queryErr error
	switch {
	case execF != nil:
		sqlResult, queryErr = execF(q, literalQuery, params...)
	case queryF != nil:
		sqlRows, queryErr = queryF(q, literalQuery, params...)
	case queryRowF != nil:
		sqlRow = queryRowF(q, literalQuery, params...)
	}

	if queryCouldBeObserved {
		if ObserveSQLQueryDuration != nil {
			var ctx context.Context
			if cq, ok := q.(ContextProvider); ok {
				ctx = cq.Context()
			}
			ObserveSQLQueryDuration(literalQuery, ctx, currentTime, queryErr)
		}
	}

	return sqlResult, sqlRows, sqlRow, queryErr
}

// BuildSQLAndExec is a function for running DML not returning any data like UPDATE, DELETE, INSERT
func BuildSQLAndExec(q Querier, sqlExpression exp.SQLExpression) (sql.Result, error) {
	result, _, _, err := queryDatabase(q, sqlExpression, Querier.Exec, nil, nil)
	return result, err
}

// BuildSQLAndQuery is a function for running SELECT statements returning many rows
func BuildSQLAndQuery(q Querier, sqlExpression exp.SQLExpression) (*sql.Rows, error) {
	_, rows, _, err := queryDatabase(q, sqlExpression, nil, Querier.Query, nil)
	return rows, err
}

// BuildSQLAndQueryRow is a function for running SELECT statements returning single row
func BuildSQLAndQueryRow(q Querier, sqlExpression exp.SQLExpression) (*sql.Row, error) {
	_, _, row, err := queryDatabase(q, sqlExpression, nil, nil, Querier.QueryRow)
	return row, err
}

// BuildSQLAndQueryScalar is a function for running SELECT statements returning single scalar value
func BuildSQLAndQueryScalar(q Querier, sqlExpression exp.SQLExpression, scalar interface{}) error {
	_, _, row, err := queryDatabase(q, sqlExpression, nil, nil, Querier.QueryRow)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	err = row.Scan(scalar)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return fmt.Errorf("scalar scan: %w", err)
	}
	return nil
}

// ScanEachRow is a helper for scanning multiple rows result set
func ScanEachRow(rows *sql.Rows, scanRow func(s Scanner) error) (rowsProcessed int, err error) {
	defer func() { _ = rows.Close() }()
	count := 0
	for rows.Next() {
		err = scanRow(rows)
		if err != nil {
			return 0, fmt.Errorf("row scanning: %w", err)
		}
		count++
	}
	if err = rows.Err(); err != nil {
		return 0, fmt.Errorf("rows scanning: %w", err)
	}
	return count, nil
}

// queryAndScanStructs runs SELECT and scans its result into multiple structs, result is a pointer to slice of structs
func queryAndScanStructs(q Querier, query *goqu.SelectDataset, result interface{}) error {
	if query.GetClauses().IsDefaultSelect() {
		query = query.Select(result)
	}

	rows, err := BuildSQLAndQuery(q, query)
	if err != nil {
		return err
	}
	scanner := exec.NewScanner(rows)
	defer func() { _ = scanner.Close() }()
	return scanner.ScanStructs(result)
}

// queryAndScanStruct runs SELECT and scans its result into single struct, result is a pointer to struct
func queryAndScanStruct(q Querier, query *goqu.SelectDataset, result interface{}) error {
	if query.GetClauses().IsDefaultSelect() {
		query = query.Select(result)
	}

	rows, err := BuildSQLAndQuery(q, query)
	if err != nil {
		return err
	}
	scanner := exec.NewScanner(rows)
	if !scanner.Next() {
		return ErrNotFound
	}
	defer func() { _ = scanner.Close() }()
	return scanner.ScanStruct(result)
}

// QueryAndScanValues runs SELECT and scans its result into values list, result is a pointer to slice of values:
// SELECT attr FROM t WHERE t.id > 123
func QueryAndScanValues(q Querier, query *goqu.SelectDataset, result interface{}) error {
	rows, err := BuildSQLAndQuery(q, query)
	if err != nil {
		return err
	}
	scanner := exec.NewScanner(rows)
	defer func() { _ = scanner.Close() }()
	return scanner.ScanVals(result)
}

func prepareSelectsForCompositeRecord(query *goqu.SelectDataset, structTyp interface{}) []interface{} {
	// prepare SELECT with default values using COALESCE:
	// SELECT COALESCE(t1.col, ?) AS `t1.col`, ...
	// this is needed to support LEFT JOINs when composite
	// members do not allow scanning NULL database values
	rec, _ := exp.NewRecordFromStruct(structTyp, false, false)
	var selects []interface{}

	type colV struct {
		col      string
		defaultV interface{}
	}
	cols := make([]colV, 0, len(rec))
	for col, defaultV := range rec {
		cols = append(cols, colV{col, defaultV})
	}

	sort.Slice(cols, func(i, j int) bool {
		return cols[i].col < cols[j].col
	})

	dialectSqlite := query.Dialect().Dialect() == string(dbkit.DialectSQLite)
	for i := range cols {
		col, defaultV := cols[i].col, cols[i].defaultV
		var selectExp exp.Expression
		_, timeColumn := defaultV.(time.Time)

		// 1. sqlite+time         - as is
		// 2. sqlite+non-time     - coalesce
		// 3. non-sqlite+time     - coalesce+cast
		// 4. non-sqlite+non-time - coalesce
		if dialectSqlite && timeColumn {
			selectExp = goqu.I(col)
		} else {
			selectExp = goqu.COALESCE(goqu.I(col), defaultV)
			if !dialectSqlite && timeColumn {
				selectExp = goqu.Cast(selectExp, "DATETIME")
			}
		}
		selects = append(selects, exp.NewAliasExpression(selectExp, exp.NewIdentifierExpression("", "", col)))
	}
	return selects
}

// QueryAndScanStructs scans results into structs (using common goqu rules about tags)
// it allows scanning from queries that contain JOINs between tables other than INNER JOIN
func QueryAndScanStructs(q Querier, query *goqu.SelectDataset, composite interface{}) error {
	if query.GetClauses().IsDefaultSelect() && len(query.GetClauses().Joins()) > 0 {
		elem := reflect.New(reflect.TypeOf(reflect.ValueOf(composite).Elem().Interface()).Elem())
		selects := prepareSelectsForCompositeRecord(query, reflect.Indirect(reflect.ValueOf(elem.Interface())).Interface())
		query = query.Select(selects...)
	}
	if err := queryAndScanStructs(q, query, composite); err != nil {
		return fmt.Errorf("composite structs query: %w", err)
	}
	return nil
}

// QueryAndScanStruct scans results into composite struct
func QueryAndScanStruct(q Querier, query *goqu.SelectDataset, composite interface{}) error {
	if query.GetClauses().IsDefaultSelect() && len(query.GetClauses().Joins()) > 0 {
		v := reflect.Indirect(reflect.ValueOf(composite))
		selects := prepareSelectsForCompositeRecord(query, v.Interface())
		query = query.Select(selects...)
	}
	if err := queryAndScanStruct(q, query, composite); err != nil {
		if errors.Is(err, ErrNotFound) {
			return ErrNotFound
		}
		return fmt.Errorf("composite struct query: %w", err)
	}
	return nil
}
