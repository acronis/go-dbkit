/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

// Package pgx provides helpers for working Postgres database via jackc/pgx driver.
// Should be imported explicitly.
// To register postgres as retryable func use side effect import like so:
//
//	import _ "github.com/acronis/go-dbkit/pgx"
package pgx

import (
	"github.com/jackc/pgconn"
	pg "github.com/jackc/pgx/v4/stdlib"

	"github.com/acronis/go-dbkit"
)

// nolint
func init() {
	dbkit.RegisterIsRetryableFunc(&pg.Driver{}, func(err error) bool {
		if pgErr, ok := err.(*pgconn.PgError); ok {
			switch errCode := dbkit.PostgresErrCode(pgErr.Code); errCode {
			case dbkit.PgxErrCodeDeadlockDetected:
				return true
			case dbkit.PgxErrCodeSerializationFailure:
				return true
			}
			if checkInvalidCachedPlanPgError(pgErr) {
				return true
			}
		}
		return false
	})
}

// CheckPostgresError checks if the passed error relates to Postgres,
// and it's internal code matches the one from the argument.
func CheckPostgresError(err error, errCode dbkit.PostgresErrCode) bool {
	if pgErr, ok := err.(*pgconn.PgError); ok {
		return pgErr.Code == string(errCode)
	}
	return false
}

// CheckInvalidCachedPlanError checks if the passed error is related to the invalid cached plan.
// By default, https://github.com/jackc/pgx has a cache for prepared statements
// (https://github.com/jackc/pgx/wiki/Automatic-Prepared-Statement-Caching),
// which can lead to the error "cached plan must not change result type (SQLSTATE 0A000)"
// for queries like `SELECT * FROM table` in case of the schema changes (e.g. column was added during the migration).
// It's recommended to handle this error as retryable since the statement cache will be invalidated,
// and the query will be re-prepared (it's done automatically by the driver).
func CheckInvalidCachedPlanError(err error) bool {
	if pgErr, ok := err.(*pgconn.PgError); ok {
		return checkInvalidCachedPlanPgError(pgErr)
	}
	return false
}

// checkInvalidCachedPlanPgError checks if the passed *pgconn.PgError is related to the invalid cached plan.
// Source: https://github.com/jackc/pgconn/blob/9cf57526250f6cd3e6cbf4fd7269c882e66898ce/stmtcache/lru.go#L91-L103
func checkInvalidCachedPlanPgError(pgErr *pgconn.PgError) bool {
	return pgErr.Severity == "ERROR" &&
		pgErr.Code == string(dbkit.PgxErrFeatureNotSupported) &&
		pgErr.Message == "cached plan must not change result type"
}
