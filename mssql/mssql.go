/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

// Package mssql provides helpers for working MSSQL database.
// Should be imported explicitly.
// To register mssql as retryable func use side effect import like so:
//
//	import _ "github.com/acronis/go-dbkit/mssql"
package mssql

import (
	mssql "github.com/denisenkom/go-mssqldb"

	"github.com/acronis/go-dbkit"
)

// nolint
func init() {
	dbkit.RegisterIsRetryableFunc(&mssql.Driver{}, func(err error) bool {
		if msErr, ok := err.(mssql.Error); ok {
			if msErr.Number == int32(MSSQLErrDeadlock) { // deadlock error
				return true
			}
		}
		return false
	})
}

// ErrCode defines the type for MSSQL error codes.
type ErrCode int32

// MSSQL error codes (will be filled gradually).
const (
	MSSQLErrDeadlock                 ErrCode = 1205
	MSSQLErrCodeUniqueViolation      ErrCode = 2627
	MSSQLErrCodeUniqueIndexViolation ErrCode = 2601
)

// CheckMSSQLError checks if the passed error relates to MSSQL and it's internal code matches the one from the argument.
func CheckMSSQLError(err error, errCode ErrCode) bool {
	if sqlErr, ok := err.(mssql.Error); ok {
		return sqlErr.SQLErrorNumber() == int32(errCode)
	}
	return false
}
