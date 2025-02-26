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
	"errors"

	mssql "github.com/microsoft/go-mssqldb"

	"github.com/acronis/go-dbkit"
)

// nolint
func init() {
	dbkit.RegisterIsRetryableFunc(&mssql.Driver{}, func(err error) bool {
		var msErr mssql.Error
		if errors.As(err, &msErr) {
			if msErr.Number == int32(ErrDeadlock) { // deadlock error
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
	ErrDeadlock                 ErrCode = 1205
	ErrCodeUniqueViolation      ErrCode = 2627
	ErrCodeUniqueIndexViolation ErrCode = 2601
)

// CheckMSSQLError checks if the passed error relates to MSSQL,
// and it's internal code matches the one from the argument.
func CheckMSSQLError(err error, errCode ErrCode) bool {
	var msErr mssql.Error
	if errors.As(err, &msErr) {
		return msErr.SQLErrorNumber() == int32(errCode)
	}
	return false
}
