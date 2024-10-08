/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

// Package mysql provides helpers for working MySQL database.
// Should be imported explicitly.
// To register mysql as retryable func use side effect import like so:
//
//	import _ "github.com/acronis/go-dbkit/mysql"
package mysql

import (
	"errors"

	"github.com/go-sql-driver/mysql"

	"github.com/acronis/go-dbkit"
)

// nolint
func init() {
	db.RegisterIsRetryableFunc(&mysql.MySQLDriver{}, func(err error) bool {
		if mysqlErr, ok := err.(*mysql.MySQLError); ok {
			switch mysqlErr.Number {
			case uint16(MySQLErrDeadlock), uint16(MySQLErrLockTimedOut):
				return true
			}
		}
		if err == mysql.ErrInvalidConn {
			return true
		}
		return false
	})
}

// MySQLErrCode defines the type for MySQL error codes.
// nolint: revive
type MySQLErrCode uint16

// MySQL error codes (will be filled gradually).
const (
	MySQLErrCodeDupEntry MySQLErrCode = 1062
	MySQLErrDeadlock     MySQLErrCode = 1213
	MySQLErrLockTimedOut MySQLErrCode = 1205
)

// CheckMySQLError checks if the passed error relates to MySQL and it's internal code matches the one from the argument.
func CheckMySQLError(err error, errCode MySQLErrCode) bool {
	var mySQLError *mysql.MySQLError
	if ok := errors.As(err, &mySQLError); ok {
		return mySQLError.Number == uint16(errCode)
	}
	return false
}
