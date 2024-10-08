/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

// Package sqlite provides helpers for working SQLite database.
// Should be imported explicitly.
// To register sqlite as retryable func use side effect import like so:
//
//	import _ "github.com/acronis/go-dbkit/sqlite"
package sqlite

import (
	sqlite3 "github.com/mattn/go-sqlite3"

	"github.com/acronis/go-dbkit"
)

// nolint
func init() {
	db.RegisterIsRetryableFunc(&sqlite3.SQLiteDriver{}, func(err error) bool {
		if sqliteErr, ok := err.(sqlite3.Error); ok {
			switch sqliteErr.Code {
			case sqlite3.ErrLocked, sqlite3.ErrBusy:
				return true
			}
		}
		return false
	})
}

// CheckSQLiteError checks if the passed error relates to SQLite and it's internal code matches the one from the argument.
func CheckSQLiteError(err error, errCode sqlite3.ErrNoExtended) bool {
	if sqliteErr, ok := err.(sqlite3.Error); ok {
		return sqliteErr.ExtendedCode == errCode
	}
	return false
}
