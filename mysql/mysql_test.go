/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package mysql

import (
	"database/sql/driver"
	"fmt"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/acronis/go-dbkit"
)

func TestMakeMySQLDSN(t *testing.T) {
	cfg := &dbkit.MySQLConfig{
		Host:     "myhost",
		Port:     3307,
		User:     "myadmin",
		Password: "mypassword",
		Database: "mydb",
	}
	wantDSN := "myadmin:mypassword@tcp(myhost:3307)/mydb?multiStatements=true&parseTime=true&autocommit=false"
	gotDSN := dbkit.MakeMySQLDSN(cfg)
	require.Equal(t, wantDSN, gotDSN)
}

func TestMysqlIsRetryable(t *testing.T) {
	isRetryable := dbkit.GetIsRetryable(&mysql.MySQLDriver{})
	require.NotNil(t, isRetryable)
	require.True(t, isRetryable(&mysql.MySQLError{
		Number: uint16(MySQLErrDeadlock),
	}))
	require.True(t, isRetryable(&mysql.MySQLError{
		Number: uint16(MySQLErrLockTimedOut),
	}))
	require.True(t, isRetryable(mysql.ErrInvalidConn))
	require.False(t, isRetryable(driver.ErrBadConn))
	require.True(t, isRetryable(fmt.Errorf("wrapped error: %w", &mysql.MySQLError{
		Number: uint16(MySQLErrDeadlock),
	})))
}

// TestCheckMySQLError covers behavior of CheckMySQLError func.
func TestCheckMySQLError(t *testing.T) {
	var deadlockErr MySQLErrCode = 1213
	sqlErr := &mysql.MySQLError{
		Number:  1213,
		Message: "deadlock found when trying to get lock",
	}

	wrapperSQLErr := fmt.Errorf("wrapped error: %w", sqlErr)

	require.True(t, CheckMySQLError(sqlErr, deadlockErr))
	require.True(t, CheckMySQLError(wrapperSQLErr, deadlockErr))
}
