/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package mssql

import (
	"database/sql/driver"
	"fmt"
	"testing"

	mssql "github.com/denisenkom/go-mssqldb"
	"github.com/stretchr/testify/require"

	"github.com/acronis/go-dbkit"
)

func TestMSSQLIsRetryable(t *testing.T) {
	isRetryable := db.GetIsRetryable(&mssql.Driver{})
	require.NotNil(t, isRetryable)
	require.True(t, isRetryable(mssql.Error{Number: 1205}))
	require.False(t, isRetryable(driver.ErrBadConn))
	require.True(t, isRetryable(fmt.Errorf("wrapped error: %w", mssql.Error{Number: 1205})))
}
