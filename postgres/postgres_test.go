/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package postgres

import (
	"database/sql/driver"
	"fmt"
	"testing"

	pg "github.com/lib/pq"
	"github.com/stretchr/testify/require"

	"github.com/acronis/go-dbkit"
)

func TestPostgresIsRetryable(t *testing.T) {
	isRetryable := dbkit.GetIsRetryable(&pg.Driver{})
	require.NotNil(t, isRetryable)
	require.True(t, isRetryable(&pg.Error{Code: "40P01"}))
	require.False(t, isRetryable(driver.ErrBadConn))
	require.True(t, isRetryable(fmt.Errorf("wrapped error: %w", &pg.Error{Code: "40P01"})))
}
