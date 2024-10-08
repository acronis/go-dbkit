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

func TestMakePostgresDSN(t *testing.T) {
	tests := []struct {
		Name    string
		Cfg     *dbkit.PostgresConfig
		WantDSN string
	}{
		{
			Name: "search_path is used",
			Cfg: &dbkit.PostgresConfig{
				Host:       "pghost",
				Port:       5433,
				User:       "pgadmin",
				Password:   "pgpassword",
				Database:   "pgdb",
				SSLMode:    dbkit.PostgresSSLModeRequire,
				SearchPath: "pgsearch",
			},
			WantDSN: "postgres://pgadmin:pgpassword@pghost:5433/pgdb?sslmode=require&search_path=pgsearch",
		},
		{
			Name: "base",
			Cfg: &dbkit.PostgresConfig{
				Host:     "pghost",
				Port:     5433,
				User:     "pgadmin",
				Password: "pgpassword",
				Database: "pgdb",
				SSLMode:  dbkit.PostgresSSLModeRequire,
			},
			WantDSN: "postgres://pgadmin:pgpassword@pghost:5433/pgdb?sslmode=require",
		},
	}
	for i := range tests {
		tt := tests[i]
		t.Run(tt.Name, func(t *testing.T) {
			require.Equal(t, dbkit.MakePostgresDSN(tt.Cfg), tt.WantDSN)
		})
	}
}

func TestPostgresIsRetryable(t *testing.T) {
	isRetryable := dbkit.GetIsRetryable(&pg.Driver{})
	require.NotNil(t, isRetryable)
	require.True(t, isRetryable(&pg.Error{Code: "40P01"}))
	require.False(t, isRetryable(driver.ErrBadConn))
	require.True(t, isRetryable(fmt.Errorf("wrapped error: %w", &pg.Error{Code: "40P01"})))
}
