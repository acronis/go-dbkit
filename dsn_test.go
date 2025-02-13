/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package dbkit

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMakeMySQLDSN(t *testing.T) {
	cfg := &MySQLConfig{
		Host:     "myhost",
		Port:     3307,
		User:     "myadmin",
		Password: "mypassword",
		Database: "mydb",
	}
	wantDSN := "myadmin:mypassword@tcp(myhost:3307)/mydb?multiStatements=true&parseTime=true&autocommit=false"
	gotDSN := MakeMySQLDSN(cfg)
	require.Equal(t, wantDSN, gotDSN)
}

func TestMakePostgresDSN(t *testing.T) {
	tests := []struct {
		Name    string
		Cfg     *PostgresConfig
		WantDSN string
	}{
		{
			Name: "search_path is used",
			Cfg: &PostgresConfig{
				Host:                 "pghost",
				Port:                 5433,
				User:                 "pgadmin",
				Password:             "pgpassword",
				Database:             "pgdb",
				SSLMode:              PostgresSSLModeRequire,
				SearchPath:           "pgsearch",
				AdditionalParameters: map[string]string{"param1": "foo", "param2": "bar"},
			},
			WantDSN: "postgres://pgadmin:pgpassword@pghost:5433/pgdb?sslmode=require&search_path=pgsearch&param1=foo&param2=bar",
		},
		{
			Name: "base",
			Cfg: &PostgresConfig{
				Host:                 "pghost",
				Port:                 5433,
				User:                 "pgadmin",
				Password:             "pgpassword",
				Database:             "pgdb",
				SSLMode:              PostgresSSLModeRequire,
				AdditionalParameters: map[string]string{"param1": "Lorem ipsum"},
			},
			WantDSN: "postgres://pgadmin:pgpassword@pghost:5433/pgdb?sslmode=require&param1=Lorem+ipsum",
		},
	}
	for i := range tests {
		tt := tests[i]
		t.Run(tt.Name, func(t *testing.T) {
			require.Equal(t, MakePostgresDSN(tt.Cfg), tt.WantDSN)
		})
	}
}

func TestMakePgSQLDSN(t *testing.T) {
	cfg := &PostgresConfig{
		Host:             "myhost",
		TxIsolationLevel: IsolationLevel(sql.LevelReadCommitted),
		Port:             5432,
		User:             "myadmin",
		Password:         "mypassword",
		Database:         "mydb",
	}
	wantDSN := "postgres://myadmin:mypassword@myhost:5432/mydb?sslmode=verify-ca"
	gotDSN := MakePostgresDSN(cfg)
	require.Equal(t, wantDSN, gotDSN)
}

func TestMakeMSSQLDSN(t *testing.T) {
	cfg := &MSSQLConfig{
		Host:             "myhost",
		TxIsolationLevel: IsolationLevel(sql.LevelReadCommitted),
		Port:             1433,
		User:             "myadmin",
		Password:         "mypassword",
		Database:         "sysdb",
	}
	wantDSN := "sqlserver://myadmin:mypassword@myhost:1433?database=sysdb"
	gotDSN := MakeMSSQLDSN(cfg)
	require.Equal(t, wantDSN, gotDSN)
}
