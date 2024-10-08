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

func TestMakePgSQLDSN(t *testing.T) {
	cfg := &PostgresConfig{
		Host:             "myhost",
		TxIsolationLevel: sql.LevelReadCommitted,
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
		TxIsolationLevel: sql.LevelReadCommitted,
		Port:             1433,
		User:             "myadmin",
		Password:         "mypassword",
		Database:         "sysdb",
	}
	wantDSN := "sqlserver://myadmin:mypassword@myhost:1433?database=sysdb"
	gotDSN := MakeMSSQLDSN(cfg)
	require.Equal(t, wantDSN, gotDSN)
}
