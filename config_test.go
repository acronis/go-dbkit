/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package db

import (
	"bytes"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/acronis/go-appkit/config"
)

func TestConfig(t *testing.T) {
	allDialects := []Dialect{DialectSQLite, DialectMySQL, DialectPostgres, DialectPgx, DialectMSSQL}

	t.Run("unknown dialect", func(t *testing.T) {
		cfgData := bytes.NewBufferString(`
db:
  dialect: fake-dialect
`)
		cfg := NewConfig(allDialects)
		err := config.NewDefaultLoader("").LoadFromReader(cfgData, config.DataTypeYAML, cfg)
		require.EqualError(t, err, `db.dialect: unknown value "fake-dialect", should be one of [sqlite3 mysql postgres pgx mssql]`)
	})

	t.Run("read mysql parameters", func(t *testing.T) {
		cfgData := bytes.NewBufferString(`
db:
  dialect: mysql
  mysql:
    host: mysql-host
    port: 3307
    database: mysql_db
    user: mysql-user
    password: mysql-password
    txLevel: Repeatable Read
`)
		cfg := NewConfig(allDialects)
		err := config.NewDefaultLoader("").LoadFromReader(cfgData, config.DataTypeYAML, cfg)
		require.NoError(t, err)
		require.Equal(t, DialectMySQL, cfg.Dialect)
		wantMySQLCfg := MySQLConfig{
			Host:             "mysql-host",
			Port:             3307,
			Database:         "mysql_db",
			User:             "mysql-user",
			Password:         "mysql-password",
			TxIsolationLevel: sql.LevelRepeatableRead,
		}
		require.Equal(t, wantMySQLCfg, cfg.MySQL)
	})

	t.Run("read postgres (lib/pq) parameters", func(t *testing.T) {
		cfgData := bytes.NewBufferString(`
db:
  dialect: postgres
  postgres:
    host: pg-host
    port: 5433
    database: pg_db
    user: pg-user
    password: pg-password
    txLevel: Repeatable Read
    sslMode: verify-full
    searchPath: pg-search
`)
		cfg := NewConfig(allDialects)
		err := config.NewDefaultLoader("").LoadFromReader(cfgData, config.DataTypeYAML, cfg)
		require.NoError(t, err)
		require.Equal(t, DialectPostgres, cfg.Dialect)
		wantPostgresCfg := PostgresConfig{
			Host:             "pg-host",
			Port:             5433,
			Database:         "pg_db",
			User:             "pg-user",
			Password:         "pg-password",
			TxIsolationLevel: sql.LevelRepeatableRead,
			SSLMode:          PostgresSSLModeVerifyFull,
			SearchPath:       "pg-search",
		}
		require.Equal(t, wantPostgresCfg, cfg.Postgres)
	})

	t.Run("read postgres (pgx) parameters", func(t *testing.T) {
		cfgData := bytes.NewBufferString(`
db:
  dialect: pgx
  postgres:
    host: pg-host
    port: 5433
    database: pg_db
    user: pg-user
    password: pg-password
    txLevel: Repeatable Read
    sslMode: verify-full
    searchPath: pg-search
`)
		cfg := NewConfig(allDialects)
		err := config.NewDefaultLoader("").LoadFromReader(cfgData, config.DataTypeYAML, cfg)
		require.NoError(t, err)
		require.Equal(t, DialectPgx, cfg.Dialect)
		wantPostgresCfg := PostgresConfig{
			Host:                 "pg-host",
			Port:                 5433,
			Database:             "pg_db",
			User:                 "pg-user",
			Password:             "pg-password",
			TxIsolationLevel:     sql.LevelRepeatableRead,
			SSLMode:              PostgresSSLModeVerifyFull,
			SearchPath:           "pg-search",
			AdditionalParameters: []Parameter{{Name: "target_session_attrs", Value: "read-write"}},
		}
		require.Equal(t, wantPostgresCfg, cfg.Postgres)
	})

	t.Run("read postgres (pgx) parameters with overridden target_session_attrs", func(t *testing.T) {
		cfgData := bytes.NewBufferString(`
db:
  dialect: pgx
  postgres:
    host: pg-host
    port: 5433
    database: pg_db
    user: pg-user
    password: pg-password
    txLevel: Repeatable Read
    sslMode: verify-full
    searchPath: pg-search
    additionalParameters:
      target_session_attrs: read-only
`)
		cfg := NewConfig(allDialects)
		err := config.NewDefaultLoader("").LoadFromReader(cfgData, config.DataTypeYAML, cfg)
		require.NoError(t, err)
		require.Equal(t, DialectPgx, cfg.Dialect)
		wantPostgresCfg := PostgresConfig{
			Host:                 "pg-host",
			Port:                 5433,
			Database:             "pg_db",
			User:                 "pg-user",
			Password:             "pg-password",
			TxIsolationLevel:     sql.LevelRepeatableRead,
			SSLMode:              PostgresSSLModeVerifyFull,
			SearchPath:           "pg-search",
			AdditionalParameters: []Parameter{{Name: "target_session_attrs", Value: "read-only"}},
		}
		require.Equal(t, wantPostgresCfg, cfg.Postgres)
	})

	t.Run("read mssql parameters", func(t *testing.T) {
		cfgData := bytes.NewBufferString(`
db:
  dialect: mssql
  mssql:
    host: mssql-host
    port: 1433
    database: mssql_db
    user: mssql-user
    password: mssql-password
    txLevel: Repeatable Read
`)
		cfg := NewConfig(allDialects)
		err := config.NewDefaultLoader("").LoadFromReader(cfgData, config.DataTypeYAML, cfg)
		require.NoError(t, err)
		require.Equal(t, DialectMSSQL, cfg.Dialect)
		wantMSSQLCfg := MSSQLConfig{
			Host:             "mssql-host",
			Port:             1433,
			Database:         "mssql_db",
			User:             "mssql-user",
			Password:         "mssql-password",
			TxIsolationLevel: sql.LevelRepeatableRead,
		}
		require.Equal(t, wantMSSQLCfg, cfg.MSSQL)
	})

	t.Run("read multiple connection parameters from one source", func(t *testing.T) {
		cfgData := bytes.NewBufferString(`
subsystemA:
  db:
    dialect: mssql
    mssql:
      host: mssql-host
      port: 1433
      database: subsystem_a
      user: mssql-user-a
      password: mssql-password-a
      txLevel: Repeatable Read
subsystemB:
  db:
    dialect: mssql
    mssql:
      host: mssql-host
      port: 1433
      database: subsystem_b
      user: mssql-user-b
      password: mssql-password-b
      txLevel: Read Committed
`)
		cfgA := NewConfigWithKeyPrefix("subsystemA", allDialects)
		cfgB := NewConfigWithKeyPrefix("subsystemB", allDialects)
		err := config.NewDefaultLoader("").LoadFromReader(cfgData, config.DataTypeYAML, cfgA, cfgB)
		require.NoError(t, err)

		require.Equal(t, DialectMSSQL, cfgA.Dialect)
		wantSubSystemACfg := MSSQLConfig{
			Host:             "mssql-host",
			Port:             1433,
			Database:         "subsystem_a",
			User:             "mssql-user-a",
			Password:         "mssql-password-a",
			TxIsolationLevel: sql.LevelRepeatableRead,
		}
		require.Equal(t, wantSubSystemACfg, cfgA.MSSQL)

		require.Equal(t, DialectMSSQL, cfgB.Dialect)
		wantSubSystemBCfg := MSSQLConfig{
			Host:             "mssql-host",
			Port:             1433,
			Database:         "subsystem_b",
			User:             "mssql-user-b",
			Password:         "mssql-password-b",
			TxIsolationLevel: sql.LevelReadCommitted,
		}
		require.Equal(t, wantSubSystemBCfg, cfgB.MSSQL)
	})
}
