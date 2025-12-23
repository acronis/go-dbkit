/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package dbkit

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/acronis/go-appkit/config"
	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

type AppConfig struct {
	DB *Config `mapstructure:"db" json:"db" yaml:"db"`
}

func TestConfig(t *testing.T) {
	supportedDialects := []Dialect{DialectSQLite, DialectMySQL, DialectPostgres, DialectPgx, DialectMSSQL}

	tests := []struct {
		name        string
		cfgData     string
		expectedCfg func() *Config
	}{
		{
			name: "mysql dialect",
			cfgData: `
db:
  maxOpenConns: 20
  maxIdleConns: 10
  connMaxLifeTime: 2m
  dialect: mysql
  mysql:
    host: mysql-host
    port: 3307
    database: mysql_db
    user: mysql-user
    password: mysql-password
    txLevel: "Repeatable Read"
`,
			expectedCfg: func() *Config {
				cfg := NewDefaultConfig(supportedDialects)
				cfg.Dialect = DialectMySQL
				cfg.MaxOpenConns = 20
				cfg.MaxIdleConns = 10
				cfg.ConnMaxLifetime = config.TimeDuration(2 * time.Minute)
				cfg.MySQL.Host = "mysql-host"
				cfg.MySQL.Port = 3307
				cfg.MySQL.Database = "mysql_db"
				cfg.MySQL.User = "mysql-user"
				cfg.MySQL.Password = "mysql-password"
				cfg.MySQL.TxIsolationLevel = IsolationLevel(sql.LevelRepeatableRead)
				return cfg
			},
		},
		{
			name: "postgres dialect, github.com/lib/pq driver",
			cfgData: `
db:
  dialect: postgres
  postgres:
    host: pg-host
    port: 5433
    database: pg_db
    user: pg-user
    password: pg-password
    txLevel: "Read Committed"
    sslMode: verify-full
    searchPath: pg-search
`,
			expectedCfg: func() *Config {
				cfg := NewDefaultConfig(supportedDialects)
				cfg.Dialect = DialectPostgres
				cfg.Postgres.Host = "pg-host"
				cfg.Postgres.Port = 5433
				cfg.Postgres.Database = "pg_db"
				cfg.Postgres.User = "pg-user"
				cfg.Postgres.Password = "pg-password"
				cfg.Postgres.TxIsolationLevel = IsolationLevel(sql.LevelReadCommitted)
				cfg.Postgres.SSLMode = PostgresSSLModeVerifyFull
				cfg.Postgres.SearchPath = "pg-search"
				return cfg
			},
		},
		{
			name: "postgres dialect, github.com/jackc/pgx driver",
			cfgData: `
db:
  dialect: pgx
  postgres:
    host: pg-host
    port: 5433
    database: pg_db
    user: pg-user
    password: pg-password
    txLevel: "Serializable"
    sslMode: verify-full
    searchPath: pg-search
`,
			expectedCfg: func() *Config {
				cfg := NewDefaultConfig(supportedDialects)
				cfg.Dialect = DialectPgx
				cfg.Postgres.Host = "pg-host"
				cfg.Postgres.Port = 5433
				cfg.Postgres.Database = "pg_db"
				cfg.Postgres.User = "pg-user"
				cfg.Postgres.Password = "pg-password"
				cfg.Postgres.TxIsolationLevel = IsolationLevel(sql.LevelSerializable)
				cfg.Postgres.SSLMode = PostgresSSLModeVerifyFull
				cfg.Postgres.SearchPath = "pg-search"
				return cfg
			},
		},
		{
			name: "postgres dialect, github.com/jackc/pgx driver, overridden target_session_attrs",
			cfgData: `
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
`,
			expectedCfg: func() *Config {
				cfg := NewDefaultConfig(supportedDialects)
				cfg.Dialect = DialectPgx
				cfg.Postgres.Host = "pg-host"
				cfg.Postgres.Port = 5433
				cfg.Postgres.Database = "pg_db"
				cfg.Postgres.User = "pg-user"
				cfg.Postgres.Password = "pg-password"
				cfg.Postgres.TxIsolationLevel = IsolationLevel(sql.LevelRepeatableRead)
				cfg.Postgres.SSLMode = PostgresSSLModeVerifyFull
				cfg.Postgres.SearchPath = "pg-search"
				cfg.Postgres.AdditionalParameters = map[string]string{"target_session_attrs": "read-only"}
				return cfg
			},
		},
		{
			name: "mssql dialect",
			cfgData: `
db:
  dialect: mssql
  mssql:
    host: mssql-host
    port: 1433
    database: mssql_db
    user: mssql-user
    password: mssql-password
    txLevel: Repeatable Read
`,
			expectedCfg: func() *Config {
				cfg := NewDefaultConfig(supportedDialects)
				cfg.Dialect = DialectMSSQL
				cfg.MSSQL.Host = "mssql-host"
				cfg.MSSQL.Port = 1433
				cfg.MSSQL.Database = "mssql_db"
				cfg.MSSQL.User = "mssql-user"
				cfg.MSSQL.Password = "mssql-password"
				cfg.MSSQL.TxIsolationLevel = IsolationLevel(sql.LevelRepeatableRead)
				return cfg
			},
		},
		{
			name: "mssql dialect overridden encrypt",
			cfgData: `
db:
  dialect: mssql
  mssql:
    host: mssql-host
    port: 1433
    database: mssql_db
    user: mssql-user
    password: mssql-password
    txLevel: Repeatable Read
    additionalParameters:
      encrypt: DISABLE
`,
			expectedCfg: func() *Config {
				cfg := NewDefaultConfig(supportedDialects)
				cfg.Dialect = DialectMSSQL
				cfg.MSSQL.Host = "mssql-host"
				cfg.MSSQL.Port = 1433
				cfg.MSSQL.Database = "mssql_db"
				cfg.MSSQL.User = "mssql-user"
				cfg.MSSQL.Password = "mssql-password"
				cfg.MSSQL.TxIsolationLevel = IsolationLevel(sql.LevelRepeatableRead)
				cfg.MSSQL.AdditionalParameters = map[string]string{"encrypt": "DISABLE"}
				return cfg
			},
		},
		{
			name: "sqlite dialect",
			cfgData: `
db:
  maxOpenConns: 20
  maxIdleConns: 10
  connMaxLifeTime: 1m
  dialect: sqlite3
  sqlite3:
    path: ":memory:"
`,
			expectedCfg: func() *Config {
				cfg := NewDefaultConfig(supportedDialects)
				cfg.Dialect = DialectSQLite
				cfg.MaxOpenConns = 20
				cfg.MaxIdleConns = 10
				cfg.ConnMaxLifetime = config.TimeDuration(time.Minute)
				cfg.SQLite.Path = ":memory:"
				return cfg
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, dataType := range []config.DataType{config.DataTypeYAML, config.DataTypeJSON} {
				cfgData := tt.cfgData
				if dataType == config.DataTypeJSON {
					cfgData = string(mustYAMLToJSON([]byte(cfgData)))
				}

				// Load config using config.Loader.
				appCfg := AppConfig{DB: NewDefaultConfig(supportedDialects)}
				expectedAppCfg := AppConfig{DB: tt.expectedCfg()}
				if expectedAppCfg.DB.Dialect == DialectPgx && expectedAppCfg.DB.Postgres.AdditionalParameters == nil {
					expectedAppCfg.DB.Postgres.AdditionalParameters = map[string]string{"target_session_attrs": "read-write"}
				}
				cfgLoader := config.NewLoader(config.NewViperAdapter())
				err := cfgLoader.LoadFromReader(bytes.NewBuffer([]byte(cfgData)), dataType, appCfg.DB)
				require.NoError(t, err)
				require.Equal(t, expectedAppCfg, appCfg)

				// Load config using viper unmarshal.
				appCfg = AppConfig{DB: NewDefaultConfig(supportedDialects)}
				expectedAppCfg = AppConfig{DB: tt.expectedCfg()}
				vpr := viper.New()
				vpr.SetConfigType(string(dataType))
				require.NoError(t, vpr.ReadConfig(bytes.NewBuffer([]byte(cfgData))))
				require.NoError(t, vpr.Unmarshal(&appCfg, func(c *mapstructure.DecoderConfig) {
					c.DecodeHook = mapstructure.TextUnmarshallerHookFunc()
				}))
				require.Equal(t, expectedAppCfg, appCfg)

				// Load config using yaml/json unmarshal.
				appCfg = AppConfig{DB: NewDefaultConfig(supportedDialects)}
				expectedAppCfg = AppConfig{DB: tt.expectedCfg()}
				switch dataType {
				case config.DataTypeYAML:
					require.NoError(t, yaml.Unmarshal([]byte(cfgData), &appCfg))
					require.Equal(t, expectedAppCfg, appCfg)
				case config.DataTypeJSON:
					require.NoError(t, json.Unmarshal([]byte(cfgData), &appCfg))
					require.Equal(t, expectedAppCfg, appCfg)
				}
			}
		})
	}
}

func TestConfigWithKeyPrefix(t *testing.T) {
	t.Run("custom key prefix", func(t *testing.T) {
		cfgData := `
customDb:
  dialect: mysql
  mysql:
    host: mysql-host
    port: 3307
`
		cfg := NewConfig([]Dialect{DialectMySQL}, WithKeyPrefix("customDb"))
		err := config.NewDefaultLoader("").LoadFromReader(bytes.NewBuffer([]byte(cfgData)), config.DataTypeYAML, cfg)
		require.NoError(t, err)
		require.Equal(t, DialectMySQL, cfg.Dialect)
		require.Equal(t, "mysql-host", cfg.MySQL.Host)
		require.Equal(t, 3307, cfg.MySQL.Port)
	})

	t.Run("default key prefix, empty struct initialization", func(t *testing.T) {
		cfgData := `
db:
  dialect: mysql
  mysql:
    host: mysql-host
    port: 3307
`
		cfg := &Config{}
		err := config.NewDefaultLoader("").LoadFromReader(bytes.NewBuffer([]byte(cfgData)), config.DataTypeYAML, cfg)
		require.NoError(t, err)
		require.Equal(t, DialectMySQL, cfg.Dialect)
		require.Equal(t, "mysql-host", cfg.MySQL.Host)
		require.Equal(t, 3307, cfg.MySQL.Port)
	})
}

func TestConfigValidationErrors(t *testing.T) {
	supportedDialects := []Dialect{DialectSQLite, DialectMySQL, DialectPostgres, DialectPgx, DialectMSSQL}

	tests := []struct {
		name           string
		yamlData       string
		expectedErrMsg string
	}{
		{
			name: "unknown dialect",
			yamlData: `
db:
  dialect: fake-dialect
`,
			expectedErrMsg: `db.dialect: unknown value "fake-dialect", should be one of [sqlite3 mysql postgres pgx mssql]`,
		},
		{
			name: "invalid max open connections",
			yamlData: `
db:
  dialect: mysql
  maxOpenConns: -1
`,
			expectedErrMsg: `db.maxOpenConns: must be positive`,
		},
		{
			name: "invalid max idel connections",
			yamlData: `
db:
  dialect: mysql
  maxIdleConns: -1
`,
			expectedErrMsg: `db.maxIdleConns: must be positive`,
		},
		{
			name: "max idle connections greater than max open connections",
			yamlData: `
db:
  dialect: mysql
  maxOpenConns: 5
  maxIdleConns: 10
`,
			expectedErrMsg: `db.maxIdleConns: must be less than maxOpenConns`,
		},
		{
			name: "invalid connection max lifetime",
			yamlData: `
db:
  dialect: mysql
  connMaxLifeTime: "invalid-duration"
`,
			expectedErrMsg: `db.connMaxLifeTime: time: invalid duration "invalid-duration"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := NewConfig(supportedDialects)
			err := config.NewDefaultLoader("").LoadFromReader(bytes.NewBuffer([]byte(tt.yamlData)), config.DataTypeYAML, cfg)
			require.EqualError(t, err, tt.expectedErrMsg)
		})
	}
}

func mustYAMLToJSON(yamlData []byte) []byte {
	var yamlMap map[string]interface{}
	if err := yaml.Unmarshal(yamlData, &yamlMap); err != nil {
		panic(err)
	}
	jsonData, err := json.MarshalIndent(yamlMap, "", "  ")
	if err != nil {
		panic(err)
	}
	return jsonData
}
