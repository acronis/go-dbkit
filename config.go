/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package db

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/acronis/go-appkit/config"
)

const (
	cfgKeyDialect         = "db.dialect"
	cfgKeyMaxIdleConns    = "db.maxIdleConns"
	cfgKeyMaxOpenConns    = "db.maxOpenConns"
	cfgKeyConnMaxLifetime = "db.connMaxLifeTime"

	cfgKeyMySQLHost     = "db.mysql.host"
	cfgKeyMySQLPort     = "db.mysql.port"
	cfgKeyMySQLDatabase = "db.mysql.database"
	cfgKeyMySQLUser     = "db.mysql.user"
	cfgKeyMySQLPassword = "db.mysql.password" //nolint: gosec
	cfgKeyMySQLTxLevel  = "db.mysql.txLevel"

	cfgKeySQLitePath = "db.sqlite3.path"

	cfgKeyPostgresHost             = "db.postgres.host"
	cfgKeyPostgresPort             = "db.postgres.port"
	cfgKeyPostgresDatabase         = "db.postgres.database"
	cfgKeyPostgresUser             = "db.postgres.user"
	cfgKeyPostgresPassword         = "db.postgres.password" //nolint: gosec
	cfgKeyPostgresTxLevel          = "db.postgres.txLevel"
	cfgKeyPostgresSSLMode          = "db.postgres.sslMode"
	cfgKeyPostgresSearchPath       = "db.postgres.searchPath"
	cfgKeyPostgresAdditionalParams = "db.postgres.additionalParameters"
	cfgKeyMSSQLHost                = "db.mssql.host"
	cfgKeyMSSQLPort                = "db.mssql.port"
	cfgKeyMSSQLDatabase            = "db.mssql.database"
	cfgKeyMSSQLUser                = "db.mssql.user"
	cfgKeyMSSQLPassword            = "db.mssql.password" //nolint: gosec
	cfgKeyMSSQLTxLevel             = "db.mssql.txLevel"
)

// MySQLConfig represents a set of configuration parameters for working with MySQL.
type MySQLConfig struct {
	Host             string
	Port             int
	User             string
	Password         string
	Database         string
	TxIsolationLevel sql.IsolationLevel
}

// MSSQLConfig represents a set of configuration parameters for working with MSSQL.
type MSSQLConfig struct {
	Host             string
	Port             int
	User             string
	Password         string
	Database         string
	TxIsolationLevel sql.IsolationLevel
}

// SQLiteConfig represents a set of configuration parameters for working with SQLite.
type SQLiteConfig struct {
	Path string
}

// Parameter represent DB connection parameter. Value will be url-encoded before adding into the connection string.
type Parameter struct {
	Name  string
	Value string
}

// PostgresConfig represents a set of configuration parameters for working with Postgres.
type PostgresConfig struct {
	Host                 string
	Port                 int
	User                 string
	Password             string
	Database             string
	TxIsolationLevel     sql.IsolationLevel
	SSLMode              PostgresSSLMode
	SearchPath           string
	AdditionalParameters []Parameter
}

// Config represents a set of configuration parameters working with SQL databases.
type Config struct {
	Dialect         Dialect
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	MySQL           MySQLConfig
	MSSQL           MSSQLConfig
	SQLite          SQLiteConfig
	Postgres        PostgresConfig

	keyPrefix         string
	supportedDialects []Dialect
}

var _ config.Config = (*Config)(nil)
var _ config.KeyPrefixProvider = (*Config)(nil)

// NewConfig creates a new instance of the Config.
func NewConfig(supportedDialects []Dialect) *Config {
	return NewConfigWithKeyPrefix("", supportedDialects)
}

// NewConfigWithKeyPrefix creates a new instance of the Config.
// Allows to specify key prefix which will be used for parsing configuration parameters.
func NewConfigWithKeyPrefix(keyPrefix string, supportedDialects []Dialect) *Config {
	for _, dialect := range supportedDialects {
		switch dialect {
		case DialectMSSQL, DialectSQLite, DialectPostgres, DialectPgx, DialectMySQL:
		default:
			panic(fmt.Sprintf("unknown dialect %q", string(dialect)))
		}
	}
	return &Config{keyPrefix: keyPrefix, supportedDialects: supportedDialects}
}

// KeyPrefix returns a key prefix with which all configuration parameters should be presented.
func (c *Config) KeyPrefix() string {
	return c.keyPrefix
}

// SupportedDialects returns list of supported dialects.
func (c *Config) SupportedDialects() []Dialect {
	if len(c.supportedDialects) != 0 {
		return c.supportedDialects
	}
	return []Dialect{DialectSQLite, DialectMySQL, DialectPostgres, DialectPgx, DialectMSSQL}
}

// SetProviderDefaults sets default configuration values in config.DataProvider.
func (c *Config) SetProviderDefaults(dp config.DataProvider) {
	dp.SetDefault(cfgKeyMaxOpenConns, DefaultMaxOpenConns)
	dp.SetDefault(cfgKeyMaxIdleConns, DefaultMaxIdleConns)
	dp.SetDefault(cfgKeyConnMaxLifetime, DefaultConnMaxLifetime)
	dp.SetDefault(cfgKeyMySQLTxLevel, MySQLDefaultTxLevel.String())
	dp.SetDefault(cfgKeyPostgresTxLevel, PostgresDefaultTxLevel.String())
	dp.SetDefault(cfgKeyPostgresSSLMode, string(PostgresDefaultSSLMode))
	dp.SetDefault(cfgKeyMSSQLTxLevel, MSSQLDefaultTxLevel.String())
}

// Set sets configuration values from config.DataProvider.
func (c *Config) Set(dp config.DataProvider) error {
	var err error

	err = c.setDialectSpecificConfig(dp)
	if err != nil {
		return err
	}

	var maxOpenConns int
	if maxOpenConns, err = dp.GetInt(cfgKeyMaxOpenConns); err != nil {
		return err
	}
	if maxOpenConns < 0 {
		return dp.WrapKeyErr(cfgKeyMaxOpenConns, fmt.Errorf("must be positive"))
	}
	var maxIdleConns int
	if maxIdleConns, err = dp.GetInt(cfgKeyMaxIdleConns); err != nil {
		return err
	}
	if maxIdleConns < 0 {
		return dp.WrapKeyErr(cfgKeyMaxIdleConns, fmt.Errorf("must be positive"))
	}
	if maxIdleConns > 0 && maxOpenConns > 0 && maxIdleConns > maxOpenConns {
		return dp.WrapKeyErr(cfgKeyMaxIdleConns, fmt.Errorf("must be less than %s", cfgKeyMaxOpenConns))
	}
	c.MaxOpenConns = maxOpenConns
	c.MaxIdleConns = maxIdleConns

	if c.ConnMaxLifetime, err = dp.GetDuration(cfgKeyConnMaxLifetime); err != nil {
		return err
	}

	return nil
}

// TxIsolationLevel returns transaction isolation level from parsed config for specified dialect.
func (c *Config) TxIsolationLevel() sql.IsolationLevel {
	switch c.Dialect {
	case DialectMySQL:
		return c.MySQL.TxIsolationLevel
	case DialectPostgres, DialectPgx:
		return c.Postgres.TxIsolationLevel
	}
	return sql.LevelDefault
}

// DriverNameAndDSN returns driver name and DSN for connecting.
func (c *Config) DriverNameAndDSN() (driverName, dsn string) {
	switch c.Dialect {
	case DialectMySQL:
		return "mysql", MakeMySQLDSN(&c.MySQL)
	case DialectSQLite:
		return "sqlite3", MakeSQLiteDSN(&c.SQLite)
	case DialectPostgres:
		return "postgres", MakePostgresDSN(&c.Postgres)
	case DialectPgx:
		return "pgx", MakePostgresDSN(&c.Postgres)
	case DialectMSSQL:
		return "mssql", MakeMSSQLDSN(&c.MSSQL)
	}
	return "", ""
}

func (c *Config) setDialectSpecificConfig(dp config.DataProvider) error {
	var err error

	var supportedDialectsStr []string
	for _, dialect := range c.SupportedDialects() {
		supportedDialectsStr = append(supportedDialectsStr, string(dialect))
	}
	var dialectStr string
	if dialectStr, err = dp.GetStringFromSet(cfgKeyDialect, supportedDialectsStr, false); err != nil {
		return err
	}
	c.Dialect = Dialect(dialectStr)

	switch c.Dialect {
	case DialectMySQL:
		err = c.setMySQLConfig(dp)
	case DialectSQLite:
		err = c.setSQLiteConfig(dp)
	case DialectPostgres, DialectPgx:
		err = c.setPostgresConfig(dp, c.Dialect)
	case DialectMSSQL:
		err = c.setMSSQLConfig(dp)
	}
	return err
}

// nolint: dupl
func (c *Config) setMySQLConfig(dp config.DataProvider) error {
	var err error

	if c.MySQL.Host, err = dp.GetString(cfgKeyMySQLHost); err != nil {
		return err
	}
	if c.MySQL.Port, err = dp.GetInt(cfgKeyMySQLPort); err != nil {
		return err
	}
	if c.MySQL.User, err = dp.GetString(cfgKeyMySQLUser); err != nil {
		return err
	}
	if c.MySQL.Password, err = dp.GetString(cfgKeyMySQLPassword); err != nil {
		return err
	}
	if c.MySQL.Database, err = dp.GetString(cfgKeyMySQLDatabase); err != nil {
		return err
	}
	if c.MySQL.TxIsolationLevel, err = getIsolationLevel(dp, cfgKeyMySQLTxLevel); err != nil {
		return err
	}

	return nil
}

// nolint: dupl
func (c *Config) setMSSQLConfig(dp config.DataProvider) error {
	var err error

	if c.MSSQL.Host, err = dp.GetString(cfgKeyMSSQLHost); err != nil {
		return err
	}
	if c.MSSQL.Port, err = dp.GetInt(cfgKeyMSSQLPort); err != nil {
		return err
	}
	if c.MSSQL.User, err = dp.GetString(cfgKeyMSSQLUser); err != nil {
		return err
	}
	if c.MSSQL.Password, err = dp.GetString(cfgKeyMSSQLPassword); err != nil {
		return err
	}
	if c.MSSQL.Database, err = dp.GetString(cfgKeyMSSQLDatabase); err != nil {
		return err
	}
	if c.MSSQL.TxIsolationLevel, err = getIsolationLevel(dp, cfgKeyMSSQLTxLevel); err != nil {
		return err
	}

	return nil
}

// nolint: dupl
func (c *Config) setPostgresConfig(dp config.DataProvider, dialect Dialect) error {
	var err error

	if c.Postgres.Host, err = dp.GetString(cfgKeyPostgresHost); err != nil {
		return err
	}
	if c.Postgres.Port, err = dp.GetInt(cfgKeyPostgresPort); err != nil {
		return err
	}
	if c.Postgres.User, err = dp.GetString(cfgKeyPostgresUser); err != nil {
		return err
	}
	if c.Postgres.Password, err = dp.GetString(cfgKeyPostgresPassword); err != nil {
		return err
	}
	if c.Postgres.Database, err = dp.GetString(cfgKeyPostgresDatabase); err != nil {
		return err
	}
	if c.Postgres.SearchPath, err = dp.GetString(cfgKeyPostgresSearchPath); err != nil {
		return err
	}
	if c.Postgres.TxIsolationLevel, err = getIsolationLevel(dp, cfgKeyPostgresTxLevel); err != nil {
		return err
	}

	var dbParams map[string]string
	if dbParams, err = dp.GetStringMapString(cfgKeyPostgresAdditionalParams); err != nil {
		return err
	}
	if len(dbParams) != 0 {
		c.Postgres.AdditionalParameters = make([]Parameter, 0, len(dbParams))
		for name, val := range dbParams {
			c.Postgres.AdditionalParameters = append(c.Postgres.AdditionalParameters, Parameter{name, val})
		}
	}

	// Force to add Patroni readonly replica aware parameter (only for pgx driver).
	// Don't override already added parameter.
	if dialect == DialectPgx {
		if _, ok := dbParams[PgTargetSessionAttrs]; !ok {
			c.Postgres.AdditionalParameters = append(c.Postgres.AdditionalParameters, Parameter{
				PgTargetSessionAttrs, PgReadWriteParam})
		}
	}

	availableSSLModesStr := []string{
		string(PostgresSSLModeDisable),
		string(PostgresSSLModeRequire),
		string(PostgresSSLModeVerifyCA),
		string(PostgresSSLModeVerifyFull),
	}
	gotSSLModeStr, err := dp.GetStringFromSet(cfgKeyPostgresSSLMode, availableSSLModesStr, false)
	if err != nil {
		return err
	}
	c.Postgres.SSLMode = PostgresSSLMode(gotSSLModeStr)

	return nil
}

func (c *Config) setSQLiteConfig(dp config.DataProvider) error {
	var err error

	if c.SQLite.Path, err = dp.GetString(cfgKeySQLitePath); err != nil {
		return err
	}

	return nil
}

var availableTxIsolationLevels = []sql.IsolationLevel{
	sql.LevelReadUncommitted,
	sql.LevelReadCommitted,
	sql.LevelRepeatableRead,
	sql.LevelSerializable,
}

func getIsolationLevel(dp config.DataProvider, key string) (sql.IsolationLevel, error) {
	availableLevelsStr := make([]string, 0, len(availableTxIsolationLevels))
	for _, lvl := range availableTxIsolationLevels {
		availableLevelsStr = append(availableLevelsStr, lvl.String())
	}
	gotLevelStr, err := dp.GetStringFromSet(key, availableLevelsStr, false)
	if err != nil {
		return sql.LevelDefault, err
	}
	for i, lvlStr := range availableLevelsStr {
		if gotLevelStr == lvlStr {
			return availableTxIsolationLevels[i], nil
		}
	}
	return sql.LevelDefault, nil
}
