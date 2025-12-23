/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package dbkit

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/acronis/go-appkit/config"
	"gopkg.in/yaml.v3"
)

const cfgDefaultKeyPrefix = "db"

const (
	cfgKeyDialect         = "dialect"
	cfgKeyMaxIdleConns    = "maxIdleConns"
	cfgKeyMaxOpenConns    = "maxOpenConns"
	cfgKeyConnMaxLifetime = "connMaxLifeTime"

	cfgKeyMySQLHost     = "mysql.host"
	cfgKeyMySQLPort     = "mysql.port"
	cfgKeyMySQLDatabase = "mysql.database"
	cfgKeyMySQLUser     = "mysql.user"
	cfgKeyMySQLPassword = "mysql.password" //nolint:gosec // Not a hardcoded password, just a config key
	cfgKeyMySQLTxLevel  = "mysql.txLevel"

	cfgKeySQLitePath = "sqlite3.path"

	cfgKeyPostgresHost             = "postgres.host"
	cfgKeyPostgresPort             = "postgres.port"
	cfgKeyPostgresDatabase         = "postgres.database"
	cfgKeyPostgresUser             = "postgres.user"
	cfgKeyPostgresPassword         = "postgres.password" //nolint:gosec // Not a hardcoded password, just a config key
	cfgKeyPostgresTxLevel          = "postgres.txLevel"
	cfgKeyPostgresSSLMode          = "postgres.sslMode"
	cfgKeyPostgresSearchPath       = "postgres.searchPath"
	cfgKeyPostgresAdditionalParams = "postgres.additionalParameters"
	cfgKeyMSSQLHost                = "mssql.host"
	cfgKeyMSSQLPort                = "mssql.port"
	cfgKeyMSSQLDatabase            = "mssql.database"
	cfgKeyMSSQLUser                = "mssql.user"
	cfgKeyMSSQLPassword            = "mssql.password" //nolint:gosec // Not a hardcoded password, just a config key
	cfgKeyMSSQLTxLevel             = "mssql.txLevel"
	cfgKeyMSSQLAdditionalParams    = "mssql.additionalParameters"
)

// Config represents a set of configuration parameters working with SQL databases.
type Config struct {
	Dialect         Dialect             `mapstructure:"dialect" yaml:"dialect" json:"dialect"`
	MaxOpenConns    int                 `mapstructure:"maxOpenConns" yaml:"maxOpenConns" json:"maxOpenConns"`
	MaxIdleConns    int                 `mapstructure:"maxIdleConns" yaml:"maxIdleConns" json:"maxIdleConns"`
	ConnMaxLifetime config.TimeDuration `mapstructure:"connMaxLifeTime" yaml:"connMaxLifeTime" json:"connMaxLifeTime"`
	MySQL           MySQLConfig         `mapstructure:"mysql" yaml:"mysql" json:"mysql"`
	MSSQL           MSSQLConfig         `mapstructure:"mssql" yaml:"mssql" json:"mssql"`
	SQLite          SQLiteConfig        `mapstructure:"sqlite3" yaml:"sqlite3" json:"sqlite3"`
	Postgres        PostgresConfig      `mapstructure:"postgres" yaml:"postgres" json:"postgres"`

	keyPrefix         string
	supportedDialects []Dialect
}

var _ config.Config = (*Config)(nil)
var _ config.KeyPrefixProvider = (*Config)(nil)

// ConfigOption is a type for functional options for the Config.
type ConfigOption func(*configOptions)

type configOptions struct {
	keyPrefix string
}

// WithKeyPrefix returns a ConfigOption that sets a key prefix for parsing configuration parameters.
// This prefix will be used by config.Loader.
func WithKeyPrefix(keyPrefix string) ConfigOption {
	return func(o *configOptions) {
		o.keyPrefix = keyPrefix
	}
}

// NewConfig creates a new instance of the Config.
func NewConfig(supportedDialects []Dialect, options ...ConfigOption) *Config {
	var opts = configOptions{keyPrefix: cfgDefaultKeyPrefix} // cfgDefaultKeyPrefix is used here for backward compatibility
	for _, opt := range options {
		opt(&opts)
	}
	return &Config{supportedDialects: supportedDialects, keyPrefix: opts.keyPrefix}
}

// NewConfigWithKeyPrefix creates a new instance of the Config with a key prefix.
// This prefix will be used by config.Loader.
// Deprecated: use NewConfig with WithKeyPrefix instead.
func NewConfigWithKeyPrefix(keyPrefix string, supportedDialects []Dialect) *Config {
	if keyPrefix != "" {
		keyPrefix += "."
	}
	keyPrefix += cfgDefaultKeyPrefix // cfgDefaultKeyPrefix is added here for backward compatibility
	return &Config{supportedDialects: supportedDialects, keyPrefix: keyPrefix}
}

// NewDefaultConfig creates a new instance of the Config with default values.
func NewDefaultConfig(supportedDialects []Dialect, options ...ConfigOption) *Config {
	opts := configOptions{keyPrefix: cfgDefaultKeyPrefix}
	for _, opt := range options {
		opt(&opts)
	}
	return &Config{
		keyPrefix:         opts.keyPrefix,
		supportedDialects: supportedDialects,
		MaxOpenConns:      DefaultMaxOpenConns,
		MaxIdleConns:      DefaultMaxIdleConns,
		ConnMaxLifetime:   config.TimeDuration(DefaultConnMaxLifetime),
		MySQL: MySQLConfig{
			TxIsolationLevel: IsolationLevel(MySQLDefaultTxLevel),
		},
		Postgres: PostgresConfig{
			TxIsolationLevel: IsolationLevel(PostgresDefaultTxLevel),
			SSLMode:          PostgresDefaultSSLMode,
		},
		MSSQL: MSSQLConfig{
			TxIsolationLevel: IsolationLevel(MSSQLDefaultTxLevel),
		},
	}
}

// KeyPrefix returns a key prefix with which all configuration parameters should be presented.
// Implements config.KeyPrefixProvider interface.
func (c *Config) KeyPrefix() string {
	if c.keyPrefix == "" {
		return cfgDefaultKeyPrefix
	}
	return c.keyPrefix
}

// SupportedDialects returns the list of supported dialects.
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

// MySQLConfig represents a set of configuration parameters for working with MySQL.
type MySQLConfig struct {
	Host             string         `mapstructure:"host" yaml:"host" json:"host"`
	Port             int            `mapstructure:"port" yaml:"port" json:"port"`
	User             string         `mapstructure:"user" yaml:"user" json:"user"`
	Password         string         `mapstructure:"password" yaml:"password" json:"password"`
	Database         string         `mapstructure:"database" yaml:"database" json:"database"`
	TxIsolationLevel IsolationLevel `mapstructure:"txLevel" yaml:"txLevel" json:"txLevel"`
}

// MSSQLConfig represents a set of configuration parameters for working with MSSQL.
type MSSQLConfig struct {
	Host                 string            `mapstructure:"host" yaml:"host" json:"host"`
	Port                 int               `mapstructure:"port" yaml:"port" json:"port"`
	User                 string            `mapstructure:"user" yaml:"user" json:"user"`
	Password             string            `mapstructure:"password" yaml:"password" json:"password"`
	Database             string            `mapstructure:"database" yaml:"database" json:"database"`
	TxIsolationLevel     IsolationLevel    `mapstructure:"txLevel" yaml:"txLevel" json:"txLevel"`
	AdditionalParameters map[string]string `mapstructure:"additionalParameters" yaml:"additionalParameters" json:"additionalParameters"`
}

// SQLiteConfig represents a set of configuration parameters for working with SQLite.
type SQLiteConfig struct {
	Path string `mapstructure:"path" yaml:"path" json:"path"`
}

// PostgresConfig represents a set of configuration parameters for working with Postgres.
type PostgresConfig struct {
	Host                 string            `mapstructure:"host" yaml:"host" json:"host"`
	Port                 int               `mapstructure:"port" yaml:"port" json:"port"`
	User                 string            `mapstructure:"user" yaml:"user" json:"user"`
	Password             string            `mapstructure:"password" yaml:"password" json:"password"`
	Database             string            `mapstructure:"database" yaml:"database" json:"database"`
	TxIsolationLevel     IsolationLevel    `mapstructure:"txLevel" yaml:"txLevel" json:"txLevel"`
	SSLMode              PostgresSSLMode   `mapstructure:"sslMode" yaml:"sslMode" json:"sslMode"`
	SearchPath           string            `mapstructure:"searchPath" yaml:"searchPath" json:"searchPath"`
	AdditionalParameters map[string]string `mapstructure:"additionalParameters" yaml:"additionalParameters" json:"additionalParameters"`
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

	var connMaxLifeTime time.Duration
	if connMaxLifeTime, err = dp.GetDuration(cfgKeyConnMaxLifetime); err != nil {
		return err
	}
	c.ConnMaxLifetime = config.TimeDuration(connMaxLifeTime)

	return nil
}

// TxIsolationLevel returns transaction isolation level from parsed config for specified dialect.
func (c *Config) TxIsolationLevel() sql.IsolationLevel {
	switch c.Dialect {
	case DialectMySQL:
		return sql.IsolationLevel(c.MySQL.TxIsolationLevel)
	case DialectPostgres, DialectPgx:
		return sql.IsolationLevel(c.Postgres.TxIsolationLevel)
	case DialectMSSQL:
		return sql.IsolationLevel(c.MSSQL.TxIsolationLevel)
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

//nolint:dupl // Similar config setters for different database dialects
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

//nolint:dupl // Similar config setters for different database dialects
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
	var additionalParams map[string]string
	if additionalParams, err = dp.GetStringMapString(cfgKeyMSSQLAdditionalParams); err != nil {
		return err
	}
	if len(additionalParams) != 0 {
		c.MSSQL.AdditionalParameters = additionalParams
	}

	return nil
}

//nolint:dupl // Similar config setters for different database dialects
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

	var additionalParams map[string]string
	if additionalParams, err = dp.GetStringMapString(cfgKeyPostgresAdditionalParams); err != nil {
		return err
	}
	if len(additionalParams) != 0 {
		c.Postgres.AdditionalParameters = additionalParams
	}
	// Force to add Patroni readonly replica-aware parameter (only for pgx driver).
	// Don't override already added parameter.
	if dialect == DialectPgx {
		if _, ok := c.Postgres.AdditionalParameters[PgTargetSessionAttrs]; !ok {
			if c.Postgres.AdditionalParameters == nil {
				c.Postgres.AdditionalParameters = make(map[string]string)
			}
			c.Postgres.AdditionalParameters[PgTargetSessionAttrs] = PgReadWriteParam
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

func getIsolationLevel(dp config.DataProvider, key string) (IsolationLevel, error) {
	s, err := dp.GetString(key)
	if err != nil {
		return IsolationLevel(sql.LevelDefault), err
	}
	return getTxIsolationLevelFromString(s)
}

type IsolationLevel sql.IsolationLevel

// UnmarshalJSON allows decoding string representation of isolation level from JSON.
// Implements json.Unmarshaler interface.
func (il *IsolationLevel) UnmarshalJSON(data []byte) error {
	level, err := getTxIsolationLevelFromString(strings.Trim(string(data), `"`))
	if err != nil {
		return err
	}
	*il = level
	return nil
}

// UnmarshalYAML allows decoding from YAML.
// Implements yaml.Unmarshaler interface.
func (il *IsolationLevel) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return fmt.Errorf("invalid isolation level: %w", err)
	}
	level, err := getTxIsolationLevelFromString(s)
	if err != nil {
		return err
	}
	*il = level
	return nil
}

// UnmarshalText allows decoding from text.
// Implements encoding.TextUnmarshaler interface, which is used by mapstructure.TextUnmarshallerHookFunc.
func (il *IsolationLevel) UnmarshalText(text []byte) error {
	return il.UnmarshalJSON(text)
}

// String returns the human-readable string representation.
// Implements fmt.Stringer interface.
func (il IsolationLevel) String() string {
	return sql.IsolationLevel(il).String()
}

// MarshalJSON encodes as a human-readable string in JSON.
// Implements json.Marshaler interface.
func (il IsolationLevel) MarshalJSON() ([]byte, error) {
	return json.Marshal(il.String())
}

// MarshalYAML encodes as a human-readable string in YAML.
// Implements yaml.Marshaler interface.
func (il IsolationLevel) MarshalYAML() (interface{}, error) {
	return il.String(), nil
}

// MarshalText encodes as a human-readable string in text.
// Implements encoding.TextMarshaler interface.
func (il *IsolationLevel) MarshalText() ([]byte, error) {
	return []byte(il.String()), nil
}

var availableTxIsolationLevelsMap = prepareAvailableTxIsolationLevelsStr()

func prepareAvailableTxIsolationLevelsStr() map[string]IsolationLevel {
	availableLevels := []sql.IsolationLevel{
		sql.LevelReadUncommitted,
		sql.LevelReadCommitted,
		sql.LevelRepeatableRead,
		sql.LevelSerializable,
	}
	m := make(map[string]IsolationLevel, len(availableLevels))
	for _, level := range availableLevels {
		m[level.String()] = IsolationLevel(level)
	}
	return m
}

func getTxIsolationLevelFromString(s string) (IsolationLevel, error) {
	level, ok := availableTxIsolationLevelsMap[s]
	if !ok {
		return IsolationLevel(sql.LevelDefault), fmt.Errorf("invalid isolation level: %s", s)
	}
	return level, nil
}
