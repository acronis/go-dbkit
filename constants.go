/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package dbkit

import (
	"database/sql"
	"time"
)

// Default values of connection parameters
const (
	DefaultMaxIdleConns    = 2
	DefaultMaxOpenConns    = 10
	DefaultConnMaxLifetime = 10 * time.Minute // Official recommendation from the DBA team
)

// MSSQLDefaultTxLevel contains transaction isolation level which will be used by default for MSSQL.
const MSSQLDefaultTxLevel = sql.LevelReadCommitted

// MySQLDefaultTxLevel contains transaction isolation level which will be used by default for MySQL.
const MySQLDefaultTxLevel = sql.LevelReadCommitted

// PostgresDefaultTxLevel contains transaction isolation level which will be used by default for Postgres.
const PostgresDefaultTxLevel = sql.LevelReadCommitted

// PostgresDefaultSSLMode contains Postgres SSL mode which will be used by default.
const PostgresDefaultSSLMode = PostgresSSLModeVerifyCA

// PgTargetSessionAttrs session attrs parameter name
const PgTargetSessionAttrs = "target_session_attrs"

// PgReadWriteParam read-write session attribute value name
const PgReadWriteParam = "read-write"

// Dialect defines possible values for planned supported SQL dialects.
type Dialect string

// SQL dialects.
const (
	DialectSQLite   Dialect = "sqlite3"
	DialectMySQL    Dialect = "mysql"
	DialectPostgres Dialect = "postgres"
	DialectPgx      Dialect = "pgx"
	DialectMSSQL    Dialect = "mssql"
)

// PostgresErrCode defines the type for Postgres error codes.
type PostgresErrCode string

// Postgres error codes (will be filled gradually).
const (
	PgxErrCodeUniqueViolation      PostgresErrCode = "23505"
	PgxErrCodeDeadlockDetected     PostgresErrCode = "40P01"
	PgxErrCodeSerializationFailure PostgresErrCode = "40001"
	PgxErrFeatureNotSupported      PostgresErrCode = "0A000"

	// nolint: staticcheck // lib/pq using is deprecated. Use pgx Postgres driver.
	PostgresErrCodeUniqueViolation PostgresErrCode = "unique_violation"
	// nolint: staticcheck // lib/pq using is deprecated. Use pgx Postgres driver.
	PostgresErrCodeDeadlockDetected     PostgresErrCode = "deadlock_detected"
	PostgresErrCodeSerializationFailure PostgresErrCode = "serialization_failure"
)

// PostgresSSLMode defines possible values for Postgres sslmode connection parameter.
type PostgresSSLMode string

// Postgres SSL modes.
const (
	PostgresSSLModeDisable    PostgresSSLMode = "disable"
	PostgresSSLModeRequire    PostgresSSLMode = "require"
	PostgresSSLModeVerifyCA   PostgresSSLMode = "verify-ca"
	PostgresSSLModeVerifyFull PostgresSSLMode = "verify-full"
)
