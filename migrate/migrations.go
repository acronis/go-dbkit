/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

// Package migrate provides functionality for applying database migrations.
package migrate

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/acronis/go-appkit/log"
	migrate "github.com/rubenv/sql-migrate"

	"github.com/acronis/go-dbkit"
)

// MigrationsTableName contains name of table in a database that stores applied migrations.
const MigrationsTableName = "migrations"

// MigrationsDirection defines possible values for direction of database migrations.
type MigrationsDirection string

// Directions of database migrations.
const (
	MigrationsDirectionUp   MigrationsDirection = "up"
	MigrationsDirectionDown MigrationsDirection = "down"
)

// MigrationsNoLimit contains a special value that will not limit the number of migrations to apply.
const MigrationsNoLimit = 0

// Migration is an interface for all database migrations.
// Migration may implement RawMigrator interface for full control.
// Migration may implement TxDisabler interface to control transactions.
type Migration interface {
	ID() string
	UpSQL() []string
	DownSQL() []string
	UpFn() func(tx *sql.Tx) error   // Not supported yet.
	DownFn() func(tx *sql.Tx) error // Not supported yet.
}

// RawMigrator is an interface which allows overwrite default generate mechanism for full control on migrations.
// Uses sql-migrate migration structure.
type RawMigrator interface {
	RawMigration(m Migration) (*migrate.Migration, error)
}

// TxDisabler is an interface for Migration for controlling transaction
type TxDisabler interface {
	DisableTx() bool
}

// NullMigration represents an empty basic migration that may be embedded in regular migrations
// in order to write less code for satisfying the Migration interface.
type NullMigration struct {
	Dialect db.Dialect
}

// ID is a stub that returns empty migration identifier.
func (m *NullMigration) ID() string {
	return ""
}

// UpSQL is a stub that returns an empty slice of SQL statements that implied to be executed during applying the migration.
func (m *NullMigration) UpSQL() []string {
	return nil
}

// DownSQL is a stub that returns an empty slice of SQL statements that implied to be executed during rolling back the migration.
func (m *NullMigration) DownSQL() []string {
	return nil
}

// UpFn is a stub that returns an empty function that implied to be called during applying the migration.
func (m *NullMigration) UpFn() func(tx *sql.Tx) error {
	return nil
}

// DownFn is a stub that returns an empty function that implied to be called during rolling back the migration.
func (m *NullMigration) DownFn() func(tx *sql.Tx) error {
	return nil
}

// CustomMigration represents simplified but customizable migration
type CustomMigration struct {
	id      string
	upSQL   []string
	downSQL []string
	upFn    func(tx *sql.Tx) error
	downFn  func(tx *sql.Tx) error
}

// NewCustomMigration creates simplified but customizable migration.
func NewCustomMigration(id string, upSQL, downSQL []string, upFn, downFn func(tx *sql.Tx) error) *CustomMigration {
	return &CustomMigration{id: id, upSQL: upSQL, downSQL: downSQL, upFn: upFn, downFn: downFn}
}

// ID returns migration identifier.
func (m *CustomMigration) ID() string {
	return m.id
}

// UpSQL returns a slice of SQL statements that will be executed during applying the migration.
func (m *CustomMigration) UpSQL() []string {
	return m.upSQL
}

// DownSQL returns a slice of SQL statements that will be executed during rolling back the migration.
func (m *CustomMigration) DownSQL() []string {
	return m.downSQL
}

// UpFn returns a function that will be called during applying the migration
func (m *CustomMigration) UpFn() func(tx *sql.Tx) error {
	return m.upFn
}

// DownFn returns a function that will be called during rolling back the migration
func (m *CustomMigration) DownFn() func(tx *sql.Tx) error {
	return m.downFn
}

// MigrationsManager is an object for running migrations.
type MigrationsManager struct {
	db      *sql.DB
	Dialect db.Dialect
	migSet  migrate.MigrationSet
	logger  log.FieldLogger
}

// MigrationsManagerOpts holds the Migration Manager options to be used in NewMigrationsManagerWithOpts
type MigrationsManagerOpts struct {
	TableName string
}

// NewMigrationsManager creates a new MigrationsManager.
func NewMigrationsManager(dbConn *sql.DB, dialect db.Dialect, logger log.FieldLogger) (*MigrationsManager, error) {
	migSet := migrate.MigrationSet{TableName: MigrationsTableName}
	return &MigrationsManager{dbConn, normalizeDialect(dialect), migSet, logger}, nil
}

// NewMigrationsManagerWithOpts creates a new MigrationsManager with custom options
func NewMigrationsManagerWithOpts(
	dbConn *sql.DB,
	dialect db.Dialect,
	logger log.FieldLogger,
	opts MigrationsManagerOpts,
) (*MigrationsManager, error) {
	tableName := opts.TableName
	if tableName == "" {
		tableName = MigrationsTableName
	}
	migSet := migrate.MigrationSet{TableName: tableName}
	return &MigrationsManager{dbConn, normalizeDialect(dialect), migSet, logger}, nil
}

// TODO: normalizeDialect sets standard lib/pq driver for pgx dialect because pgx isn't supported by sql-migrate yet.
func normalizeDialect(dialect db.Dialect) db.Dialect {
	if dialect == db.DialectPgx {
		return db.DialectPostgres
	}
	return dialect
}

// Run runs all passed migrations.
func (mm *MigrationsManager) Run(migrations []Migration, direction MigrationsDirection) error {
	return mm.RunLimit(migrations, direction, MigrationsNoLimit)
}

// convertMigration converts migration to internal sql-migrate format.
// If migration implements RawMigrator interface then RawMigration function is used.
// If migration implements TxDisabler interface then it may be not in transaction.
func convertMigration(m Migration) (*migrate.Migration, error) {
	if migrator, ok := m.(RawMigrator); ok {
		raw, err := migrator.RawMigration(m)
		if err != nil {
			return nil, fmt.Errorf("preparing migration %s failed with error: %w", m.ID(), err)
		}
		if raw != nil {
			return raw, nil
		}
	}

	if len(m.UpSQL()) == 0 { // Check will be removed when UpFn() will be supported.
		return nil, fmt.Errorf("migration %s should implement UpSQL", m.ID())
	}
	if (m.UpFn() == nil && len(m.UpSQL()) == 0) || (m.UpFn() != nil && len(m.UpSQL()) != 0) {
		// Will be actual when UpFn() will be supported.
		return nil, fmt.Errorf("migration %s should implement either UpFn or UpSQL", m.ID())
	}
	if m.DownFn() != nil && len(m.DownSQL()) != 0 {
		// Will be actual when DownFn() will be supported.
		return nil, fmt.Errorf("migration %s should implement either DownFn or DownSQL", m.ID())
	}
	disableTx := false
	if disableTransactor, ok := m.(TxDisabler); ok {
		disableTx = disableTransactor.DisableTx()
	}
	return &migrate.Migration{
		Id:                     m.ID(),
		Up:                     m.UpSQL(),
		Down:                   m.DownSQL(),
		DisableTransactionUp:   disableTx,
		DisableTransactionDown: disableTx,
	}, nil
}

// RunLimit runs at most `limit` migrations. Pass 0 (or MigrationsNoLimit const) for no limit (or use Run).
func (mm *MigrationsManager) RunLimit(migrations []Migration, direction MigrationsDirection, limit int) error {
	convertedMigrationList := make([]*migrate.Migration, 0, len(migrations))
	for i, m := range migrations {
		if m.ID() == "" {
			return fmt.Errorf("migration #%d has empty ID", i+1)
		}

		convertedMigration, err := convertMigration(m)
		if err != nil {
			return err
		}
		convertedMigrationList = append(convertedMigrationList, convertedMigration)
	}

	source := &migrate.MemoryMigrationSource{Migrations: convertedMigrationList}

	var dir migrate.MigrationDirection
	switch direction {
	case MigrationsDirectionUp:
		dir = migrate.Up
	case MigrationsDirectionDown:
		dir = migrate.Down
	default:
		return fmt.Errorf("unknown direction %q", dir)
	}

	n, err := mm.migSet.ExecMax(mm.db, string(mm.Dialect), source, dir, limit)

	logger := mm.logger.With(log.String("direction", string(direction)), log.Int("applied", n))
	if err != nil {
		logger.Error("db migration failed", log.Error(err))
		return err
	}
	logger.Info("db migration up succeeded")
	return nil
}

// Status returns the current migration status.
func (mm *MigrationsManager) Status() (MigrationStatus, error) {
	var migStatus MigrationStatus

	appliedMigRecords, err := mm.migSet.GetMigrationRecords(mm.db, string(mm.Dialect))
	if err != nil {
		return migStatus, fmt.Errorf("get applied migrations: %w", err)
	}
	migStatus.AppliedMigrations = make([]AppliedMigration, 0, len(appliedMigRecords))
	for _, migRec := range appliedMigRecords {
		migStatus.AppliedMigrations = append(migStatus.AppliedMigrations, AppliedMigration{ID: migRec.Id, AppliedAt: migRec.AppliedAt})
	}

	return migStatus, nil
}

// AppliedMigration represent a single already applied migration.
type AppliedMigration struct {
	ID        string
	AppliedAt time.Time
}

// MigrationStatus is the migration status.
type MigrationStatus struct {
	AppliedMigrations []AppliedMigration
}

// LastAppliedMigration returns last applied migration if it exists.
func (ms *MigrationStatus) LastAppliedMigration() (appliedMig AppliedMigration, exist bool) {
	if len(ms.AppliedMigrations) == 0 {
		return AppliedMigration{}, false
	}
	return ms.AppliedMigrations[len(ms.AppliedMigrations)-1], true
}
