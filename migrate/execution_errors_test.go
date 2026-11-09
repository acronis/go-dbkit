/*
Copyright © 2026 Acronis International GmbH.

Released under MIT license.
*/

package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	"github.com/acronis/go-dbkit"
)

var errSentinel = errors.New("sentinel failure")

func newMockStore(t *testing.T) (*sql.DB, sqlmock.Sqlmock, *metadataStore) {
	t.Helper()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	store, err := newMetadataStore(db, dbkit.DialectSQLite, MigrationsTableName)
	require.NoError(t, err)
	return db, mock, store
}

func step(id string, disableTx bool) plannedMigration {
	return plannedMigration{id: id, queries: []string{"EXEC " + id}, disableTx: disableTx}
}

// expectStep expects the statements and the history write of one migration.
func expectStep(mock sqlmock.Sqlmock, id string, direction MigrationsDirection) *sqlmock.ExpectedExec {
	mock.ExpectExec("EXEC " + id).WillReturnResult(sqlmock.NewResult(0, 1))
	if direction == MigrationsDirectionUp {
		return mock.ExpectExec(`INSERT INTO "migrations"`).WithArgs(id, sqlmock.AnyArg())
	}
	return mock.ExpectExec(`DELETE FROM "migrations"`).WithArgs(id)
}

// TestExecutePlan_transactionFailures checks BeginTx and Commit failures in both directions: the earlier migration is
// counted, the failing one is reported, and no later SQL runs.
func TestExecutePlan_transactionFailures(t *testing.T) {
	for _, direction := range []MigrationsDirection{MigrationsDirectionUp, MigrationsDirectionDown} {
		t.Run(fmt.Sprintf("%s/begin", direction), func(t *testing.T) {
			db, mock, store := newMockStore(t)
			mock.ExpectBegin()
			expectStep(mock, "0001", direction).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectCommit()
			mock.ExpectBegin().WillReturnError(errSentinel) // No statements, history write or commit follow.

			plan := []plannedMigration{step("0001", false), step("0002", false), step("0003", false)}
			n, err := executePlan(context.Background(), db, store, plan, direction)
			require.ErrorIs(t, err, errSentinel)
			require.ErrorContains(t, err, "handling 0002")
			require.Equal(t, 1, n)
			require.NoError(t, mock.ExpectationsWereMet())
		})

		t.Run(fmt.Sprintf("%s/commit", direction), func(t *testing.T) {
			db, mock, store := newMockStore(t)
			mock.ExpectBegin()
			expectStep(mock, "0001", direction).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectCommit()
			mock.ExpectBegin()
			expectStep(mock, "0002", direction).WillReturnResult(sqlmock.NewResult(0, 1))
			// Durability after a failed commit is up to the database; database/sql has finished the transaction.
			mock.ExpectCommit().WillReturnError(errSentinel)

			plan := []plannedMigration{step("0001", false), step("0002", false), step("0003", false)}
			n, err := executePlan(context.Background(), db, store, plan, direction)
			require.ErrorIs(t, err, errSentinel)
			require.ErrorContains(t, err, "handling 0002")
			require.Equal(t, 1, n)
			require.NoError(t, mock.ExpectationsWereMet())
		})

		t.Run(fmt.Sprintf("%s/no transaction calls with opt-out", direction), func(t *testing.T) {
			db, mock, store := newMockStore(t)
			expectStep(mock, "0001", direction).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectBegin()
			expectStep(mock, "0002", direction).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectCommit()
			expectStep(mock, "0003", direction).WillReturnError(errSentinel)

			plan := []plannedMigration{step("0001", true), step("0002", false), step("0003", true)}
			n, err := executePlan(context.Background(), db, store, plan, direction)
			require.ErrorIs(t, err, errSentinel)
			require.ErrorContains(t, err, "handling 0003")
			require.Equal(t, 2, n)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// TestMigrationsManager_metadataWriteErrors checks through the manager that a failed history INSERT or DELETE stops the
// run, is reachable with errors.Is, names the migration, and rolls back only when the transaction is enabled.
func TestMigrationsManager_metadataWriteErrors(t *testing.T) {
	migs := func(disableTx bool) []Migration {
		res := make([]Migration, 0, 3)
		for _, id := range []string{"0001", "0002", "0003"} {
			var m Migration = NewCustomMigration(id, []string{"EXEC " + id}, []string{"EXEC " + id}, nil, nil)
			if disableTx {
				m = &txDisabledMigration{m.(*CustomMigration)}
			}
			res = append(res, m)
		}
		return res
	}
	for _, disableTx := range []bool{false, true} {
		for _, direction := range []MigrationsDirection{MigrationsDirectionUp, MigrationsDirectionDown} {
			t.Run(fmt.Sprintf("%s/disableTx=%t", direction, disableTx), func(t *testing.T) {
				mm, mock := newMockManager(t)
				expectPreflight(mock)
				first := "0001" // Up applies 0001 first, down rolls back 0003 first.
				if direction == MigrationsDirectionUp {
					expectHistory(mock)
				} else {
					expectHistory(mock, "0001", "0002", "0003")
					first = "0003"
				}
				if !disableTx {
					mock.ExpectBegin()
				}
				expectStep(mock, first, direction).WillReturnResult(sqlmock.NewResult(0, 1))
				if !disableTx {
					mock.ExpectCommit()
					mock.ExpectBegin()
				}
				expectStep(mock, "0002", direction).WillReturnError(errSentinel)
				if !disableTx {
					mock.ExpectRollback()
				}

				err := mm.Run(migs(disableTx), direction)
				require.ErrorIs(t, err, errSentinel)
				require.ErrorContains(t, err, "handling 0002")
				require.NoError(t, mock.ExpectationsWereMet())
			})
		}
	}
}

// rawFallbackMigration returns (nil, nil) from RawMigration, so the default conversion is used.
type rawFallbackMigration struct {
	*CustomMigration
	disableTx bool
}

func (m *rawFallbackMigration) RawMigration(Migration) (*RawMigration, error) { return nil, nil }

func (m *rawFallbackMigration) DisableTx() bool { return m.disableTx }

// TestMigrationsManager_rawMigrationExecution checks the transaction calls for all four raw flag combinations in both
// directions and the (nil, nil) fallback. Raw and fallback SQL differ, so a wrong conversion path is visible.
func TestMigrationsManager_rawMigrationExecution(t *testing.T) {
	type tc struct {
		name          string
		mig           Migration
		wantSQL       map[MigrationsDirection]string
		wantTx        map[MigrationsDirection]bool
		wantConvError string
	}
	var cases []tc
	for _, flags := range [][2]bool{{false, false}, {true, false}, {false, true}, {true, true}} {
		cases = append(cases, tc{
			name: fmt.Sprintf("raw DisableTransactionUp=%t DisableTransactionDown=%t", flags[0], flags[1]),
			mig: &rawFlagsMigration{raw: RawMigration{ID: "0001", UpSQL: []string{"RAW UP"}, DownSQL: []string{"RAW DOWN"},
				DisableTransactionUp: flags[0], DisableTransactionDown: flags[1]}},
			wantSQL: map[MigrationsDirection]string{MigrationsDirectionUp: "RAW UP", MigrationsDirectionDown: "RAW DOWN"},
			wantTx:  map[MigrationsDirection]bool{MigrationsDirectionUp: !flags[0], MigrationsDirectionDown: !flags[1]},
		})
	}
	for _, disableTx := range []bool{false, true} {
		cases = append(cases, tc{
			name: fmt.Sprintf("nil fallback TxDisabler=%t", disableTx),
			mig: &rawFallbackMigration{NewCustomMigration("0001", []string{"FALLBACK UP"}, []string{"FALLBACK DOWN"}, nil, nil),
				disableTx},
			wantSQL: map[MigrationsDirection]string{MigrationsDirectionUp: "FALLBACK UP", MigrationsDirectionDown: "FALLBACK DOWN"},
			wantTx:  map[MigrationsDirection]bool{MigrationsDirectionUp: !disableTx, MigrationsDirectionDown: !disableTx},
		})
	}
	cases = append(cases, tc{
		name:          "nil fallback keeps validation",
		mig:           &rawFallbackMigration{CustomMigration: NewCustomMigration("0001", nil, []string{"FALLBACK DOWN"}, nil, nil)},
		wantConvError: "migration 0001 should implement UpSQL",
	})

	for _, c := range cases {
		for _, direction := range []MigrationsDirection{MigrationsDirectionUp, MigrationsDirectionDown} {
			t.Run(c.name+"/"+string(direction), func(t *testing.T) {
				mm, mock := newMockManager(t)
				if c.wantConvError != "" { // Conversion fails before any database call.
					require.EqualError(t, mm.Run([]Migration{c.mig}, direction), c.wantConvError)
					require.NoError(t, mock.ExpectationsWereMet())
					return
				}
				expectPreflight(mock)
				if direction == MigrationsDirectionUp {
					expectHistory(mock)
				} else {
					expectHistory(mock, "0001")
				}
				if c.wantTx[direction] {
					mock.ExpectBegin()
				}
				mock.ExpectExec(c.wantSQL[direction]).WillReturnResult(sqlmock.NewResult(0, 1))
				if direction == MigrationsDirectionUp {
					mock.ExpectExec(`INSERT INTO "migrations"`).WithArgs("0001", sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
				} else {
					mock.ExpectExec(`DELETE FROM "migrations"`).WithArgs("0001").WillReturnResult(sqlmock.NewResult(0, 1))
				}
				if c.wantTx[direction] {
					mock.ExpectCommit()
				}
				require.NoError(t, mm.Run([]Migration{c.mig}, direction))
				require.NoError(t, mock.ExpectationsWereMet())
			})
		}
	}
}

// TestMigrationsManager_rawMigrationRejectedBeforeSQL checks that the incompatible old RawMigration signature and a
// preparation error stop the run before any database call, although the migration has usable UpSQL.
func TestMigrationsManager_rawMigrationRejectedBeforeSQL(t *testing.T) {
	for name, tc := range map[string]struct {
		mig     Migration
		wantErr string
	}{
		"incompatible signature": {
			mig:     legacyRawMigration{NewCustomMigration("0001", []string{"EXEC 0001"}, []string{"EXEC 0001"}, nil, nil)},
			wantErr: "migration 0001 has RawMigration method with incompatible signature",
		},
		"preparation error": {
			mig:     &testMigration00003RawMigration{MakeError: true},
			wantErr: "preparing migration 00003_raw_migration failed with error: fake fatal error",
		},
	} {
		t.Run(name, func(t *testing.T) {
			mm, mock := newMockManager(t)
			for _, direction := range []MigrationsDirection{MigrationsDirectionUp, MigrationsDirectionDown} {
				require.ErrorContains(t, mm.Run([]Migration{tc.mig}, direction), tc.wantErr)
			}
			require.NoError(t, mock.ExpectationsWereMet()) // No metadata, migration or history SQL.
		})
	}
}
