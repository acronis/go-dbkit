/*
Copyright © 2026 Acronis International GmbH.

Released under MIT license.
*/

package migrate_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	gotesting "testing"
	"time"

	"github.com/acronis/go-appkit/log/logtest"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	_ "github.com/microsoft/go-mssqldb"
	"github.com/stretchr/testify/require"

	"github.com/acronis/go-dbkit"
	"github.com/acronis/go-dbkit/internal/testing"
	"github.com/acronis/go-dbkit/migrate"
)

// TestMigrationsManagerIntegration runs the same migration scenarios against every supported backend.
// Server backends run in Testcontainers and are required: a startup failure fails the test.
//
//	Full suite:  go test -race -count=1 -timeout=15m ./migrate -run '^TestMigrationsManagerIntegration$'
//	SQLite only: go test -race -count=1 ./migrate -run '^TestMigrationsManagerIntegration/SQLite$'
func TestMigrationsManagerIntegration(t *gotesting.T) {
	for _, b := range integrationBackends {
		t.Run(b.name, func(t *gotesting.T) {
			openDB := b.start(t)
			for _, sc := range integrationScenarios() {
				t.Run(sc.name, func(t *gotesting.T) {
					for _, mt := range []struct{ name, table string }{{"DefaultTable", ""}, {"CustomTable", "custom"}} {
						t.Run(mt.name, func(t *gotesting.T) {
							sc.run(t, newIntegrationEnv(t, b, openDB(t), mt.table))
						})
					}
				})
			}
		})
	}
}

const (
	containerStartTimeout = 7 * time.Minute // Startup and readiness timeouts of the helper apply within it.
	cleanupTimeout        = time.Minute
	// timestampTolerance covers DATETIME precision: MySQL stores seconds, MSSQL rounds to 1/300 s.
	timestampTolerance = 2 * time.Second
)

// integrationBackend describes dialect-specific parts of the suite. Scenarios and assertions are shared.
type integrationBackend struct {
	name    string
	dialect dbkit.Dialect
	// transactionalDDL is false when DDL commits the current transaction implicitly (MySQL, MariaDB).
	transactionalDDL bool
	placeholder      func(n int) string
	// start prepares the backend and returns a function opening a database for a single scenario.
	start func(t *gotesting.T) func(t *gotesting.T) *sql.DB
}

var integrationBackends = []integrationBackend{
	{
		name: "SQLite", dialect: dbkit.DialectSQLite, transactionalDDL: true,
		placeholder: func(int) string { return "?" }, start: startSQLite,
	},
	{
		name: "MySQL", dialect: dbkit.DialectMySQL, transactionalDDL: false,
		placeholder: func(int) string { return "?" }, start: startContainer(string(dbkit.DialectMySQL)),
	},
	{
		name: "MariaDB", dialect: dbkit.DialectMySQL, transactionalDDL: false,
		placeholder: func(int) string { return "?" }, start: startContainer(testing.DialectMariaDB),
	},
	{
		name: "Postgres", dialect: dbkit.DialectPostgres, transactionalDDL: true,
		placeholder: func(n int) string { return "$" + strconv.Itoa(n) }, start: startContainer(string(dbkit.DialectPostgres)),
	},
	{
		name: "Pgx", dialect: dbkit.DialectPgx, transactionalDDL: true,
		placeholder: func(n int) string { return "$" + strconv.Itoa(n) }, start: startContainer(string(dbkit.DialectPgx)),
	},
	{
		name: "MSSQL", dialect: dbkit.DialectMSSQL, transactionalDDL: true,
		placeholder: func(n int) string { return "@p" + strconv.Itoa(n) }, start: startContainer(string(dbkit.DialectMSSQL)),
	},
}

// startSQLite gives every scenario its own database file. Foreign keys are enabled for the metadata write failure scenario.
func startSQLite(*gotesting.T) func(t *gotesting.T) *sql.DB {
	return func(t *gotesting.T) *sql.DB {
		db, err := sql.Open("sqlite3", "file:"+filepath.Join(t.TempDir(), "test.db")+"?_foreign_keys=on")
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		return db
	}
}

// startContainer starts one container per backend group. Scenarios share it, isolate themselves by table names and
// get their own connection pool, so driver statement caches do not outlive tables recreated by other scenarios.
func startContainer(kind string) func(t *gotesting.T) func(t *gotesting.T) *sql.DB {
	return func(t *gotesting.T) func(t *gotesting.T) *sql.DB {
		ctx, cancel := context.WithTimeout(context.Background(), containerStartTimeout)
		defer cancel()
		tdb, err := testing.RunTestDB(ctx, kind)
		require.NoError(t, err, "start required %s test database", kind)
		t.Cleanup(func() {
			stopCtx, stopCancel := context.WithTimeout(context.Background(), cleanupTimeout)
			defer stopCancel()
			require.NoError(t, tdb.Stop(stopCtx))
		})
		return func(t *gotesting.T) *sql.DB {
			db, err := sql.Open(tdb.DriverName, tdb.DSN)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			require.NoError(t, db.Ping())
			return db
		}
	}
}

type integrationScenario struct {
	name string
	run  func(t *gotesting.T, e *integrationEnv)
}

func integrationScenarios() []integrationScenario {
	return append([]integrationScenario{
		{"Lifecycle", testLifecycle},
		{"FailedUpDML", testFailedUpDML},
		{"FailedDownDML", testFailedDownDML},
		{"FailedDDL", testFailedDDL},
		{"TxDisabler", testTxDisabler},
		{"RawTxFlags", testRawTxFlags},
		{"MetadataWriteFailure", testMetadataWriteFailure},
		{"MetadataWriteFailureNoTx", testMetadataWriteFailureNoTx},
	}, legacyHistoryScenarios()...)
}

var tableSeq atomic.Int64

// fixtureTables are the tables a scenario may create, in drop order: tables referencing the metadata table go first.
var fixtureTables = []string{"ref", "users", "items", "created", "fixture"}

// integrationEnv is the state of one scenario run with one metadata table variant.
type integrationEnv struct {
	t         *gotesting.T
	b         integrationBackend
	db        *sql.DB
	mm        *migrate.MigrationsManager
	metaTable string
	replacer  *strings.Replacer
}

// newIntegrationEnv creates the environment. Tables get a unique prefix; "{name}" in SQL refers to them and "{meta}" to
// the metadata table. Dropping all of them is registered before the scenario runs and does not depend on Down migrations.
func newIntegrationEnv(t *gotesting.T, b integrationBackend, db *sql.DB, customMetaTable string) *integrationEnv {
	prefix := fmt.Sprintf("it%d_", tableSeq.Add(1))
	e := &integrationEnv{t: t, b: b, db: db, metaTable: migrate.MigrationsTableName}
	if customMetaTable != "" {
		e.metaTable = prefix + customMetaTable
	}
	pairs := []string{"{meta}", e.metaTable}
	for _, name := range fixtureTables {
		pairs = append(pairs, "{"+name+"}", prefix+name)
	}
	e.replacer = strings.NewReplacer(pairs...)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		for _, name := range append(append([]string{}, fixtureTables...), "meta") {
			table := e.q("{" + name + "}")
			if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
				t.Errorf("drop table %s: %v", table, err)
			}
		}
	})

	opts := migrate.MigrationsManagerOpts{}
	if customMetaTable != "" {
		opts.TableName = e.metaTable
	}
	var err error
	e.mm, err = migrate.NewMigrationsManagerWithOpts(db, b.dialect, logtest.NewLogger(), opts)
	require.NoError(t, err)
	return e
}

// q expands table placeholders in a query.
func (e *integrationEnv) q(query string) string {
	return e.replacer.Replace(query)
}

func (e *integrationEnv) qs(queries []string) []string {
	res := make([]string, len(queries))
	for i, query := range queries {
		res[i] = e.q(query)
	}
	return res
}

// exec runs fixture SQL directly, outside of migrations.
func (e *integrationEnv) exec(query string, args ...any) {
	e.t.Helper()
	_, err := e.db.Exec(e.q(query), args...)
	require.NoError(e.t, err, query)
}

func (e *integrationEnv) mig(id string, up, down []string) *migrate.CustomMigration {
	return migrate.NewCustomMigration(id, e.qs(up), e.qs(down), nil, nil)
}

// noTxMigration disables the transaction through TxDisabler.
type noTxMigration struct {
	*migrate.CustomMigration
}

func (noTxMigration) DisableTx() bool { return true }

func (e *integrationEnv) noTxMig(id string, up, down []string) migrate.Migration {
	return noTxMigration{e.mig(id, up, down)}
}

// rawMigration provides a RawMigration with per-direction transaction flags through RawMigrator.
type rawMigration struct {
	migrate.NullMigration
	raw migrate.RawMigration
}

func (m *rawMigration) ID() string { return m.raw.ID }

func (m *rawMigration) RawMigration(migrate.Migration) (*migrate.RawMigration, error) {
	raw := m.raw
	return &raw, nil
}

func (e *integrationEnv) rawMig(id string, up, down []string, disableTxUp, disableTxDown bool) migrate.Migration {
	return &rawMigration{raw: migrate.RawMigration{
		ID: id, UpSQL: e.qs(up), DownSQL: e.qs(down), DisableTransactionUp: disableTxUp, DisableTransactionDown: disableTxDown,
	}}
}

func (e *integrationEnv) up(migs ...migrate.Migration) error {
	return e.mm.Run(migs, migrate.MigrationsDirectionUp)
}

func (e *integrationEnv) down(migs ...migrate.Migration) error {
	return e.mm.Run(migs, migrate.MigrationsDirectionDown)
}

// tableExists reports whether a query against the table succeeds.
func (e *integrationEnv) tableExists(table string) bool {
	e.t.Helper()
	rows, err := e.db.Query("SELECT 1 FROM " + e.q(table) + " WHERE 1 = 0")
	if err != nil {
		return false
	}
	require.NoError(e.t, rows.Close())
	return true
}

// users returns the content of the users fixture table.
func (e *integrationEnv) users() map[int]string {
	e.t.Helper()
	rows, err := e.db.Query(e.q("SELECT id, name FROM {users}"))
	require.NoError(e.t, err)
	defer func() { require.NoError(e.t, rows.Close()) }()
	res := map[int]string{}
	for rows.Next() {
		var id int
		var name string
		require.NoError(e.t, rows.Scan(&id, &name))
		res[id] = name
	}
	require.NoError(e.t, rows.Err())
	return res
}

func (e *integrationEnv) requireUsers(want map[int]string) {
	e.t.Helper()
	require.Equal(e.t, want, e.users())
}

func (e *integrationEnv) status() []migrate.AppliedMigration {
	e.t.Helper()
	status, err := e.mm.Status()
	require.NoError(e.t, err)
	return status.AppliedMigrations
}

// requireHistory checks the exact set of applied migration IDs.
func (e *integrationEnv) requireHistory(want ...string) {
	e.t.Helper()
	got := make([]string, 0, len(want))
	for _, m := range e.status() {
		got = append(got, m.ID)
	}
	sort.Strings(got)
	want = append([]string{}, want...)
	sort.Strings(want)
	require.Equal(e.t, want, got)
}

func requireTimeNear(t *gotesting.T, want, got time.Time) {
	t.Helper()
	require.WithinDuration(t, want, got, timestampTolerance)
}

// requireSameStatus compares statuses ignoring time zone representation.
func requireSameStatus(t *gotesting.T, want, got []migrate.AppliedMigration) {
	t.Helper()
	require.Len(t, got, len(want))
	for i := range want {
		require.Equal(t, want[i].ID, got[i].ID)
		require.True(t, want[i].AppliedAt.Equal(got[i].AppliedAt), "applied_at of %s: %v != %v", want[i].ID, want[i].AppliedAt, got[i].AppliedAt)
	}
}

const createUsersSQL = "CREATE TABLE {users} (id INT NOT NULL PRIMARY KEY, name VARCHAR(100) NOT NULL)"

func insertUser(id int, name string) string {
	return fmt.Sprintf("INSERT INTO {users} (id, name) VALUES (%d, '%s')", id, name)
}

// lifecycleMigrations are listed unsorted. Running 10rename after 2seed proves the numeric order:
// the lexicographic order would rename bob before he is inserted.
func lifecycleMigrations(e *integrationEnv) []migrate.Migration {
	return []migrate.Migration{
		e.mig("10rename",
			[]string{"UPDATE {users} SET name = 'carol' WHERE id = 2"},
			[]string{"UPDATE {users} SET name = 'bob' WHERE id = 2"}),
		e.mig("1create", []string{createUsersSQL}, []string{"DROP TABLE {users}"}),
		e.mig("2seed", []string{insertUser(1, "alice"), insertUser(2, "bob") + ";\n"}, []string{"DELETE FROM {users}"}),
	}
}

func testLifecycle(t *gotesting.T, e *integrationEnv) {
	migs := lifecycleMigrations(e)

	require.False(t, e.tableExists("{meta}"))
	require.Empty(t, e.status())
	require.Empty(t, e.status()) // Metadata table creation is idempotent.
	require.True(t, e.tableExists("{meta}"))

	before := time.Now()
	require.NoError(t, e.mm.RunLimit(migs, migrate.MigrationsDirectionUp, 1))
	e.requireHistory("1create")
	e.requireUsers(map[int]string{})

	require.NoError(t, e.mm.RunLimit(migs, migrate.MigrationsDirectionUp, 1))
	e.requireHistory("1create", "2seed")
	e.requireUsers(map[int]string{1: "alice", 2: "bob"})

	require.NoError(t, e.up(migs...))
	after := time.Now()
	e.requireHistory("1create", "2seed", "10rename")
	e.requireUsers(map[int]string{1: "alice", 2: "carol"})

	status, err := e.mm.Status()
	require.NoError(t, err)
	for _, m := range status.AppliedMigrations {
		require.False(t, m.AppliedAt.Before(before.Add(-timestampTolerance)), "%s applied at %v, before %v", m.ID, m.AppliedAt, before)
		require.False(t, m.AppliedAt.After(after.Add(timestampTolerance)), "%s applied at %v, after %v", m.ID, m.AppliedAt, after)
	}
	// Status is ordered by ID as text, as in previous versions.
	last, ok := status.LastAppliedMigration()
	require.True(t, ok)
	require.Equal(t, "2seed", last.ID)

	require.NoError(t, e.up(migs...)) // No-op.
	requireSameStatus(t, status.AppliedMigrations, e.status())
	e.requireUsers(map[int]string{1: "alice", 2: "carol"})

	require.NoError(t, e.mm.RunLimit(migs, migrate.MigrationsDirectionDown, 1))
	e.requireHistory("1create", "2seed")
	e.requireUsers(map[int]string{1: "alice", 2: "bob"})

	require.NoError(t, e.down(migs...))
	e.requireHistory()
	require.False(t, e.tableExists("{users}"))

	require.NoError(t, e.down(migs...)) // No-op.
	e.requireHistory()
}

// dmlMigrations returns migrations where 3bob may fail on Up and 4carol may fail on Down.
// Failures are unique key violations in a separate statement after successful DML.
func dmlMigrations(e *integrationEnv, brokenUp, brokenDown bool) []migrate.Migration {
	up3 := []string{insertUser(2, "bob")}
	if brokenUp {
		up3 = append(up3, insertUser(1, "dup"))
	}
	down4 := []string{"UPDATE {users} SET name = 'changed' WHERE id = 1", "DELETE FROM {users} WHERE id = 3"}
	if brokenDown {
		down4 = append(down4, insertUser(2, "dup"))
	}
	return []migrate.Migration{
		e.mig("1create", []string{createUsersSQL}, []string{"DROP TABLE {users}"}),
		e.mig("2alice", []string{insertUser(1, "alice")}, []string{"DELETE FROM {users} WHERE id = 1"}),
		e.mig("3bob", up3, []string{"DELETE FROM {users} WHERE id = 2"}),
		e.mig("4carol", []string{insertUser(3, "carol")}, down4),
	}
}

func testFailedUpDML(t *gotesting.T, e *integrationEnv) {
	err := e.up(dmlMigrations(e, true, false)...)
	require.ErrorContains(t, err, "3bob")
	e.requireHistory("1create", "2alice")
	e.requireUsers(map[int]string{1: "alice"}) // 3bob rolled back, 4carol not run.

	require.NoError(t, e.up(dmlMigrations(e, false, false)...))
	e.requireHistory("1create", "2alice", "3bob", "4carol")
	e.requireUsers(map[int]string{1: "alice", 2: "bob", 3: "carol"})
}

func testFailedDownDML(t *gotesting.T, e *integrationEnv) {
	require.NoError(t, e.up(dmlMigrations(e, false, false)...))

	err := e.down(dmlMigrations(e, false, true)...)
	require.ErrorContains(t, err, "4carol")
	e.requireHistory("1create", "2alice", "3bob", "4carol")
	e.requireUsers(map[int]string{1: "alice", 2: "bob", 3: "carol"}) // 4carol rolled back, 3bob not run.

	require.NoError(t, e.down(dmlMigrations(e, false, false)...))
	e.requireHistory()
	require.False(t, e.tableExists("{users}"))
}

// testFailedDDL checks CREATE/DROP TABLE followed by a failure. MySQL commits DDL implicitly, so the schema change remains.
func testFailedDDL(t *gotesting.T, e *integrationEnv) {
	e.exec("CREATE TABLE {fixture} (id INT NOT NULL PRIMARY KEY)")
	e.exec("INSERT INTO {fixture} (id) VALUES (1)")
	const createSQL, dropSQL, failSQL = "CREATE TABLE {created} (id INT NOT NULL PRIMARY KEY)",
		"DROP TABLE {created}", "INSERT INTO {fixture} (id) VALUES (1)"

	err := e.up(e.mig("1ddl", []string{createSQL, failSQL}, []string{dropSQL}))
	require.ErrorContains(t, err, "1ddl")
	e.requireHistory()
	require.Equal(t, !e.b.transactionalDDL, e.tableExists("{created}"))
	if !e.b.transactionalDDL {
		e.exec(dropSQL) // Allows the retry.
	}

	require.NoError(t, e.up(e.mig("1ddl", []string{createSQL}, []string{dropSQL})))
	e.requireHistory("1ddl")
	require.True(t, e.tableExists("{created}"))

	err = e.down(e.mig("1ddl", []string{createSQL}, []string{dropSQL, failSQL}))
	require.ErrorContains(t, err, "1ddl")
	e.requireHistory("1ddl")
	require.Equal(t, e.b.transactionalDDL, e.tableExists("{created}"))
}

// testTxDisabler checks that statements succeeded before a failure remain and history changes only on success.
func testTxDisabler(t *gotesting.T, e *integrationEnv) {
	create := e.mig("1create", []string{createUsersSQL}, []string{"DROP TABLE {users}"})

	err := e.up(create, e.noTxMig("2notx", []string{insertUser(1, "a"), insertUser(1, "dup")}, nil))
	require.ErrorContains(t, err, "2notx")
	e.requireHistory("1create")
	e.requireUsers(map[int]string{1: "a"})

	upB := []string{insertUser(2, "b")}
	require.NoError(t, e.up(create, e.noTxMig("2notx", upB, nil)))
	e.requireHistory("1create", "2notx")
	e.requireUsers(map[int]string{1: "a", 2: "b"})

	err = e.mm.RunLimit([]migrate.Migration{create,
		e.noTxMig("2notx", upB, []string{"DELETE FROM {users} WHERE id = 2", insertUser(1, "dup")})},
		migrate.MigrationsDirectionDown, 1)
	require.ErrorContains(t, err, "2notx")
	e.requireHistory("1create", "2notx")
	e.requireUsers(map[int]string{1: "a"})

	require.NoError(t, e.mm.RunLimit([]migrate.Migration{create,
		e.noTxMig("2notx", upB, []string{"DELETE FROM {users}"})}, migrate.MigrationsDirectionDown, 1))
	e.requireHistory("1create")
	e.requireUsers(map[int]string{})
}

// testRawTxFlags checks that RawMigration transaction flags apply per direction:
// 2txup runs Up in a transaction and Down without one, 3txdown the opposite.
func testRawTxFlags(t *gotesting.T, e *integrationEnv) {
	create := e.mig("1create", []string{createUsersSQL}, []string{"DROP TABLE {users}"})
	txUp := func(up, down []string) migrate.Migration { return e.rawMig("2txup", up, down, false, true) }
	txDown := func(up, down []string) migrate.Migration { return e.rawMig("3txdown", up, down, true, false) }
	txUpOK := txUp([]string{insertUser(10, "a")}, []string{"DELETE FROM {users}"})
	txDownOK := txDown([]string{insertUser(30, "c")}, []string{"DELETE FROM {users} WHERE id = 30"})

	err := e.up(create, txUp([]string{insertUser(10, "a"), insertUser(10, "dup")}, nil))
	require.ErrorContains(t, err, "2txup")
	e.requireHistory("1create")
	e.requireUsers(map[int]string{})

	err = e.up(create, txUpOK, txDown([]string{insertUser(20, "b"), insertUser(20, "dup")}, nil))
	require.ErrorContains(t, err, "3txdown")
	e.requireHistory("1create", "2txup")
	e.requireUsers(map[int]string{10: "a", 20: "b"})

	require.NoError(t, e.up(create, txUpOK, txDownOK))
	e.requireHistory("1create", "2txup", "3txdown")
	e.requireUsers(map[int]string{10: "a", 20: "b", 30: "c"})

	err = e.down(create, txUpOK, txDown(nil, []string{"DELETE FROM {users} WHERE id = 30", insertUser(10, "dup")}))
	require.ErrorContains(t, err, "3txdown")
	e.requireHistory("1create", "2txup", "3txdown")
	e.requireUsers(map[int]string{10: "a", 20: "b", 30: "c"})

	require.NoError(t, e.mm.RunLimit([]migrate.Migration{create, txUpOK, txDownOK}, migrate.MigrationsDirectionDown, 1))
	e.requireHistory("1create", "2txup")

	err = e.down(create, txUp(nil, []string{"DELETE FROM {users} WHERE id = 10", insertUser(20, "dup")}), txDownOK)
	require.ErrorContains(t, err, "2txup")
	e.requireHistory("1create", "2txup")
	e.requireUsers(map[int]string{20: "b"})

	require.NoError(t, e.mm.RunLimit([]migrate.Migration{create, txUpOK, txDownOK}, migrate.MigrationsDirectionDown, 1))
	e.requireHistory("1create")
	e.requireUsers(map[int]string{})
}

func testMetadataWriteFailure(t *gotesting.T, e *integrationEnv) {
	runMetadataWriteFailure(t, e, false)
}

func testMetadataWriteFailureNoTx(t *gotesting.T, e *integrationEnv) {
	runMetadataWriteFailure(t, e, true)
}

// runMetadataWriteFailure makes history writes fail after successful DML. The metadata table is created by the test with
// a CHECK constraint rejecting the 3blocked row; a foreign key from the ref table prevents deleting the 2referenced row.
func runMetadataWriteFailure(t *gotesting.T, e *integrationEnv, disableTx bool) {
	e.exec(fmt.Sprintf("CREATE TABLE {meta} (id VARCHAR(255) NOT NULL PRIMARY KEY, applied_at %s, CHECK (id <> '3blocked'))",
		legacyAppliedAtType(e.b.dialect)))
	mig := func(id string, up, down []string) migrate.Migration {
		if disableTx {
			return e.noTxMig(id, up, down)
		}
		return e.mig(id, up, down)
	}
	migs := []migrate.Migration{
		e.mig("1create", []string{createUsersSQL}, []string{"DROP TABLE {users}"}),
		mig("2referenced", []string{insertUser(2, "b")}, []string{"DELETE FROM {users} WHERE id = 2"}),
		mig("3blocked", []string{insertUser(3, "c")}, []string{"DELETE FROM {users} WHERE id = 3"}),
	}

	err := e.up(migs...)
	require.ErrorContains(t, err, "3blocked")
	e.requireHistory("1create", "2referenced")
	if disableTx {
		e.requireUsers(map[int]string{2: "b", 3: "c"})
	} else {
		e.requireUsers(map[int]string{2: "b"})
	}

	e.exec("CREATE TABLE {ref} (migration_id VARCHAR(255) NOT NULL, FOREIGN KEY (migration_id) REFERENCES {meta} (id))")
	e.exec("INSERT INTO {ref} (migration_id) VALUES ('2referenced')")
	err = e.mm.RunLimit(migs, migrate.MigrationsDirectionDown, 1)
	require.ErrorContains(t, err, "2referenced")
	e.requireHistory("1create", "2referenced")
	if disableTx {
		e.requireUsers(map[int]string{3: "c"})
	} else {
		e.requireUsers(map[int]string{2: "b"})
	}
}
