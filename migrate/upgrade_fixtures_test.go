/*
Copyright © 2026 Acronis International GmbH.

Released under MIT license.
*/

package migrate_test

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	gotesting "testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/acronis/go-dbkit"
	"github.com/acronis/go-dbkit/migrate"
)

// Artifacts in testdata/upgrade were written by the sql-migrate based MigrationsManager; see testdata/upgrade/README.md.
const upgradeFixtureDir = "testdata/upgrade"

// upgradeFixtureLongID is the applied migration ID longer than 255 characters stored in postgres.sql.
var upgradeFixtureLongID = "4_" + strings.Repeat("long_migration_id_", 17)

type upgradeFixture struct {
	file   string
	sha256 string
	// history is the recorded applied_at of every applied migration, as stored.
	history map[string]time.Time
}

func fixtureTime(value string) time.Time {
	tm, err := time.Parse("2006-01-02 15:04:05.999999", value)
	if err != nil {
		panic(err)
	}
	return tm
}

// upgradeFixtures maps backend names to artifacts. Both PostgreSQL drivers use the same dump.
var upgradeFixtures = func() map[string]upgradeFixture {
	postgres := upgradeFixture{
		file: "postgres.sql", sha256: "daf6c31d8665a4fd6541e3aded0859434153c1d1a8fceccf165a39e458e5477e",
		history: map[string]time.Time{
			"1_create_users":     fixtureTime("2026-11-09 14:26:50.934966"),
			"2_seed_users":       fixtureTime("2026-11-09 14:26:52.043798"),
			"3_create_audit":     fixtureTime("2026-11-09 14:26:53.158802"),
			upgradeFixtureLongID: fixtureTime("2026-11-09 14:26:54.267874"),
		},
	}
	return map[string]upgradeFixture{
		"SQLite": {
			file: "sqlite.db", sha256: "7d70fcbc549afbf1aaaa97574857cd820e84dfc3e595592a8471252b8d45f351",
			history: map[string]time.Time{
				"1_create_users": fixtureTime("2026-11-09 14:26:19.913111"),
				"2_seed_users":   fixtureTime("2026-11-09 14:26:21.015626"),
				"3_create_audit": fixtureTime("2026-11-09 14:26:22.117923"),
			},
		},
		"Postgres": postgres,
		"Pgx":      postgres,
		"MySQL": {
			file: "mysql.sql", sha256: "e0f5bdb1ab02c7263d00ebf2f981e8e5f6a72c349a90710c9b27a9f5099d092b",
			history: map[string]time.Time{
				"1_create_users": fixtureTime("2026-11-09 14:26:55"),
				"2_seed_users":   fixtureTime("2026-11-09 14:26:57"),
				"3_create_audit": fixtureTime("2026-11-09 14:26:58"),
			},
		},
		"MariaDB": {
			file: "mariadb.sql", sha256: "82f399d78e0c6735fffcfa69f7675868467f81460eb30664c717223ea22223f5",
			history: map[string]time.Time{
				"1_create_users": fixtureTime("2026-11-09 14:26:58"),
				"2_seed_users":   fixtureTime("2026-11-09 14:26:59"),
				"3_create_audit": fixtureTime("2026-11-09 14:27:01"),
			},
		},
		"MSSQL": {
			file: "mssql.sql", sha256: "144d00afe5f201a4415de1c4e4db32d1ef00d2f5b448ab756fb481a3c5853d3a",
			history: map[string]time.Time{
				"1_create_users": fixtureTime("2026-11-09 14:27:02.267"),
				"2_seed_users":   fixtureTime("2026-11-09 14:27:03.403"),
				"3_create_audit": fixtureTime("2026-11-09 14:27:04.523"),
			},
		},
	}
}()

// Fixed table names in the artifacts.
const (
	fixtureMetaTable  = "migrations"
	fixtureUsersTable = "legacy_users"
	fixtureAuditTable = "legacy_audit"
)

// readUpgradeFixture returns the artifact bytes after checking they match the recorded hash.
func readUpgradeFixture(t *gotesting.T, f upgradeFixture) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(upgradeFixtureDir, f.file))
	require.NoError(t, err)
	sum := sha256.Sum256(data)
	require.Equal(t, f.sha256, hex.EncodeToString(sum[:]), "artifact %s changed", f.file)
	return data
}

// restoreUpgradeFixture restores the backend artifact and renames its tables to the scenario names:
// legacy_users -> {users}, legacy_audit -> {items}, migrations -> {meta}.
// SQLite opens a copy of the database file; server dumps are executed statement by statement on a dedicated connection,
// so session settings of the dump do not leak into the scenario pool.
func (e *integrationEnv) restoreUpgradeFixture() upgradeFixture {
	e.t.Helper()
	f, ok := upgradeFixtures[e.b.name]
	require.True(e.t, ok, "no upgrade fixture for %s", e.b.name)
	data := readUpgradeFixture(e.t, f)
	// Source names are dropped on cleanup too, in case restore or rename fails midway.
	e.extraDrops = append(e.extraDrops, fixtureUsersTable, fixtureAuditTable, fixtureMetaTable)

	if e.b.dialect == dbkit.DialectSQLite {
		require.NoError(e.t, e.db.Close())
		require.NoError(e.t, os.WriteFile(e.conn.sqliteFile, data, 0o600))
		e.open()
	} else {
		session := e.session()
		defer func() { require.NoError(e.t, session.Close()) }()
		for _, stmt := range splitSQLStatements(e.b.dialect, string(data)) {
			_, err := session.Exec(stmt)
			require.NoError(e.t, err, "restore %s: %s", f.file, stmt)
		}
	}

	e.renameTable(fixtureUsersTable, e.q("{users}"))
	e.renameTable(fixtureAuditTable, e.q("{items}"))
	if e.metaTable != fixtureMetaTable {
		e.renameTable(fixtureMetaTable, e.metaTable)
	}
	return f
}

func (e *integrationEnv) renameTable(from, to string) {
	e.t.Helper()
	var query string
	switch e.b.dialect {
	case dbkit.DialectMySQL:
		query = fmt.Sprintf("RENAME TABLE %s TO %s", from, to)
	case dbkit.DialectMSSQL:
		query = fmt.Sprintf("EXEC sp_rename '%s', '%s'", from, to)
	default:
		query = fmt.Sprintf("ALTER TABLE %s RENAME TO %s", from, to)
	}
	_, err := e.db.Exec(query)
	require.NoError(e.t, err, query)
}

// splitSQLStatements splits a dump into statements, keeping statement boundaries of the dialect:
// MSSQL batches are separated by GO lines; PostgreSQL and MySQL statements end with a semicolon outside of quotes and
// comments. psql meta-command lines (e.g. \restrict) and plain comments are dropped; MySQL executable comments
// (/*! ... */, /*M! ... */) are kept.
func splitSQLStatements(dialect dbkit.Dialect, text string) []string {
	if dialect == dbkit.DialectMSSQL {
		var res []string
		var batch strings.Builder
		flush := func() {
			if stmt := strings.TrimSpace(batch.String()); stmt != "" {
				res = append(res, stmt)
			}
			batch.Reset()
		}
		for _, line := range strings.SplitAfter(text, "\n") {
			if strings.EqualFold(strings.TrimSpace(line), "GO") {
				flush()
				continue
			}
			batch.WriteString(line)
		}
		flush()
		return res
	}

	mysql := dialect == dbkit.DialectMySQL
	var res []string
	var stmt strings.Builder
	meaningful := false // Statement has content other than whitespace and plain comments.
	atLineStart := true
	flush := func() {
		if meaningful {
			res = append(res, strings.TrimSpace(stmt.String()))
		}
		stmt.Reset()
		meaningful = false
	}
	for i := 0; i < len(text); {
		c := text[i]
		switch {
		case atLineStart && !mysql && c == '\\' && !meaningful: // psql meta-command.
			for i < len(text) && text[i] != '\n' {
				i++
			}
			continue
		case c == '-' && strings.HasPrefix(text[i:], "--"), mysql && c == '#':
			end := strings.IndexByte(text[i:], '\n')
			if end < 0 {
				end = len(text) - i
			}
			i += end // Plain comments are dropped.
			continue
		case c == '/' && strings.HasPrefix(text[i:], "/*"):
			end := strings.Index(text[i+2:], "*/")
			if end < 0 {
				end = len(text) - i - 4
			}
			comment := text[i : i+2+end+2]
			if mysql && (strings.HasPrefix(comment, "/*!") || strings.HasPrefix(comment, "/*M!")) {
				meaningful = true
				stmt.WriteString(comment) // Executable comment.
			}
			i += len(comment)
			atLineStart = false
			continue
		case c == '\'' || c == '"' || (mysql && c == '`'):
			j := i + 1
			for j < len(text) {
				if mysql && text[j] == '\\' && c != '`' {
					j += 2
					continue
				}
				if text[j] == c {
					if j+1 < len(text) && text[j+1] == c { // Doubled quote.
						j += 2
						continue
					}
					break
				}
				j++
			}
			stmt.WriteString(text[i:min(j+1, len(text))])
			i = j + 1
			meaningful = true
			atLineStart = false
			continue
		case c == ';':
			stmt.WriteByte(c)
			i++
			flush()
			atLineStart = false
			continue
		}
		stmt.WriteByte(c)
		if c == '\n' {
			atLineStart = true
		} else {
			if c != ' ' && c != '\t' && c != '\r' {
				meaningful = true
			}
			atLineStart = false
		}
		i++
	}
	flush()
	return res
}

func TestSplitSQLStatements(t *gotesting.T) {
	tests := []struct {
		name    string
		dialect dbkit.Dialect
		text    string
		want    []string
	}{
		{
			name:    "postgres meta-commands, comments and quotes",
			dialect: dbkit.DialectPostgres,
			text: "\\restrict abc\n--\n-- comment; with semicolon\n--\nSET a = 0;\n" +
				"INSERT INTO t VALUES (1, 'x;y''z', \"q;\");\n\\unrestrict abc\n",
			want: []string{"SET a = 0;", "INSERT INTO t VALUES (1, 'x;y''z', \"q;\");"},
		},
		{
			name:    "mysql executable comments, backslash escapes and backticks",
			dialect: dbkit.DialectMySQL,
			text: "/*M!999999\\- enable the sandbox mode */ \n/*!40101 SET @a = 1 */;\n" +
				"/* plain; comment */\nINSERT INTO `t;` VALUES ('a\\';b');\n# trailing\n",
			want: []string{
				"/*M!999999\\- enable the sandbox mode */ \n/*!40101 SET @a = 1 */;",
				"INSERT INTO `t;` VALUES ('a\\';b');",
			},
		},
		{
			name:    "mssql batches",
			dialect: dbkit.DialectMSSQL,
			text:    "-- header\nCREATE TABLE t (id INT);\nGO\nINSERT INTO t VALUES (1);\ngo\n",
			want:    []string{"-- header\nCREATE TABLE t (id INT);", "INSERT INTO t VALUES (1);"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *gotesting.T) {
			require.Equal(t, tt.want, splitSQLStatements(tt.dialect, tt.text))
		})
	}
}

// upgradeMigrations are the migrations recorded in the artifacts plus the pending 10_rename_alice.
// The recorded ones are not idempotent, so a replay fails.
func upgradeMigrations(e *integrationEnv) []migrate.Migration {
	str, unicodeName := "VARCHAR(100)", "'Zoë 東京'"
	if e.b.dialect == dbkit.DialectMSSQL {
		str, unicodeName = "NVARCHAR(100)", "N'Zoë 東京'"
	}
	migs := []migrate.Migration{
		e.mig("1_create_users", []string{"CREATE TABLE {users} (id INT NOT NULL PRIMARY KEY, name " + str + " NOT NULL)"},
			[]string{"DROP TABLE {users}"}),
		e.mig("2_seed_users",
			[]string{insertUser(1, "alice"), "INSERT INTO {users} (id, name) VALUES (2, " + unicodeName + ")", insertUser(3, "untouched")},
			[]string{"DELETE FROM {users} WHERE id IN (1, 2, 3)"}),
		e.mig("3_create_audit",
			[]string{"CREATE TABLE {items} (id INT NOT NULL PRIMARY KEY, note " + str + " NOT NULL)",
				"INSERT INTO {items} (id, note) VALUES (1, 'created by 3')"},
			[]string{"DROP TABLE {items}"}),
		e.mig("10_rename_alice",
			[]string{"UPDATE {users} SET name = 'alice v2' WHERE id = 1"},
			[]string{"UPDATE {users} SET name = 'alice' WHERE id = 1"}),
	}
	if e.b.dialect == dbkit.DialectPostgres || e.b.dialect == dbkit.DialectPgx {
		migs = append(migs, e.mig(upgradeFixtureLongID,
			[]string{"INSERT INTO {items} (id, note) VALUES (4, 'long id')"},
			[]string{"DELETE FROM {items} WHERE id = 4"}))
	}
	return migs
}

// requireFixtureHistory checks the exact applied IDs and that recorded timestamps are unchanged.
// Migrations applied by the current engine are listed in added.
func (e *integrationEnv) requireFixtureHistory(f upgradeFixture, added ...string) {
	e.t.Helper()
	ids := append([]string{}, added...)
	for id := range f.history {
		ids = append(ids, id)
	}
	e.requireHistory(ids...)
	for _, m := range e.status() {
		if want, ok := f.history[m.ID]; ok {
			require.True(e.t, want.Equal(m.AppliedAt), "applied_at of %s: want %v, got %v", m.ID, want, m.AppliedAt)
		}
	}
}

func (e *integrationEnv) items() map[int]string {
	e.t.Helper()
	return e.queryNames("SELECT id, note FROM {items}")
}

// testUpgradeFromArtifact upgrades a database written by the sql-migrate based manager.
func testUpgradeFromArtifact(t *gotesting.T, e *integrationEnv) {
	f := e.restoreUpgradeFixture()
	baseUsers := map[int]string{1: "alice", 2: "Zoë 東京", 3: "untouched"}
	baseItems := map[int]string{1: "created by 3"}
	if _, ok := f.history[upgradeFixtureLongID]; ok {
		baseItems[4] = "long id"
	}
	upgradedUsers := map[int]string{1: "alice v2", 2: "Zoë 東京", 3: "untouched"}

	// Baseline before running the current engine.
	e.requireFixtureHistory(f)
	e.requireUsers(baseUsers)
	require.Equal(t, baseItems, e.items())

	migs := upgradeMigrations(e)
	require.NoError(t, e.up(migs...))
	e.requireFixtureHistory(f, "10_rename_alice")
	e.requireUsers(upgradedUsers)
	require.Equal(t, baseItems, e.items())

	status := e.status()
	require.NoError(t, e.up(migs...)) // No-op.
	requireSameStatus(t, status, e.status())
	e.requireUsers(upgradedUsers)

	require.NoError(t, e.mm.RunLimit(migs, migrate.MigrationsDirectionDown, 1))
	e.requireFixtureHistory(f)
	e.requireUsers(baseUsers)
	require.Equal(t, baseItems, e.items())

	require.NoError(t, e.up(migs...))
	e.requireFixtureHistory(f, "10_rename_alice")
	e.requireUsers(upgradedUsers)

	require.NoError(t, e.down(migs...))
	e.requireHistory()
	require.False(t, e.tableExists("{users}"))
	require.False(t, e.tableExists("{items}"))

	readUpgradeFixture(t, f) // The artifact is unchanged.
}
