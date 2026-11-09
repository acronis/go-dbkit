/*
Copyright © 2026 Acronis International GmbH.

Released under MIT license.
*/

package migrate_test

import (
	"fmt"
	"strings"
	gotesting "testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/acronis/go-dbkit"
	"github.com/acronis/go-dbkit/migrate"
)

// metadataSchemaScenarios check the metadata table schema created by the package and by the previous implementation.
func metadataSchemaScenarios(b integrationBackend) []integrationScenario {
	scenarios := []integrationScenario{
		{"MetadataSchemaFresh", testMetadataSchemaFresh},
		{"MetadataSchemaLegacy", testMetadataSchemaLegacy},
		{"SchemaCompatibleVariants", testSchemaCompatibleVariants},
		{"SchemaIncompatibleVariants", testSchemaIncompatibleVariants},
		{"SchemaRequiredColumnRegression", testSchemaRequiredColumnRegression},
		{"SchemaResolution", testSchemaResolution},
	}
	if b.dialect == dbkit.DialectPostgres || b.dialect == dbkit.DialectPgx {
		scenarios = append(scenarios, integrationScenario{"PostgresLongID", testPostgresLongID})
	}
	return scenarios
}

// expectedMetadataSchema is the schema created by the previous implementation (testdata/upgrade/README.md).
type expectedMetadataSchema struct {
	idType, appliedAtType string
	engine, charset       string // MySQL and MariaDB only.
}

func (b integrationBackend) expectedMetadataSchema() expectedMetadataSchema {
	switch b.dialect {
	case dbkit.DialectPostgres, dbkit.DialectPgx:
		return expectedMetadataSchema{idType: "text", appliedAtType: "timestamp with time zone"}
	case dbkit.DialectMySQL:
		return expectedMetadataSchema{idType: "varchar(255)", appliedAtType: "datetime", engine: "InnoDB", charset: "utf8mb3"}
	default:
		return expectedMetadataSchema{idType: "varchar(255)", appliedAtType: "datetime"}
	}
}

type catalogColumn struct {
	name     string
	typeName string // Lower case declared type with length, e.g. "varchar(255)".
	nullable bool
}

type catalogTable struct {
	columns    []catalogColumn
	primaryKey []string
	engine     string
	charset    string
}

// describeTable reads table metadata from the database catalogs. It is the test oracle and does not use package code.
func (e *integrationEnv) describeTable(table string) catalogTable {
	e.t.Helper()
	var res catalogTable
	var colQuery, pkQuery string
	switch e.b.dialect {
	case dbkit.DialectSQLite:
		colQuery = `SELECT name, lower(type), "notnull" = 0 FROM pragma_table_info(?) ORDER BY cid`
		pkQuery = `SELECT name FROM pragma_table_info(?) WHERE pk > 0 ORDER BY pk`
	case dbkit.DialectPostgres, dbkit.DialectPgx:
		colQuery = `SELECT a.attname, format_type(a.atttypid, a.atttypmod), NOT a.attnotnull FROM pg_attribute a
WHERE a.attrelid = to_regclass(quote_ident($1)) AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum`
		pkQuery = `SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
WHERE i.indrelid = to_regclass(quote_ident($1)) AND i.indisprimary`
	case dbkit.DialectMySQL:
		colQuery = `SELECT COLUMN_NAME, lower(COLUMN_TYPE), IS_NULLABLE = 'YES' FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION`
		pkQuery = `SELECT COLUMN_NAME FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME = 'PRIMARY' ORDER BY SEQ_IN_INDEX`
		// The charset is the collation name prefix; MariaDB 11 collations such as utf8mb4_uca1400_ai_ci are missing from
		// COLLATION_CHARACTER_SET_APPLICABILITY.COLLATION_NAME.
		var collation string
		require.NoError(e.t, e.db.QueryRow(`SELECT ENGINE, TABLE_COLLATION FROM information_schema.TABLES
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?`, table).Scan(&res.engine, &collation))
		res.charset, _, _ = strings.Cut(collation, "_")
		if res.charset == "utf8" { // Alias of utf8mb3 in older servers.
			res.charset = "utf8mb3"
		}
	case dbkit.DialectMSSQL:
		colQuery = `SELECT c.name, t.name + CASE WHEN t.name IN ('varchar', 'char') THEN '(' + CAST(c.max_length AS VARCHAR) + ')'
WHEN t.name IN ('nvarchar', 'nchar') THEN '(' + CAST(c.max_length / 2 AS VARCHAR) + ')' ELSE '' END, c.is_nullable
FROM sys.columns c JOIN sys.types t ON t.user_type_id = c.user_type_id
WHERE c.object_id = OBJECT_ID(QUOTENAME(@p1), 'U') ORDER BY c.column_id`
		pkQuery = `SELECT c.name FROM sys.indexes i
JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
WHERE i.object_id = OBJECT_ID(QUOTENAME(@p1), 'U') AND i.is_primary_key = 1 ORDER BY ic.key_ordinal`
	}

	rows, err := e.db.Query(colQuery, table)
	require.NoError(e.t, err)
	for rows.Next() {
		var c catalogColumn
		require.NoError(e.t, rows.Scan(&c.name, &c.typeName, &c.nullable))
		c.typeName = strings.ToLower(c.typeName)
		res.columns = append(res.columns, c)
	}
	require.NoError(e.t, rows.Err())
	require.NoError(e.t, rows.Close())

	rows, err = e.db.Query(pkQuery, table)
	require.NoError(e.t, err)
	for rows.Next() {
		var name string
		require.NoError(e.t, rows.Scan(&name))
		res.primaryKey = append(res.primaryKey, name)
	}
	require.NoError(e.t, rows.Err())
	require.NoError(e.t, rows.Close())
	return res
}

// requireMetadataSchema checks that the metadata table has exactly id (non-null, sole primary key) and nullable applied_at
// with the types created by the previous implementation.
func (e *integrationEnv) requireMetadataSchema() {
	e.t.Helper()
	want := e.b.expectedMetadataSchema()
	got := e.describeTable(e.metaTable)
	require.Equal(e.t, []catalogColumn{
		{name: "id", typeName: want.idType, nullable: false},
		{name: "applied_at", typeName: want.appliedAtType, nullable: true},
	}, got.columns)
	require.Equal(e.t, []string{"id"}, got.primaryKey)
	require.Equal(e.t, want.engine, got.engine)
	require.Equal(e.t, want.charset, got.charset)
}

func testMetadataSchemaFresh(t *gotesting.T, e *integrationEnv) {
	if e.b.dialect == dbkit.DialectMySQL {
		// The database default differs from the historical charset, so a table relying on defaults fails the check.
		var charset string
		require.NoError(t, e.db.QueryRow(
			"SELECT DEFAULT_CHARACTER_SET_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = DATABASE()").Scan(&charset))
		require.Equal(t, "utf8mb4", charset)
	}
	require.Empty(t, e.status())
	e.requireMetadataSchema()

	// NULL applied_at round-trip.
	e.exec("INSERT INTO {meta} (id, applied_at) VALUES ('1create', NULL)")
	status := e.status()
	require.Len(t, status, 1)
	require.True(t, status[0].AppliedAt.IsZero())

	before := time.Now()
	e.exec(createUsersSQL)
	require.NoError(t, e.up(lifecycleMigrations(e)...))
	e.requireHistory("1create", "2seed", "10rename")
	e.requireAppliedAt("1create", time.Time{})
	for _, m := range e.status() {
		if m.ID != "1create" {
			require.WithinDuration(t, before, m.AppliedAt, time.Minute)
		}
	}
	e.requireMetadataSchema()
}

func testMetadataSchemaLegacy(t *gotesting.T, e *integrationEnv) {
	f := e.restoreUpgradeFixture()
	e.requireMetadataSchema()
	require.NoError(t, e.up(upgradeMigrations(e)...))
	e.requireFixtureHistory(f, "10_rename_alice")
	e.requireMetadataSchema() // The existing table is not altered.
}

// testPostgresLongID applies, reads and rolls back migrations with IDs longer than 255 characters on a fresh table.
func testPostgresLongID(t *gotesting.T, e *integrationEnv) {
	longID1 := "1_" + strings.Repeat("a", 300)
	longID2 := "2_" + strings.Repeat("b", 1000)
	migs := []migrate.Migration{
		e.mig(longID1, []string{createUsersSQL}, []string{"DROP TABLE {users}"}),
		e.mig(longID2, []string{insertUser(1, "alice")}, []string{"DELETE FROM {users}"}),
	}
	require.NoError(t, e.up(migs...))
	e.requireHistory(longID1, longID2)
	status := e.status()
	require.NoError(t, e.up(migs...))
	requireSameStatus(t, status, e.status())
	e.requireUsers(map[int]string{1: "alice"})

	require.NoError(t, e.mm.RunLimit(migs, migrate.MigrationsDirectionDown, 1))
	e.requireHistory(longID1)
	require.NoError(t, e.down(migs...))
	e.requireHistory()
	require.False(t, e.tableExists("{users}"))
}

// schemaVariant is a metadata table created by the test. {ts} is the backend timestamp type.
type schemaVariant struct {
	name string
	ddl  []string
	// check verifies extra columns after the manager wrote rows; optional.
	check func(t *gotesting.T, e *integrationEnv)
	// reason is a fragment of the expected incompatibility reason; empty for a compatible variant.
	reason string
}

func (e *integrationEnv) createVariant(v schemaVariant) {
	e.t.Helper()
	e.exec("DROP TABLE IF EXISTS {meta}")
	for _, ddl := range v.ddl {
		e.exec(strings.ReplaceAll(ddl, "{ts}", e.b.timestampType))
	}
}

func checkTenantAndIDLen(t *gotesting.T, e *integrationEnv) {
	var tenant string
	var idLen int
	require.NoError(t, e.db.QueryRow(e.q("SELECT tenant, id_len FROM {meta} WHERE id = '1seed'")).Scan(&tenant, &idLen))
	require.Equal(t, "acme", tenant)
	require.Equal(t, 5, idLen)
}

// compatibleVariants are existing tables the manager accepts although they differ from the tables it creates.
func compatibleVariants(b integrationBackend) []schemaVariant {
	switch b.dialect {
	case dbkit.DialectSQLite:
		return []schemaVariant{
			{name: "unique text id, reordered, timestamp, optional and generated extras", ddl: []string{
				`CREATE TABLE {meta} (note TEXT, applied_at TIMESTAMP, tenant TEXT NOT NULL DEFAULT 'acme',
				id TEXT NOT NULL UNIQUE, id_len INT GENERATED ALWAYS AS (length(id)) VIRTUAL)`,
			}, check: checkTenantAndIDLen},
			{name: "unique index, NOT NULL applied_at, integer primary key extra", ddl: []string{
				"CREATE TABLE {meta} (seq INTEGER PRIMARY KEY, id VARCHAR(64) NOT NULL, applied_at DATETIME NOT NULL)",
				"CREATE UNIQUE INDEX {meta}_id ON {meta} (id)",
			}},
		}
	case dbkit.DialectPostgres, dbkit.DialectPgx:
		return []schemaVariant{
			{name: "unique varchar id, reordered, timestamp without time zone, optional, generated and identity extras", ddl: []string{
				`CREATE TABLE {meta} (note TEXT, applied_at TIMESTAMP WITHOUT TIME ZONE, tenant VARCHAR(20) NOT NULL DEFAULT 'acme',
				id VARCHAR(255) NOT NULL UNIQUE, id_len INT GENERATED ALWAYS AS (length(id)) STORED,
				seq BIGINT GENERATED ALWAYS AS IDENTITY)`,
			}, check: checkTenantAndIDLen},
			{name: "text primary key, NOT NULL timestamptz, serial extra", ddl: []string{
				"CREATE TABLE {meta} (serial_id SERIAL, id TEXT NOT NULL PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL)",
			}},
		}
	case dbkit.DialectMySQL:
		return []schemaVariant{
			{name: "unique id, reordered, DATETIME(6), optional, generated and auto-increment extras", ddl: []string{
				`CREATE TABLE {meta} (note VARCHAR(50), applied_at DATETIME(6), tenant VARCHAR(20) NOT NULL DEFAULT 'acme',
				id VARCHAR(191) NOT NULL, id_len INT GENERATED ALWAYS AS (CHAR_LENGTH(id)) VIRTUAL,
				seq BIGINT NOT NULL AUTO_INCREMENT, UNIQUE KEY (id), UNIQUE KEY (seq))`,
			}, check: checkTenantAndIDLen},
			{name: "TIMESTAMP applied_at, utf8mb4 text id", ddl: []string{
				"CREATE TABLE {meta} (id VARCHAR(255) NOT NULL PRIMARY KEY, applied_at TIMESTAMP NULL) DEFAULT CHARSET=utf8mb4",
			}},
		}
	default: // MSSQL
		return []schemaVariant{
			{name: "unique nvarchar id, reordered, DATETIME2, optional, computed and identity extras", ddl: []string{
				`CREATE TABLE {meta} (note NVARCHAR(50), applied_at DATETIME2, tenant VARCHAR(20) NOT NULL DEFAULT 'acme',
				id NVARCHAR(255) NOT NULL UNIQUE, id_len AS LEN(id), seq BIGINT IDENTITY(1, 1))`,
			}, check: checkTenantAndIDLen},
			{name: "NOT NULL DATETIMEOFFSET, rowversion extra", ddl: []string{
				"CREATE TABLE {meta} (id VARCHAR(255) NOT NULL PRIMARY KEY, applied_at DATETIMEOFFSET NOT NULL, rv ROWVERSION)",
			}},
		}
	}
}

// incompatibleVariants are tables the manager rejects before running migrations.
func incompatibleVariants(b integrationBackend) []schemaVariant {
	const noUnique = `column "id" has no primary key or unique index on it alone`
	variants := []schemaVariant{
		{name: "missing applied_at", ddl: []string{"CREATE TABLE {meta} (id VARCHAR(255) NOT NULL PRIMARY KEY)"},
			reason: `column "applied_at" is missing`},
		{name: "missing id", ddl: []string{"CREATE TABLE {meta} (name VARCHAR(255) NOT NULL PRIMARY KEY, applied_at {ts})"},
			reason: `column "id" is missing`},
		{name: "numeric id", ddl: []string{"CREATE TABLE {meta} (id INT NOT NULL PRIMARY KEY, applied_at {ts})"},
			reason: `column "id" has unsupported type`},
		{name: "string applied_at", ddl: []string{"CREATE TABLE {meta} (id VARCHAR(255) NOT NULL PRIMARY KEY, applied_at VARCHAR(50))"},
			reason: `column "applied_at" has unsupported type`},
		{name: "date-only applied_at", ddl: []string{"CREATE TABLE {meta} (id VARCHAR(255) NOT NULL PRIMARY KEY, applied_at DATE)"},
			reason: `column "applied_at" has unsupported type`},
		{name: "nullable unique id", ddl: []string{"CREATE TABLE {meta} (id VARCHAR(255) NULL UNIQUE, applied_at {ts})"},
			reason: `column "id" is nullable`},
		{name: "non-unique id", ddl: []string{"CREATE TABLE {meta} (id VARCHAR(255) NOT NULL, applied_at {ts})"}, reason: noUnique},
		{name: "composite primary key", ddl: []string{
			"CREATE TABLE {meta} (id VARCHAR(255) NOT NULL, applied_at {ts} NOT NULL, PRIMARY KEY (id, applied_at))"}, reason: noUnique},
		{name: "required extra column", ddl: []string{
			"CREATE TABLE {meta} (id VARCHAR(255) NOT NULL PRIMARY KEY, applied_at {ts}, tenant_id INT NOT NULL)"},
			reason: `column "tenant_id" is NOT NULL without a default`},
	}
	nonUniqueTable := "CREATE TABLE {meta} (id VARCHAR(255) NOT NULL, applied_at {ts})"
	switch b.dialect {
	case dbkit.DialectSQLite:
		variants = append(variants,
			schemaVariant{name: "partial unique index", reason: noUnique,
				ddl: []string{nonUniqueTable, "CREATE UNIQUE INDEX {meta}_id ON {meta} (id) WHERE id <> ''"}},
			schemaVariant{name: "expression unique index", reason: noUnique,
				ddl: []string{nonUniqueTable, "CREATE UNIQUE INDEX {meta}_id ON {meta} (lower(id))"}},
			schemaVariant{name: "generated applied_at", reason: `column "applied_at" is generated`, ddl: []string{
				"CREATE TABLE {meta} (id VARCHAR(255) NOT NULL PRIMARY KEY, applied_at DATETIME GENERATED ALWAYS AS ('2020-01-01') VIRTUAL)"}},
			schemaVariant{name: "NOT NULL DEFAULT NULL extra", reason: `column "tenant_id" is NOT NULL without a default`, ddl: []string{
				"CREATE TABLE {meta} (id VARCHAR(255) NOT NULL PRIMARY KEY, applied_at {ts}, tenant_id INT NOT NULL DEFAULT NULL)"}},
		)
	case dbkit.DialectPostgres, dbkit.DialectPgx:
		variants = append(variants,
			schemaVariant{name: "partial unique index", reason: noUnique,
				ddl: []string{nonUniqueTable, "CREATE UNIQUE INDEX ON {meta} (id) WHERE id <> ''"}},
			schemaVariant{name: "expression unique index", reason: noUnique,
				ddl: []string{nonUniqueTable, "CREATE UNIQUE INDEX ON {meta} (lower(id))"}},
			schemaVariant{name: "generated applied_at", reason: `column "applied_at" is generated`, ddl: []string{
				"CREATE TABLE {meta} (id TEXT NOT NULL PRIMARY KEY, applied_at TIMESTAMP GENERATED ALWAYS AS (make_timestamp(2020, 1, 1, 0, 0, 0)) STORED)"}},
			schemaVariant{name: "NOT NULL DEFAULT NULL extra", reason: `column "tenant_id" is NOT NULL without a default`, ddl: []string{
				"CREATE TABLE {meta} (id TEXT NOT NULL PRIMARY KEY, applied_at {ts}, tenant_id INT NOT NULL DEFAULT NULL)"}},
		)
	case dbkit.DialectMySQL:
		variants = append(variants,
			schemaVariant{name: "prefix unique key", reason: noUnique,
				ddl: []string{"CREATE TABLE {meta} (id VARCHAR(255) NOT NULL, applied_at {ts}, UNIQUE KEY (id(10)))"}},
			schemaVariant{name: "binary id", reason: `column "id" has unsupported type`,
				ddl: []string{"CREATE TABLE {meta} (id VARBINARY(255) NOT NULL PRIMARY KEY, applied_at {ts})"}},
			schemaVariant{name: "generated applied_at", reason: `column "applied_at" is generated`, ddl: []string{
				"CREATE TABLE {meta} (id VARCHAR(255) NOT NULL PRIMARY KEY, applied_at DATETIME AS ('2020-01-01 00:00:00') VIRTUAL)"}},
		)
		if b.name == "MySQL" { // MariaDB has no functional key parts.
			variants = append(variants, schemaVariant{name: "functional unique key", reason: noUnique,
				ddl: []string{"CREATE TABLE {meta} (id VARCHAR(255) NOT NULL, applied_at {ts}, UNIQUE KEY ((lower(id))))"}})
		}
	case dbkit.DialectMSSQL:
		variants = append(variants,
			schemaVariant{name: "filtered unique index", reason: noUnique,
				ddl: []string{nonUniqueTable, "CREATE UNIQUE INDEX ix_id ON {meta} (id) WHERE id <> ''"}},
			schemaVariant{name: "disabled unique index", reason: noUnique,
				ddl: []string{nonUniqueTable, "CREATE UNIQUE INDEX ix_id ON {meta} (id)", "ALTER INDEX ix_id ON {meta} DISABLE"}},
			schemaVariant{name: "computed applied_at", reason: `column "applied_at" is generated`, ddl: []string{
				"CREATE TABLE {meta} (id VARCHAR(255) NOT NULL PRIMARY KEY, applied_at AS CONVERT(DATETIME, '2020-01-01', 126))"}},
			schemaVariant{name: "legacy text id", reason: `column "id" has unsupported type`,
				ddl: []string{"CREATE TABLE {meta} (id TEXT NOT NULL, applied_at {ts})"}},
			schemaVariant{name: "NOT NULL DEFAULT NULL extra", reason: `column "tenant_id" is NOT NULL without a default`, ddl: []string{
				"CREATE TABLE {meta} (id VARCHAR(255) NOT NULL PRIMARY KEY, applied_at {ts}, tenant_id INT NOT NULL DEFAULT NULL)"}},
		)
	}
	return variants
}

// testSchemaCompatibleVariants writes, reads and deletes history in accepted tables and keeps an existing row.
func testSchemaCompatibleVariants(t *gotesting.T, e *integrationEnv) {
	existingAt := time.Date(2023, 3, 4, 5, 6, 7, 0, time.UTC)
	for _, v := range compatibleVariants(e.b) {
		t.Run(v.name, func(t *gotesting.T) {
			e.t = t
			e.reopen()
			e.createVariant(v)
			e.exec("DROP TABLE IF EXISTS {users}")
			e.exec(createUsersSQL)
			e.exec(fmt.Sprintf("INSERT INTO {meta} (id, applied_at) VALUES ('0create', %s)", e.b.placeholder(1)), existingAt)
			migs := []migrate.Migration{
				e.mig("0create", []string{createUsersSQL}, []string{"DROP TABLE {users}"}), // Replay would fail.
				e.mig("1seed", []string{insertUser(1, "alice")}, []string{"DELETE FROM {users} WHERE id = 1"}),
				e.mig("2bob", []string{insertUser(2, "bob")}, []string{"DELETE FROM {users} WHERE id = 2"}),
			}
			require.NoError(t, e.up(migs...))
			e.requireHistory("0create", "1seed", "2bob")
			e.requireUsers(map[int]string{1: "alice", 2: "bob"})
			e.requireAppliedAt("0create", existingAt)
			if v.check != nil {
				v.check(t, e)
			}
			require.NoError(t, e.mm.RunLimit(migs, migrate.MigrationsDirectionDown, 1))
			e.requireHistory("0create", "1seed")
			e.requireUsers(map[int]string{1: "alice"})
			require.NoError(t, e.down(migs...))
			e.requireHistory()
			require.False(t, e.tableExists("{users}"))
		})
	}
}

// testSchemaIncompatibleVariants checks that Run in both directions and Status reject the table before any migration
// SQL and leave the table and data unchanged.
func testSchemaIncompatibleVariants(t *gotesting.T, e *integrationEnv) {
	for _, v := range incompatibleVariants(e.b) {
		t.Run(v.name, func(t *gotesting.T) {
			e.t = t
			e.reopen()
			e.createVariant(v)
			e.exec("DROP TABLE IF EXISTS {users}")
			e.exec(createUsersSQL)
			e.exec(insertUser(1, "alice"))
			before := e.describeTable(e.metaTable)
			migs := []migrate.Migration{e.mig("1rename",
				[]string{"UPDATE {users} SET name = 'changed'"}, []string{"DELETE FROM {users}"})}

			requireIncompatible := func(err error) {
				t.Helper()
				require.ErrorContains(t, err, fmt.Sprintf("migration history table %q (dialect %s) is incompatible: ",
					e.metaTable, e.mm.Dialect)) // The manager reports pgx as postgres.
				require.ErrorContains(t, err, v.reason)
				require.ErrorContains(t, err, "configure MigrationsManagerOpts.TableName")
			}
			_, err := e.mm.Status()
			requireIncompatible(err)
			requireIncompatible(e.up(migs...))
			requireIncompatible(e.down(migs...))
			e.requireUsers(map[int]string{1: "alice"})
			require.Equal(t, before, e.describeTable(e.metaTable))
		})
	}
}

// testSchemaRequiredColumnRegression: a history table with a required extra column must be rejected before migration
// SQL runs, also when the migration runs without a transaction; otherwise the history insert fails after the data was
// changed. The previous implementation rejected such a table before execution as well.
func testSchemaRequiredColumnRegression(t *gotesting.T, e *integrationEnv) {
	appliedAt := time.Date(2022, 1, 2, 3, 4, 5, 0, time.UTC)
	e.exec(fmt.Sprintf("CREATE TABLE {meta} (id VARCHAR(255) NOT NULL PRIMARY KEY, applied_at %s, tenant_id INT NOT NULL)",
		e.b.timestampType))
	e.exec(fmt.Sprintf("INSERT INTO {meta} (id, applied_at, tenant_id) VALUES ('1create', %s, 7)", e.b.placeholder(1)), appliedAt)
	e.exec(createUsersSQL)
	e.exec(insertUser(1, "alice"))
	before := e.describeTable(e.metaTable)

	create := e.mig("1create", []string{createUsersSQL}, []string{"DROP TABLE {users}"})
	up, down := []string{"UPDATE {users} SET name = 'changed'"}, []string{"DELETE FROM {users}"}
	modes := map[string]migrate.Migration{
		"transaction":       e.mig("2rename", up, down),
		"TxDisabler":        e.noTxMig("2rename", up, down),
		"raw up opt-out":    e.rawMig("2rename", up, down, true, false),
		"raw down opt-out":  e.rawMig("2rename", up, down, false, true),
		"raw both opt-outs": e.rawMig("2rename", up, down, true, true),
	}
	for name, mig := range modes {
		for _, direction := range []migrate.MigrationsDirection{migrate.MigrationsDirectionUp, migrate.MigrationsDirectionDown} {
			err := e.mm.Run([]migrate.Migration{create, mig}, direction)
			require.ErrorContains(t, err, `column "tenant_id" is NOT NULL without a default`, "%s %s", name, direction)
			require.ErrorContains(t, err, "MigrationsManagerOpts.TableName", "%s %s", name, direction)
		}
	}
	_, err := e.mm.Status()
	require.ErrorContains(t, err, `column "tenant_id" is NOT NULL without a default`)

	e.requireUsers(map[int]string{1: "alice"})
	require.Equal(t, before, e.describeTable(e.metaTable))
	var id string
	var tenant, rows int
	require.NoError(t, e.db.QueryRow(e.q("SELECT COUNT(*) FROM {meta}")).Scan(&rows))
	require.NoError(t, e.db.QueryRow(e.q("SELECT id, tenant_id FROM {meta}")).Scan(&id, &tenant))
	require.Equal(t, []any{1, "1create", 7}, []any{rows, id, tenant})

	// A separate, compatible history table works; the rejected table stays untouched.
	alt := e.prefix + "alt"
	e.extraDrops = append(e.extraDrops, alt)
	altManager := e.newManager(migrate.MigrationsManagerOpts{TableName: alt})
	require.NoError(t, altManager.Run([]migrate.Migration{modes["transaction"]}, migrate.MigrationsDirectionUp))
	e.requireUsers(map[int]string{1: "changed"})
	require.Equal(t, before, e.describeTable(e.metaTable))
	require.NoError(t, e.db.QueryRow(e.q("SELECT COUNT(*) FROM {meta}")).Scan(&rows))
	require.Equal(t, 1, rows)
}

// testSchemaResolution checks that a same-name table in another schema or database does not affect validation.
func testSchemaResolution(t *gotesting.T, e *integrationEnv) {
	var other, qualified string
	switch e.b.dialect {
	case dbkit.DialectPostgres, dbkit.DialectPgx:
		other = e.prefix + "schema"
		e.exec("CREATE SCHEMA " + other)
		t.Cleanup(func() { _, _ = e.db.Exec("DROP SCHEMA " + other + " CASCADE") })
		qualified = other + "." + e.metaTable
	case dbkit.DialectMySQL:
		other = e.prefix + "db"
		e.exec("CREATE DATABASE " + other)
		t.Cleanup(func() { _, _ = e.db.Exec("DROP DATABASE " + other) })
		qualified = other + "." + e.metaTable
	case dbkit.DialectMSSQL:
		other = e.prefix + "schema"
		e.exec("EXEC('CREATE SCHEMA " + other + "')")
		qualified = other + "." + e.metaTable
		t.Cleanup(func() {
			_, _ = e.db.Exec("DROP TABLE IF EXISTS " + qualified)
			_, _ = e.db.Exec("DROP SCHEMA " + other)
		})
	default:
		return // SQLite: covered by TestMetadataSchema_SQLiteResolution, which needs a single connection.
	}

	// Incompatible table elsewhere, none in the current schema: the manager creates and accepts its own.
	e.exec(fmt.Sprintf("CREATE TABLE %s (id INT, applied_at DATE, tenant_id INT NOT NULL)", qualified))
	require.Empty(t, e.status())
	e.requireMetadataSchema()

	// Incompatible table in the current schema, compatible table elsewhere: rejected.
	e.exec("DROP TABLE " + qualified)
	e.exec(fmt.Sprintf("CREATE TABLE %s (id VARCHAR(255) NOT NULL PRIMARY KEY, applied_at %s)", qualified, e.b.timestampType))
	e.exec("DROP TABLE {meta}")
	e.exec(fmt.Sprintf("CREATE TABLE {meta} (id VARCHAR(255) NOT NULL PRIMARY KEY, applied_at %s, tenant_id INT NOT NULL)",
		e.b.timestampType))
	_, err := e.mm.Status()
	require.ErrorContains(t, err, `column "tenant_id" is NOT NULL without a default`)
}
