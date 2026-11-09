# Upgrade fixtures

Databases written by the previous, sql-migrate based `MigrationsManager`. Tests restore them and upgrade them with the current engine. They are generated once; tests never regenerate them.

## Source

| Item | Value |
|---|---|
| go-dbkit commit | `d3d5fd68712baf09e6ca498f7250751234aca9f1` (parent of `87f15ba`, which replaced sql-migrate) |
| Migration libraries | sql-migrate v1.0.0, gorp v1.7.2 (from that commit's `go.mod`) |
| Drivers | go-sqlite3 v1.14.37, lib/pq v1.12.0, go-sql-driver/mysql v1.9.3, go-mssqldb v1.9.7 |
| Go | 1.26.0 |
| Generator | [`generate.go.txt`](generate.go.txt): applies migrations with the old public `MigrationsManager` (`NewMigrationsManager`, default `migrations` table, `RunLimit(..., up, 1)` per migration) and prints `Status`; writes the MSSQL export |
| Time zone | `TZ=UTC` |

The generator file has a `.txt` suffix so builds and `go mod tidy` of this module never load the old dependencies.

## Contents

Migrations applied by the old manager, in order:

| ID | Up | Down |
|---|---|---|
| `1_create_users` | `CREATE TABLE legacy_users (id INT NOT NULL PRIMARY KEY, name VARCHAR(100) NOT NULL)` | `DROP TABLE legacy_users` |
| `2_seed_users` | inserts `(1, 'alice')`, `(2, 'Zoë 東京')`, `(3, 'untouched')` | `DELETE FROM legacy_users WHERE id IN (1, 2, 3)` |
| `3_create_audit` | `CREATE TABLE legacy_audit (id INT NOT NULL PRIMARY KEY, note VARCHAR(100) NOT NULL)`; inserts `(1, 'created by 3')` | `DROP TABLE legacy_audit` |
| `4_` + `long_migration_id_` × 17 (308 characters) | PostgreSQL only: inserts `(4, 'long id')` into `legacy_audit` | `DELETE FROM legacy_audit WHERE id = 4` |

MSSQL uses `NVARCHAR(100)` and `N'...'` literals for the string columns. All up statements are non-idempotent: replaying any of them fails.

Recorded `applied_at` values (UTC, as stored):

| Artifact | `1_create_users` | `2_seed_users` | `3_create_audit` | `4_long…` |
|---|---|---|---|---|
| `sqlite.db` | 2026-11-09 14:26:19.913111 | 14:26:21.015626 | 14:26:22.117923 | |
| `postgres.sql` | 2026-11-09 14:26:50.934966 | 14:26:52.043798 | 14:26:53.158802 | 14:26:54.267874 |
| `mysql.sql` | 2026-11-09 14:26:55 | 14:26:57 | 14:26:58 | |
| `mariadb.sql` | 2026-11-09 14:26:58 | 14:26:59 | 14:27:01 | |
| `mssql.sql` | 2026-11-09 14:27:02.267 | 14:27:03.403 | 14:27:04.523 | |

Metadata table as created by the old manager:

| Artifact | `id` | `applied_at` | Other |
|---|---|---|---|
| `sqlite.db` | `varchar(255) not null primary key` | `datetime` | values stored as text `2006-01-02 15:04:05.999999999-07:00` |
| `postgres.sql` | `text NOT NULL`, primary key | `timestamp with time zone` | |
| `mysql.sql` | `varchar(255) NOT NULL`, primary key | `datetime DEFAULT NULL` | `ENGINE=InnoDB DEFAULT CHARSET=utf8mb3` |
| `mariadb.sql` | `varchar(255) NOT NULL`, primary key | `datetime DEFAULT NULL` | `ENGINE=InnoDB DEFAULT CHARSET=utf8mb3` |
| `mssql.sql` | `varchar(255) NOT NULL`, primary key | `datetime NULL` | |

## Servers and tools

| Artifact | Server image | Export tool |
|---|---|---|
| `sqlite.db` | none (file database, rollback journal, no WAL) | file copy after the generator closed it |
| `postgres.sql` | `postgres:16.15-alpine` | `pg_dump` 16.15 |
| `mysql.sql` | `mysql:8.4.13` | `mysqldump` 8.4.13 |
| `mariadb.sql` | `mariadb:11.4.13` | `mariadb-dump` 11.4.13 |
| `mssql.sql` | `mcr.microsoft.com/mssql/server:2022-CU27-ubuntu-22.04` | generator `-export-mssql` |

The MSSQL export is built from `sys.columns`/`sys.types` (names, types, lengths, nullability) and `sys.indexes`/`sys.index_columns` (primary key) plus a `SELECT` of every row. Strings are written as `N'...'` with doubled quotes; `DATETIME` values as `CONVERT(DATETIME, 'yyyy-mm-ddThh:mi:ss.mmm', 126)`, which converts back to the same 1/300 s value. Batches are separated by `GO` lines. The old manager needed go-mssqldb's legacy `mssql` driver because gorp binds `?` placeholders.

## Regeneration

Maintenance only. Output timestamps differ on every run; update this README and the expectations in `migrate/upgrade_fixtures_test.go`.

```sh
W=$(mktemp -d)
mkdir "$W/old" && git archive d3d5fd68712baf09e6ca498f7250751234aca9f1 | tar -x -C "$W/old"
mkdir "$W/gen" && cp migrate/testdata/upgrade/generate.go.txt "$W/gen/main.go"
cat > "$W/gen/go.mod" <<'EOF'
module upgradegen

go 1.26.0

require github.com/acronis/go-dbkit v0.0.0

replace github.com/acronis/go-dbkit => ../old

replace google.golang.org/genproto => google.golang.org/genproto v0.0.0-20250102185135-69823020774d
EOF
(cd "$W/gen" && go mod tidy && go build -o gen .)
cd "$W"
export TZ=UTC

# SQLite
./gen/gen -dialect sqlite3 -driver sqlite3 -dsn file:sqlite.db

# Servers
docker run -d --name ug-pg -e POSTGRES_USER=root -e POSTGRES_PASSWORD=password -e POSTGRES_DB=testdb -e TZ=UTC -p 127.0.0.1::5432 postgres:16.15-alpine
docker run -d --name ug-my -e MYSQL_ROOT_PASSWORD=password -e MYSQL_DATABASE=testdb -p 127.0.0.1::3306 mysql:8.4.13 --default-storage-engine=InnoDB
docker run -d --name ug-ma -e MARIADB_ROOT_PASSWORD=password -e MARIADB_DATABASE=testdb -p 127.0.0.1::3306 mariadb:11.4.13 --default-storage-engine=InnoDB
docker run -d --name ug-ms --platform linux/amd64 -e ACCEPT_EULA=Y -e 'MSSQL_SA_PASSWORD=Test-Passw0rd!' -p 127.0.0.1::1433 mcr.microsoft.com/mssql/server:2022-CU27-ubuntu-22.04
docker exec ug-ms /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P 'Test-Passw0rd!' -Q "CREATE DATABASE testdb"
port() { docker port "$1" "$2" | cut -d: -f2; }

./gen/gen -dialect postgres -driver postgres -dsn "postgres://root:password@127.0.0.1:$(port ug-pg 5432)/testdb?sslmode=disable"
./gen/gen -dialect mysql -driver mysql -dsn "root:password@tcp(127.0.0.1:$(port ug-my 3306))/testdb?parseTime=true"
./gen/gen -dialect mysql -driver mysql -dsn "root:password@tcp(127.0.0.1:$(port ug-ma 3306))/testdb?parseTime=true"
./gen/gen -dialect mssql -driver mssql -dsn "sqlserver://sa:Test-Passw0rd!@127.0.0.1:$(port ug-ms 1433)?database=testdb" -export-mssql mssql.sql

docker exec ug-pg pg_dump -U root -d testdb --no-owner --no-privileges --no-comments --no-tablespaces \
  --inserts --rows-per-insert=1 -t public.migrations -t public.legacy_users -t public.legacy_audit > postgres.sql
docker exec ug-my mysqldump -uroot -ppassword --compact --skip-extended-insert --no-tablespaces --set-gtid-purged=OFF \
  --default-character-set=utf8mb4 --skip-tz-utc testdb migrations legacy_users legacy_audit > mysql.sql
docker exec ug-ma mariadb-dump -uroot -ppassword --compact --skip-extended-insert --no-tablespaces \
  --default-character-set=utf8mb4 --skip-tz-utc testdb migrations legacy_users legacy_audit > mariadb.sql
```

Verification done after generation: each export was restored into a new database on the same server with the native client (`psql -v ON_ERROR_STOP=1`, `mysql`/`mariadb` with `--default-character-set=utf8mb4`, `sqlcmd -b`) and compared with the source: re-dumps of PostgreSQL, MySQL and MariaDB are identical; for MSSQL the catalog columns, primary keys, history and rows are identical.

## Restore notes

- `postgres.sql` contains psql meta-commands (`\restrict`, `\unrestrict`) and session `SET` statements, including an empty `search_path`. Skip meta-command lines and restore on a dedicated connection.
- `mysql.sql`/`mariadb.sql` have no `SET NAMES`; the client connection must use `utf8mb4`. `mariadb.sql` starts with a version-gated comment (`/*M!999999\- ...*/`) that has no terminating `;`.
- `mssql.sql`: execute each `GO`-separated batch separately.

## SHA-256

```
7d70fcbc549afbf1aaaa97574857cd820e84dfc3e595592a8471252b8d45f351  sqlite.db
daf6c31d8665a4fd6541e3aded0859434153c1d1a8fceccf165a39e458e5477e  postgres.sql
e0f5bdb1ab02c7263d00ebf2f981e8e5f6a72c349a90710c9b27a9f5099d092b  mysql.sql
82f399d78e0c6735fffcfa69f7675868467f81460eb30664c717223ea22223f5  mariadb.sql
144d00afe5f201a4415de1c4e4db32d1ef00d2f5b448ab756fb481a3c5853d3a  mssql.sql
```
