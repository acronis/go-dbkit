/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package goquutil

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/acronis/go-dbkit"
	_ "github.com/acronis/go-dbkit/sqlite"
)

const sqlCreateAndSeedTestUsersTable = `
CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, created_at DATETIME DEFAULT NULL);
INSERT INTO users(id, name, created_at) VALUES
	(1, "Albert", "2021-11-01 00:00:00"), 
	(2, "Bob", "2021-11-01 00:00:00"), 
	(3, "John", "2021-11-01 00:00:00"), 
	(4, "Sam", "2021-11-01 00:00:00");

CREATE TABLE items (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, user_id INTEGER, name TEXT NOT NULL, created_at DATETIME DEFAULT NULL
);
INSERT INTO items(user_id, name, created_at) VALUES(1, "foo", "2021-11-01 00:00:00"), (2, "bar", "2021-11-01 00:00:00")
`

var tt, _ = time.Parse(time.RFC3339, "2021-11-01T00:00:00Z")

type User struct {
	ID        int      `db:"id" goqu:"skipinsert,skipupdate"`
	Name      string   `db:"name"`
	CreatedAt NullTime `db:"created_at"`
}

type Item struct {
	ID        sql.NullInt32  `db:"id" goqu:"skipinsert,skipupdate"`
	Name      sql.NullString `db:"name"`
	UserID    sql.NullInt32  `db:"user_id"`
	CreatedAt NullTime       `db:"created_at"`
}

type ItemWithUser struct {
	User User `db:"users"`
	Item Item `db:"items"`
}

func openAndSeedDB(t *testing.T) *DB {
	t.Helper()

	cfg := &db.Config{
		Dialect:         db.DialectSQLite,
		SQLite:          db.SQLiteConfig{Path: "file::memory:?cache=shared"},
		MaxOpenConns:    1,
		MaxIdleConns:    1,
		ConnMaxLifetime: 0,
	}

	dbConn, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	require.NoError(t, db.InitOpenedDB(dbConn, cfg, false))

	_, err = dbConn.Exec(sqlCreateAndSeedTestUsersTable)
	require.NoError(t, err)

	goquDB := goqu.New("sqlite3", dbConn)
	return NewDB(context.Background(), goquDB)
}

type goquSuite struct {
	suite.Suite
	db *DB
	bs SQLBuilderSettings
}

func TestGoqutils(t *testing.T) {
	suite.Run(t, &goquSuite{})
}

func (s *goquSuite) SetupTest() {
	s.db = openAndSeedDB(s.T())
	s.bs = SQLBuilderSettings{goqu.Dialect("sqlite3")}
}

func (s *goquSuite) TestBuildSQLAndExec() {
	_ = s.db.DoInTx(func(q Querier) error {
		var rowCount int
		s.Require().NoError(
			BuildSQLAndQueryScalar(q, s.bs.Dialect.From("users").Select(goqu.COUNT(goqu.Star())), &rowCount),
		)
		s.Require().Equal(4, rowCount)

		_, err := BuildSQLAndExec(q, s.bs.Dialect.Delete("users").Where(goqu.I("name").Eq("John")))
		s.Require().NoError(err)

		s.Require().NoError(
			BuildSQLAndQueryScalar(q, s.bs.Dialect.From("users").Select(goqu.COUNT(goqu.Star())), &rowCount),
		)
		s.Require().Equal(3, rowCount)

		return nil
	})
}

func (s *goquSuite) TestBuildSQLAndQueryScalar() {
	_ = s.db.DoInTx(func(q Querier) error {
		var name string
		s.Require().NoError(
			BuildSQLAndQueryScalar(
				q, s.bs.Dialect.From("users").Select(goqu.I("name")).Where(goqu.I("id").Eq(1)), &name,
			),
		)
		s.Require().Equal("Albert", name)
		s.Require().Equal(
			ErrNotFound,
			BuildSQLAndQueryScalar(
				q, s.bs.Dialect.From("users").Select(goqu.I("name")).Where(goqu.I("id").Eq(123)), &name,
			),
		)
		return nil
	})
}

func (s *goquSuite) TestBuildSQLAndQueryRow() {
	_ = s.db.DoInTx(func(q Querier) error {
		row, err := BuildSQLAndQueryRow(q, s.bs.Dialect.From("users").Where(goqu.I("id").Eq(1)))
		s.Require().NoError(err)
		var id int
		var name string
		var createdAt NullTime
		s.Require().NoError(row.Scan(&id, &name, &createdAt))
		s.Require().Equal(1, id)
		s.Require().Equal("Albert", name)
		s.Require().Equal(tt, createdAt.Time)
		return nil
	})
}

func (s *goquSuite) TestBuildSQLAndQuery() {
	_ = s.db.DoInTx(func(q Querier) error {
		rows, err := BuildSQLAndQuery(
			q,
			s.bs.Dialect.From("users").Select(goqu.I("id"), goqu.I("name")).
				Where(goqu.I("id")).Where(goqu.I("id").In(1, 2)),
		)
		s.Require().NoError(err)

		users := make([]User, 0, 2)

		scanF := func(s Scanner) error {
			u := User{}
			if scanErr := s.Scan(&u.ID, &u.Name); scanErr != nil {
				return scanErr
			}
			users = append(users, u)
			return nil
		}

		rowsScanned, err := ScanEachRow(rows, scanF)
		s.Require().NoError(err)
		s.Require().Equal(2, rowsScanned)
		s.Require().ElementsMatch([]User{{ID: 1, Name: "Albert"}, {ID: 2, Name: "Bob"}}, users)

		rows, err = BuildSQLAndQuery(
			q,
			s.bs.Dialect.From("users").Select(goqu.I("id"), goqu.I("name")).
				Where(goqu.I("id")).Where(goqu.I("id").In(123, 321)),
		)
		s.Require().NoError(err)
		rowsScanned, err = ScanEachRow(rows, scanF)
		s.Require().Equal(nil, err)
		s.Require().Equal(0, rowsScanned)

		return nil
	})
}

func (s *goquSuite) TestQueryAndScanValues() {
	_ = s.db.DoInTx(func(q Querier) error {
		var res []int
		s.Require().NoError(QueryAndScanValues(q, s.bs.Dialect.From("users").Select(goqu.I("id")), &res))
		s.Require().ElementsMatch([]int{1, 2, 3, 4}, res)

		s.Require().EqualError(
			QueryAndScanValues(q, s.bs.Dialect.From("users").Select(goqu.I("id"), goqu.I("name")), &res),
			"sql: expected 2 destination arguments in Scan, not 1",
		)
		return nil
	})
}

func (s *goquSuite) TestQueryAndScanStruct() {
	_ = s.db.DoInTx(func(q Querier) error {
		user := User{}
		s.Require().NoError(QueryAndScanStruct(q, s.bs.Dialect.From("users").Where(goqu.I("id").Eq(1)), &user))
		s.Require().Equal(User{1, "Albert", NullTimeFrom(tt)}, user)

		s.Require().Equal(
			ErrNotFound, QueryAndScanStruct(q, s.bs.Dialect.From("users").Where(goqu.I("id").Gte(123)), &user),
		)
		return nil
	})
}

func (s *goquSuite) TestQueryAndScanStructs() {
	_ = s.db.DoInTx(func(q Querier) error {
		users := make([]User, 0, 2)
		s.Require().NoError(QueryAndScanStructs(q, s.bs.Dialect.From("users").Where(goqu.I("id").In(1, 2)), &users))
		s.Require().ElementsMatch([]User{{1, "Albert", NullTimeFrom(tt)}, {2, "Bob", NullTimeFrom(tt)}}, users)

		users = make([]User, 0, 2)
		s.Require().NoError(
			QueryAndScanStructs(q, s.bs.Dialect.From("users").Where(goqu.I("id").Gte(123)), &users),
		)
		s.Require().Empty(users)
		return nil
	})
}

func (s *goquSuite) TestQueryAndScanCompositeStructs() {
	_ = s.db.DoInTx(func(q Querier) error {
		items := make([]ItemWithUser, 0, 2)
		s.Require().NoError(
			QueryAndScanStructs(
				q,
				s.bs.Dialect.From("users").
					LeftJoin(goqu.T("items"), goqu.On(goqu.I("items.user_id").Eq(goqu.I("users.id")))).
					Where(goqu.I("users.id").In(1, 4)),
				&items,
			),
		)

		s.Require().ElementsMatch(
			[]ItemWithUser{
				{
					User: User{1, "Albert", NullTimeFrom(tt)},
					Item: Item{
						ID:        sql.NullInt32{Int32: 1, Valid: true},
						Name:      sql.NullString{String: "foo", Valid: true},
						UserID:    sql.NullInt32{Int32: 1, Valid: true},
						CreatedAt: NullTimeFrom(tt),
					},
				},
				{
					User: User{4, "Sam", NullTimeFrom(tt)},
					Item: Item{},
				},
			},
			items,
		)

		items = make([]ItemWithUser, 0, 2)
		s.Require().NoError(
			QueryAndScanStructs(
				q,
				s.bs.Dialect.From("users").
					LeftJoin(goqu.T("items"), goqu.On(goqu.I("items.user_id").Eq(goqu.I("users.id")))).
					Where(goqu.I("users.id").Gte(123)),
				&items,
			),
		)
		s.Require().Empty(items)
		return nil
	})
}

func (s *goquSuite) TestQueryAndScanCompositeStruct() {
	_ = s.db.DoInTx(func(q Querier) error {
		item := ItemWithUser{}
		s.Require().NoError(
			QueryAndScanStruct(
				q,
				s.bs.Dialect.From("users").
					LeftJoin(goqu.T("items"), goqu.On(goqu.I("items.user_id").Eq(goqu.I("users.id")))).
					Where(goqu.I("users.id").In(1)),
				&item,
			),
		)

		s.Require().Equal(
			ItemWithUser{
				User: User{1, "Albert", NullTimeFrom(tt)},
				Item: Item{
					ID:        sql.NullInt32{Int32: 1, Valid: true},
					Name:      sql.NullString{String: "foo", Valid: true},
					UserID:    sql.NullInt32{Int32: 1, Valid: true},
					CreatedAt: NullTimeFrom(tt),
				},
			},
			item,
		)

		s.Require().Equal(
			ErrNotFound,
			QueryAndScanStruct(
				q,
				s.bs.Dialect.From("users").
					LeftJoin(goqu.T("items"), goqu.On(goqu.I("items.user_id").Eq(goqu.I("users.id")))).
					Where(goqu.I("users.id").In(123)),
				&item,
			),
		)
		return nil
	})
}

func (s *goquSuite) TestStructSelectColumnsHasFixedOrder() {
	type testT struct {
		C1 string `db:"c1"`
		C2 string `db:"c2"`
		C3 string `db:"c3"`
		C4 string `db:"c4"`
		C5 string `db:"c5"`
	}

	for i := 0; i < 100; i++ {
		cols := prepareSelectsForCompositeRecord(s.bs.Dialect.From("any_table"), testT{})
		// nolint:lll
		s.Require().Equal(
			"[{{COALESCE [{  c1} ]} {  c1}} {{COALESCE [{  c2} ]} {  c2}} {{COALESCE [{  c3} ]} {  c3}} {{COALESCE [{  c4} ]} {  c4}} {{COALESCE [{  c5} ]} {  c5}}]",
			fmt.Sprintf("%v", cols),
		)
	}
}
