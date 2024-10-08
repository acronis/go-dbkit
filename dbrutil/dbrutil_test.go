/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package dbrutil

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/acronis/go-appkit/log/logtest"
	"github.com/acronis/go-appkit/testutil"
	"github.com/gocraft/dbr/v2"
	_ "github.com/mattn/go-sqlite3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/acronis/go-dbkit"
)

const sqlCreateAndSeedTestUsersTable = `
CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL);
INSERT INTO users(name) VALUES("Albert"), ("Bob"), ("John"), ("Sam"), ("Sam");
`

func openAndSeedDB(t *testing.T) *dbr.Connection {
	t.Helper()

	cfg := &db.Config{
		Dialect:         db.DialectSQLite,
		SQLite:          db.SQLiteConfig{Path: "file::memory:?cache=shared"},
		MaxOpenConns:    1,
		MaxIdleConns:    1,
		ConnMaxLifetime: 0,
	}
	dbConn, err := Open(cfg, true, nil)
	require.NoError(t, err)

	_, err = dbConn.Exec(sqlCreateAndSeedTestUsersTable)
	require.NoError(t, err)

	return dbConn
}

func countUsersByName(t *testing.T, dbSess dbr.SessionRunner, annotation, name string, wantCount int) {
	t.Helper()
	var usersCount int
	err := dbSess.
		Select("COUNT(*)").
		From("users").
		Where(dbr.Eq("name", name)).
		Comment(annotation).
		LoadOne(&usersCount)
	require.NoError(t, err)
	require.Equal(t, wantCount, usersCount)
}

func TestDbrBegTxContextCancel(t *testing.T) {
	dbConn := openAndSeedDB(t)
	defer func() {
		require.NoError(t, dbConn.Close())
	}()

	tx := NewTxSession(dbConn, &sql.TxOptions{Isolation: sql.LevelDefault})
	var wg sync.WaitGroup
	count := 100
	wg.Add(count)
	for i := 0; i < count; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			cancel()
			wg.Done()
		}()
		err := tx.DoInTx(ctx, func(runner dbr.SessionRunner) error {
			return nil
		})
		if txErr, ok := err.(*TxBeginError); ok && txErr.Inner == ctx.Err() {
			err = nil
		}
		require.NoError(t, err)
	}
	wg.Wait()
}

func TestDbrOpen(t *testing.T) {
	dbConn := openAndSeedDB(t)
	defer func() {
		require.NoError(t, dbConn.Close())
	}()

	dbSess := dbConn.NewSession(nil)
	var usersCount int
	require.NoError(t, dbSess.Select("COUNT(*)").From("users").LoadOne(&usersCount))
	require.Equal(t, 5, usersCount)
}

func TestDbrSlowQueryLogEventReceiver_TimingKv(t *testing.T) {
	dbConn := openAndSeedDB(t)
	defer func() {
		require.NoError(t, dbConn.Close())
	}()

	t.Run("fast query is not logged", func(t *testing.T) {
		logRecorder := logtest.NewRecorder()
		slowQueryEventReceiver := NewSlowQueryLogEventReceiver(logRecorder, time.Second, "query_")
		dbSess := dbConn.NewSession(slowQueryEventReceiver)
		countUsersByName(t, dbSess, "query_count_users_by_name", "Bob", 1)
		require.Equal(t, 0, len(logRecorder.Entries()))
	})

	t.Run("slow query with wrong annotation is not logged", func(t *testing.T) {
		logRecorder := logtest.NewRecorder()
		slowQueryEventReceiver := NewSlowQueryLogEventReceiver(logRecorder, time.Second, "prom_")
		dbSess := dbConn.NewSession(slowQueryEventReceiver)
		countUsersByName(t, dbSess, "query_count_users_by_name", "Bob", 1)
		require.Equal(t, 0, len(logRecorder.Entries()))
	})

	t.Run("slow query is logged", func(t *testing.T) {
		logRecorder := logtest.NewRecorder()
		slowQueryEventReceiver := NewSlowQueryLogEventReceiver(logRecorder, 0, "query_")
		dbSess := dbConn.NewSession(slowQueryEventReceiver)

		countUsersByName(t, dbSess, "query_count_users_by_name", "Bob", 1)

		// Check that query was logged.
		require.Equal(t, 1, len(logRecorder.Entries()))
		logRecEntry := logRecorder.Entries()[0]
		require.Equal(t, "slow SQL query", logRecEntry.Text)
		logField, sqlFieldFound := logRecEntry.FindField("annotation")
		require.True(t, sqlFieldFound)
		require.Equal(t, "query_count_users_by_name", string(logField.Bytes))
	})
}

func TestDbrQueryMetricsEventReceiver_TimingKv(t *testing.T) {
	dbConn := openAndSeedDB(t)
	defer func() {
		require.NoError(t, dbConn.Close())
	}()

	t.Run("metrics for query with wrong annotation are not collected", func(t *testing.T) {
		mc := db.NewMetricsCollector()
		metricsEventReceiver := NewQueryMetricsEventReceiver(mc, "query_")
		dbSess := dbConn.NewSession(metricsEventReceiver)

		countUsersByName(t, dbSess, "count_users_by_name", "Sam", 2)

		labels := prometheus.Labels{db.MetricsLabelQuery: "count_users_by_name"}
		hist := mc.QueryDurations.With(labels).(prometheus.Histogram)
		testutil.RequireSamplesCountInHistogram(t, hist, 0)
	})

	t.Run("metrics for query are collected", func(t *testing.T) {
		mc := db.NewMetricsCollector()
		metricsEventReceiver := NewQueryMetricsEventReceiver(mc, "query_")
		dbSess := dbConn.NewSession(metricsEventReceiver)

		countUsersByName(t, dbSess, "query_count_users_by_name", "Sam", 2)

		labels := prometheus.Labels{db.MetricsLabelQuery: "query_count_users_by_name"}
		hist := mc.QueryDurations.With(labels).(prometheus.Histogram)
		testutil.RequireSamplesCountInHistogram(t, hist, 1)
	})
}

func addExclamation(s string) string {
	return "!" + s + "!"
}

func doEmpty(s string) string {
	return ""
}

func TestParseAnnotationInQuery(t *testing.T) {
	type caseData struct {
		query    string
		prefix   string
		want     string
		modifier func(string) string
	}
	cases := []caseData{
		{
			query:  "",
			prefix: "",
			want:   "",
		},
		{
			query:  "select 1",
			prefix: "",
			want:   "",
		},
		{
			query:  "/* foobar */select 1",
			prefix: "",
			want:   "foobar",
		},
		{
			query:  "/* foobar select 1",
			prefix: "",
			want:   "",
		},
		{
			query:  "/* foobar */select 1",
			prefix: "query_",
			want:   "",
		},
		{
			query:  "/* query_select1 */select 1",
			prefix: "query_",
			want:   "query_select1",
		},
		{
			query:  "/* query_count_users */\n/* just_comment */\n/*query_select_user_like_name*/select count(*) from users where name like \"p%\"",
			prefix: "query_",
			want:   "query_count_users|query_select_user_like_name",
		},
		{
			query:  "/* just_comment */\n/* query_count_users */\nselect count(*) from users where name = \"/* query_foobar */\"",
			prefix: "query_",
			want:   "query_count_users",
		},
		{
			query:    "/* just_comment */\n/* query_count_users */\nselect count(*) from users where name = \"/* query_foobar */\"",
			prefix:   "query_",
			want:     "!query_count_users!",
			modifier: addExclamation,
		},
		{
			query:    "/* just_comment */\n/* query_count_users */\nselect count(*) from users where name = \"/* query_foobar */\"",
			prefix:   "query_",
			want:     "",
			modifier: doEmpty,
		},
	}
	for _, c := range cases {
		got := ParseAnnotationInQuery(c.query, c.prefix, c.modifier)
		assert.Equal(t, c.want, got)
	}
}
