/*
Copyright © 2025-2026 Acronis International GmbH.

Released under MIT license.
*/

package main

import (
	"context"
	"database/sql"
	"fmt"
	stdlog "log"
	"os"
	"time"

	"github.com/acronis/go-appkit/log"
	"github.com/gocraft/dbr/v2"

	"github.com/acronis/go-dbkit"
	"github.com/acronis/go-dbkit/dbrutil"
)

func main() {
	logger, loggerClose := log.NewLogger(&log.Config{
		Output: log.OutputStderr,
		Level:  log.LevelInfo,
	})
	defer loggerClose()

	// Create a Prometheus metrics collector.
	promMetrics := dbkit.NewPrometheusMetrics()
	promMetrics.MustRegister()
	defer promMetrics.Unregister()

	// Open the database connection with instrumentation.
	// Instrumentation includes collecting metrics about SQL queries and logging slow queries.
	eventReceiver := dbrutil.NewCompositeReceiver([]dbr.EventReceiver{
		dbrutil.NewQueryMetricsEventReceiver(promMetrics, queryAnnotationPrefix),
		dbrutil.NewSlowQueryLogEventReceiver(logger, 100*time.Millisecond, queryAnnotationPrefix),
	})
	conn, err := openDB(eventReceiver)
	if err != nil {
		stdlog.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	txRunner := dbrutil.NewTxRunner(conn, &sql.TxOptions{Isolation: sql.LevelReadCommitted}, nil)

	// Execute function in a transaction.
	// The transaction will be automatically committed if the function returns nil, otherwise it
	// will be rolled back.
	if dbErr := txRunner.DoInTx(context.Background(), func(tx dbr.SessionRunner) error {
		var result int
		return tx.Select("SLEEP(1)").
			// Annotate the query for Prometheus metrics and slow query log.
			Comment(annotateQuery("long_operation")).
			LoadOne(&result)
	}); dbErr != nil {
		stdlog.Fatal(dbErr)
	}

	// The following log message will be printed:
	// {"level":"warn","time":"2025-02-14T16:29:55.429257+02:00","msg":"slow SQL query",
	// "pid":14030,"annotation":"query:long_operation","duration_ms":1007}

	// Prometheus metrics will be collected:
	// db_query_duration_seconds_bucket{query="query:long_operation",le="2.5"} 1
	// db_query_duration_seconds_sum{query="query:long_operation"} 1.004573875
	// db_query_duration_seconds_count{query="query:long_operation"} 1
}

const queryAnnotationPrefix = "query:"

func annotateQuery(queryName string) string {
	return queryAnnotationPrefix + queryName
}

func openDB(eventReceiver dbr.EventReceiver) (*dbr.Connection, error) {
	cfg := &dbkit.Config{
		Dialect: dbkit.DialectMySQL,
		MySQL: dbkit.MySQLConfig{
			Host:     os.Getenv("MYSQL_HOST"),
			Port:     3306,
			User:     os.Getenv("MYSQL_USER"),
			Password: os.Getenv("MYSQL_PASSWORD"),
			Database: os.Getenv("MYSQL_DATABASE"),
		},
	}

	// Open database with instrumentation based on the provided event receiver
	// (see github.com/gocraft/dbr doc for details).
	// Opening includes configuring the max open/idle connections and their lifetime and
	// pinging the database.
	conn, err := dbrutil.Open(cfg, true, eventReceiver)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	return conn, nil
}
