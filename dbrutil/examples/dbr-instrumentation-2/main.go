/*
Copyright © 2025 Acronis International GmbH.

Released under MIT license.
*/
package main

import (
	"database/sql"
	"errors"
	"fmt"
	stdlog "log"
	"net/http"
	"os"
	"time"

	"github.com/acronis/go-appkit/httpserver/middleware"
	"github.com/acronis/go-appkit/log"
	"github.com/gocraft/dbr/v2"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/acronis/go-dbkit"
	"github.com/acronis/go-dbkit/dbrutil"
)

func main() {
	logger, loggerClose := log.NewLogger(&log.Config{Output: log.OutputStdout, Level: log.LevelInfo})
	defer loggerClose()

	// Create a Prometheus metrics collector.
	promMetrics := dbkit.NewPrometheusMetrics()
	promMetrics.MustRegister()
	defer promMetrics.Unregister()

	// Open the database connection with instrumentation.
	// Instrumentation includes collecting metrics about SQL queries.
	conn, err := openDB(dbrutil.NewQueryMetricsEventReceiver(promMetrics, queryAnnotationPrefix))
	if err != nil {
		stdlog.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	// Construct the middleware that will put the transaction runner into the request context.
	var txRunnerOpts dbrutil.TxRunnerMiddlewareOpts
	txRunnerOpts.SlowQueryLog.MinTime = 100 * time.Millisecond // Log queries that take more than 100ms.
	txRunnerOpts.SlowQueryLog.AnnotationPrefix = queryAnnotationPrefix
	txRunnerMiddleware := dbrutil.TxRunnerMiddlewareWithOpts(conn, sql.LevelReadCommitted, txRunnerOpts)

	// Create a handler that will execute a long operation in a transaction.
	handler := http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		txRunner := dbrutil.GetTxRunnerFromContext(r.Context()) // Get the request-scoped transaction runner.

		// Execute function in a transaction.
		// The transaction will be automatically committed if the function returns nil, otherwise it will be rolled back.
		if dbErr := txRunner.DoInTx(r.Context(), func(tx dbr.SessionRunner) error {
			var result int
			return tx.Select("SLEEP(1)").
				Comment(annotateQuery("long_operation")). // Annotate the query for Prometheus metrics and slow query log.
				LoadOne(&result)
		}); dbErr != nil {
			http.Error(rw, "Internal Server Error", http.StatusInternalServerError)
			return
		}
		_, _ = rw.Write([]byte("OK"))
	})

	// Construct the middleware chain and start the server.
	middlewares := []func(http.Handler) http.Handler{
		middleware.RequestID(),
		middleware.Logging(logger),
		txRunnerMiddleware,
	}
	var h http.Handler = handler
	for i := len(middlewares) - 1; i >= 0; i-- {
		h = middlewares[i](h)
	}
	mux := http.NewServeMux()
	mux.Handle("/long-operation", h)
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{
		Addr:         ":8080",
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}
	if srvErr := srv.ListenAndServe(); srvErr != nil && errors.Is(srvErr, http.ErrServerClosed) {
		stdlog.Fatalf("failed to start server: %v", srvErr)
	}
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

	// Open database with instrumentation based on the provided event receiver (see github.com/gocraft/dbr doc for details).
	// Opening includes configuring the max open/idle connections and their lifetime and pinging the database.
	conn, err := dbrutil.Open(cfg, true, eventReceiver)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	return conn, nil
}
