# dbrutil

[![GoDoc Widget]][GoDoc]

`dbrutil` is a Go package that provides utilities and helpers for working with the [dbr query builder](https://github.com/gocraft/dbr).
It simplifies database operations by offering:
- **Database Connection Management**: Open a database connection with instrumentation for collecting metrics and logging slow queries.
- **Transaction Management**: Run functions within transactions using a unified `TxRunner` interface that automatically commits or rolls back.
- **Retryable Transactions**: Execute transactions with configurable retry policies.
- **Prometheus Metrics Collection**: Collect and observe SQL query durations via SQL comment annotations.
- **Slow Query Logging**: Log SQL queries that exceed a configurable duration threshold.

## Usage

The following basic example demonstrates how to use `dbrutil` to open a database connection with instrumentation,
and execute queries with an automatic slow query logging and Prometheus metrics collection within transaction.

```go
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
	defer conn.Close()

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
	// "pid":14030, "annotation":"query:long_operation","duration_ms":1007}

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
```

The next example demonstrates how to use `dbrutil` to create a middleware that injects a transaction runner into the request context.
This transaction runner will use request-scoped logger for logging slow queries.

```go
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
	defer conn.Close()

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
	if srvErr := srv.ListenAndServe(); srvErr != nil && !errors.Is(srvErr, http.ErrServerClosed) {
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
```

If you run the server and send a request to the `/long-operation` endpoint, you will see the following log message:

```shell
{"level":"warn","time":"2025-02-16T19:06:39.029356+02:00","msg":"slow SQL query","pid":14117,"request_id":"cup1m7gc65t3e995a1i0","int_request_id":"cup1m7gc65t3e995a1ig","trace_id":"","annotation":"query:long_operation","duration_ms":1002}
{"level":"info","time":"2025-02-16T19:06:39.03459+02:00","msg":"response completed in 1.012s","pid":14117,"request_id":"cup1m7gc65t3e995a1i0","int_request_id":"cup1m7gc65t3e995a1ig","trace_id":"","method":"GET","uri":"/long-operation","remote_addr":"[::1]:54849","content_length":0,"user_agent":"curl/8.7.1","remote_addr_ip":"::1","remote_addr_port":54849,"duration_ms":1011,"duration":1011996,"status":200,"bytes_sent":2,"time_slots":{"writing_response_ms":0}}
```

As you can see, the slow query log message contains the annotation `query:long_operation`, and this log message is associated with the request ID `cup1m7gc65t3e995a1i0`.
Using the request ID, you can correlate the slow query log message with the request log message.

Additionally, the Prometheus metrics collector will collect the following metrics:

```shell
db_query_duration_seconds_bucket{query="query:long_operation",le="0.001"} 0
...
db_query_duration_seconds_bucket{query="query:long_operation",le="2.5"} 1
...
db_query_duration_seconds_bucket{query="query:long_operation",le="+Inf"} 1
db_query_duration_seconds_sum{query="query:long_operation"} 1.00294175
db_query_duration_seconds_count{query="query:long_operation"} 1
```

## License

Copyright © 2025-2026 Acronis International GmbH.

Licensed under [MIT License](./../LICENSE).

[GoDoc]: https://pkg.go.dev/github.com/acronis/go-dbkit/dbrutil
[GoDoc Widget]: https://godoc.org/github.com/acronis/go-dbkit/dbrutil?status.svg