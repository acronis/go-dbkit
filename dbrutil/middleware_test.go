/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package dbrutil

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/acronis/go-appkit/retry"
	"github.com/gocraft/dbr/v2"
	"github.com/stretchr/testify/require"
)

// the simplest mock for http.Handler
type middlewareMock struct{}

func (mock *middlewareMock) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
}

// Test that middleware uses external factory function for creating DB sessions.
func TestTxRunnerMiddlewareUsesSessionFactory(t *testing.T) {
	dbConn := openAndSeedDB(t)
	defer func() {
		require.NoError(t, dbConn.Close())
	}()

	passed := false
	sut := func(conn *dbr.Connection, opts *sql.TxOptions, log dbr.EventReceiver) TxRunner {
		require.NotNil(t, conn)
		require.NotNil(t, opts)
		require.NotNil(t, log)
		require.False(t, passed)
		passed = true

		return NewRetryableTxRunner(conn, opts, log, retry.NewExponentialBackoffPolicy(10*time.Millisecond, 3))
	}

	next := &middlewareMock{}
	opts := TxRunnerMiddlewareOpts{NewTxRunner: sut}
	middleware := TxRunnerMiddlewareWithOpts(dbConn, sql.LevelDefault, opts)(next)
	require.NotNil(t, middleware)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "/", nil)
	require.NoError(t, err)
	resp := httptest.NewRecorder()
	defer require.NoError(t, resp.Result().Body.Close())

	middleware.ServeHTTP(resp, req)
	require.True(t, passed, "Implementation of middleware.ServeHTTP must use opts.NewSession!")
}
