/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package dbrutil

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/jackc/pgconn"
	pg "github.com/jackc/pgx/v4/stdlib"
	"github.com/stretchr/testify/require"

	"github.com/acronis/go-dbkit"
	_ "github.com/acronis/go-dbkit/pgx"
)

// Test that retriable errors stays retriable even wrapped in Tx structures
func TestTxErrorsIsRetriable(t *testing.T) {
	retriable := []db.PostgresErrCode{
		db.PgxErrCodeDeadlockDetected,
		db.PgxErrCodeSerializationFailure,
	}

	mkerr := func(code string) []error {
		return []error{
			&TxCommitError{Inner: &pgconn.PgError{Code: code}},
			&TxRollbackError{Inner: &pgconn.PgError{Code: code}},
			&TxBeginError{Inner: &pgconn.PgError{Code: code}},
		}
	}

	check := db.GetIsRetryable(&pg.Driver{})

	for _, c := range retriable {
		for _, err := range mkerr(string(c)) {
			require.True(t, check(err), fmt.Sprintf("Failed on %v", reflect.TypeOf(err)))
		}
	}
}
