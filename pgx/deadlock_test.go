/*
Copyright © 2019-2023 Acronis International GmbH.
*/

package pgx

import (
	gotesting "testing"

	_ "github.com/jackc/pgx/v4/stdlib"

	"github.com/acronis/go-dbkit"
	"github.com/acronis/go-dbkit/internal/testing"
)

func TestDeadlockErrorHandling(t *gotesting.T) {
	testing.DeadlockTest(t, db.DialectPgx,
		func(err error) bool {
			return CheckPostgresError(err, db.PgxErrCodeDeadlockDetected)
		})
}
