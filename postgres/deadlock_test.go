/*
Copyright © 2019-2023 Acronis International GmbH.
*/

package postgres

import (
	testing2 "github.com/acronis/go-dbkit/internal/testing"
	"testing"

	_ "github.com/lib/pq"

	"github.com/acronis/go-dbkit"
)

func TestDeadlockErrorHandling(t *testing.T) {
	testing2.DeadlockTest(t, db.DialectPostgres,
		func(err error) bool {
			return CheckPostgresError(err, db.PostgresErrCodeDeadlockDetected)
		})
}
