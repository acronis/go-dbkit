/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package db

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestDoInTx(t *testing.T) {
	tests := []struct {
		Name         string
		InitMock     func(m sqlmock.Sqlmock)
		Fn           func(tx *sql.Tx) error
		WantErr      error
		WantPanicErr error
	}{
		{
			Name: "success",
			InitMock: func(m sqlmock.Sqlmock) {
				m.ExpectBegin()
				m.ExpectCommit()
			},
			Fn: func(tx *sql.Tx) error {
				return nil
			},
		},
		{
			Name: "error on begin",
			InitMock: func(m sqlmock.Sqlmock) {
				m.ExpectBegin().WillReturnError(fmt.Errorf("begin error"))
			},
			Fn: func(tx *sql.Tx) error {
				return nil
			},
			WantErr: fmt.Errorf("begin tx: begin error"),
		},
		{
			Name: "error on commit",
			InitMock: func(m sqlmock.Sqlmock) {
				m.ExpectBegin()
				m.ExpectCommit().WillReturnError(fmt.Errorf("commit error"))
			},
			Fn: func(tx *sql.Tx) error {
				return nil
			},
			WantErr: fmt.Errorf("commit tx: commit error"),
		},
		{
			Name: "error in func",
			InitMock: func(m sqlmock.Sqlmock) {
				m.ExpectBegin()
				m.ExpectRollback()
			},
			Fn: func(tx *sql.Tx) error {
				return fmt.Errorf("fn error")
			},
			WantErr: fmt.Errorf("fn error"),
		},
		{
			Name: "panic in func",
			InitMock: func(m sqlmock.Sqlmock) {
				m.ExpectBegin()
				m.ExpectRollback()
			},
			Fn: func(tx *sql.Tx) error {
				panic(fmt.Errorf("panic"))
			},
			WantPanicErr: fmt.Errorf("panic"),
		},
	}
	for i := range tests {
		tt := tests[i]
		t.Run(tt.Name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer func() {
				requireNoErrOnClose(t, db)
				require.NoError(t, mock.ExpectationsWereMet())
			}()

			tt.InitMock(mock)
			mock.ExpectClose()

			if tt.WantPanicErr != nil {
				require.PanicsWithError(t, tt.WantPanicErr.Error(), func() {
					_ = DoInTx(context.Background(), db, tt.Fn)
				})
				return
			}
			err = DoInTx(context.Background(), db, tt.Fn)
			if tt.WantErr == nil {
				require.NoError(t, err)
				return
			}
			require.EqualError(t, err, tt.WantErr.Error())
		})
	}
}

func requireNoErrOnClose(t *testing.T, closer io.Closer) {
	t.Helper()
	require.NoError(t, closer.Close())
}
