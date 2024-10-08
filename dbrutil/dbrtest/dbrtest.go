/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package dbrtest

import (
	"context"

	"github.com/gocraft/dbr/v2"

	"github.com/acronis/go-dbkit/dbrutil"
)

// MockTxRunner is a mock that implements dbrutils.TxRunner interface.
type MockTxRunner struct {
	Err           error
	Tx            *dbr.Tx
	BeginTxCalled int
	DoInTxCalled  int
}

var _ dbrutil.TxRunner = (*MockTxRunner)(nil)

// BeginTx returns error or dbr.Tx object.
func (m *MockTxRunner) BeginTx(ctx context.Context) (*dbr.Tx, error) {
	m.BeginTxCalled++
	if m.Err != nil {
		return nil, m.Err
	}
	return m.Tx, nil
}

// DoInTx returns error or calls passed callback.
func (m *MockTxRunner) DoInTx(ctx context.Context, fn func(dbRunner dbr.SessionRunner) error) error {
	m.DoInTxCalled++
	if m.Err != nil {
		return m.Err
	}
	return fn(m.Tx)
}
