package pgxutils

import (
	"context"
	"database/sql"

	"github.com/acronis/go-dbkit"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PoolProvisioner struct {
	Pool       *pgxpool.Pool
	Isolation  pgx.TxIsoLevel
	AccessMode pgx.TxAccessMode
}

func (pp *PoolProvisioner) ToTxOptions() pgx.TxOptions {
	return pgx.TxOptions{
		AccessMode: pp.AccessMode,
		IsoLevel:   pp.Isolation,
	}
}

func IsolationLevelToPgx(isolation sql.IsolationLevel) pgx.TxIsoLevel {
	switch isolation {
	case sql.LevelReadUncommitted:
		return pgx.ReadUncommitted
	case sql.LevelReadCommitted:
		return pgx.ReadCommitted
	case sql.LevelRepeatableRead:
		return pgx.RepeatableRead
	case sql.LevelSerializable:
		return pgx.Serializable
	default:
		// Default to read committed if unknown
		return pgx.ReadCommitted
	}
}

func NewPoolProvisioner(ctx context.Context, cfg *dbkit.Config, accessMode pgx.TxAccessMode) (*PoolProvisioner, error) {
	_, dsn := cfg.DriverNameAndDSN()
	parseConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}

	pool, err := pgxpool.NewWithConfig(ctx, parseConfig)
	if err != nil {
		return nil, err
	}

	return &PoolProvisioner{
		Pool:       pool,
		Isolation:  IsolationLevelToPgx(cfg.TxIsolationLevel()),
		AccessMode: accessMode,
	}, nil
}
