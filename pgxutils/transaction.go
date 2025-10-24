package pgxutils

import (
	"context"

	"github.com/jackc/pgx/v5"
)

func DoInPgxTx(ctx context.Context, fn func(tx pgx.Tx) error) error {
	return DoInTx(ctx, func(tx pgx.Tx) pgx.Tx { return tx }, fn)
}

func DoInRepoTx[T interface{ WithTx(tx pgx.Tx) T }](ctx context.Context, repo T, fn func(T) error) error {
	return DoInTx(ctx, repo.WithTx, fn)
}

func DoInTx[T any](ctx context.Context, generator func(tx pgx.Tx) T, fn func(T) error) error {
	provisioner, err := GetContextPoolProvisioner(ctx)
	if err != nil {
		return err
	}

	beginTx, err := provisioner.Pool.BeginTx(ctx, provisioner.ToTxOptions())
	if err != nil {
		return err
	}

	defer func() {
		if p := recover(); p != nil {
			_ = beginTx.Rollback(ctx)
			panic(p)
		} else {
			_ = beginTx.Rollback(ctx)
		}
	}()

	if err := fn(generator(beginTx)); err != nil {
		return err
	}

	if err := beginTx.Commit(ctx); err != nil {
		return err
	}

	return nil
}
