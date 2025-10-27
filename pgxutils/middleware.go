package pgxutils

import (
	"context"
	"errors"
	"net/http"
)

type ctxKey int

const ctxKeyPool ctxKey = iota

func PgxPoolMiddleware(provisioner *PoolProvisioner) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return &pgxPoolMiddleware{
			next:            next,
			PoolProvisioner: provisioner,
		}
	}
}

type pgxPoolMiddleware struct {
	next http.Handler
	*PoolProvisioner
}

func (m *pgxPoolMiddleware) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	reqCtx := r.Context()

	m.next.ServeHTTP(rw, r.WithContext(NewContextWithPoolProvisioner(reqCtx, m.PoolProvisioner)))
}

func NewContextWithPoolProvisioner(parentCtx context.Context, pool *PoolProvisioner) context.Context {
	return context.WithValue(parentCtx, ctxKeyPool, pool)
}

var ErrNoPoolProvisionerInContext = errors.New("no pool provisioner in context")

func GetContextPoolProvisioner(ctx context.Context) (*PoolProvisioner, error) {
	pp, ok := ctx.Value(ctxKeyPool).(*PoolProvisioner)
	if !ok {
		return nil, ErrNoPoolProvisionerInContext
	}
	return pp, nil
}
