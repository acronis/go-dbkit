/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package db

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/acronis/go-appkit/retry"
	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/assert"
)

func TestMultipleIsRetryError(t *testing.T) {
	var called string

	// cleanup handlers
	oldHandlers := retryableErrors
	retryableErrors = map[reflect.Type]retry.IsRetryable{}
	defer func() {
		retryableErrors = oldHandlers
	}()

	RegisterIsRetryableFunc(nil, func(e error) bool {
		called += "1"
		return false
	})
	RegisterIsRetryableFunc(nil, func(e error) bool {
		called += "2"
		return false
	})
	RegisterIsRetryableFunc(nil, func(e error) bool {
		called += "3"
		return false
	})

	p := retry.NewExponentialBackoffPolicy(backoff.DefaultInitialInterval, 10)
	_ = retry.DoWithRetry(context.Background(), p, GetIsRetryable(nil), nil, func(ctx context.Context) error {
		return fmt.Errorf("fake error")
	})

	assert.Equal(t, "123", called, "Wrong call order")
}
