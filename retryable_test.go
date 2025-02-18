/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package dbkit

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/acronis/go-appkit/retry"
	"github.com/stretchr/testify/assert"
)

func TestMultipleIsRetryError(t *testing.T) {
	retryPolicy := retry.NewExponentialBackoffPolicy(time.Millisecond*50, 10)

	UnregisterAllIsRetryableFuncs(nil)

	// test multiple IsRetryable functions
	called := ""
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
	_ = retry.DoWithRetry(context.Background(), retryPolicy, GetIsRetryable(nil), nil, func(ctx context.Context) error {
		return fmt.Errorf("fake error")
	})
	assert.Equal(t, "123", called, "Wrong call order")

	// unregister all functions and test that no one is called
	UnregisterAllIsRetryableFuncs(nil)
	called = ""
	_ = retry.DoWithRetry(context.Background(), retryPolicy, GetIsRetryable(nil), nil, func(ctx context.Context) error {
		return fmt.Errorf("fake error")
	})
	assert.Equal(t, "", called)
}
