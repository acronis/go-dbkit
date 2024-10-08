/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package db

import (
	"database/sql/driver"
	"errors"
	"reflect"

	"github.com/acronis/go-appkit/retry"
)

var retryableErrors = map[reflect.Type]retry.IsRetryable{}

// GetIsRetryable returns a function that can tell for given driver if error is retryable.
func GetIsRetryable(d driver.Driver) retry.IsRetryable {
	t := reflect.TypeOf(d)
	if r, ok := retryableErrors[t]; ok {
		return r
	}
	return isRetryableNoDriver
}

func isRetryableNoDriver(error) bool {
	return false
}

// RegisterIsRetryableFunc registers callback to determinate specific DB error is retryable or not.
// Several registered functions will be called one after another in FIFO order before some function returns true.
// Note: this function is not concurrent-safe. Typical scenario: register all custom IsRetryable in module init()
func RegisterIsRetryableFunc(d driver.Driver, retryable retry.IsRetryable) {
	t := reflect.TypeOf(d)
	prev, ok := retryableErrors[t]
	retryableErrors[t] = func(e error) bool {
		for ; e != nil; e = errors.Unwrap(e) {
			if ok && prev(e) {
				return true
			}
			if retryable(e) {
				return true
			}
		}
		return false
	}
}
