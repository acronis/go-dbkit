/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package distrlock

import (
	"errors"
)

// Distributed lock errors.
var (
	ErrLockAlreadyAcquired = errors.New("distributed lock already acquired")
	ErrLockAlreadyReleased = errors.New("distributed lock already released")
)
