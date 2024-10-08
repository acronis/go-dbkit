/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package dbrutil

import (
	"github.com/gocraft/dbr/v2"
)

// CompositeEventReceiver represents a composition of event receivers from dbr package and implements Composite design pattern.
type CompositeEventReceiver struct {
	Receivers []dbr.EventReceiver
}

// NewCompositeReceiver creates a new CompositeEventReceiver.
func NewCompositeReceiver(receivers []dbr.EventReceiver) *CompositeEventReceiver {
	return &CompositeEventReceiver{receivers}
}

// Event receives a simple notification when various events occur.
func (r *CompositeEventReceiver) Event(eventName string) {
	for _, recv := range r.Receivers {
		recv.Event(eventName)
	}
}

// EventKv receives a notification when various events occur along with
// optional key/value data.
func (r *CompositeEventReceiver) EventKv(eventName string, kvs map[string]string) {
	for _, recv := range r.Receivers {
		recv.EventKv(eventName, kvs)
	}
}

// EventErr receives a notification of an error if one occurs.
func (r *CompositeEventReceiver) EventErr(eventName string, err error) error {
	for _, recv := range r.Receivers {
		_ = recv.EventErr(eventName, err)
	}
	return err
}

// EventErrKv receives a notification of an error if one occurs along with
// optional key/value data.
func (r *CompositeEventReceiver) EventErrKv(eventName string, err error, kvs map[string]string) error {
	for _, recv := range r.Receivers {
		_ = recv.EventErrKv(eventName, err, kvs)
	}
	return err
}

// Timing receives the time an event took to happen.
func (r *CompositeEventReceiver) Timing(eventName string, nanoseconds int64) {
	for _, recv := range r.Receivers {
		recv.Timing(eventName, nanoseconds)
	}
}

// TimingKv is called when SQL query is executed. It receives the duration of how long the query takes,
// and calls TimingKv for each receiver in composition.
func (r *CompositeEventReceiver) TimingKv(eventName string, nanoseconds int64, kvs map[string]string) {
	for _, recv := range r.Receivers {
		recv.TimingKv(eventName, nanoseconds, kvs)
	}
}
