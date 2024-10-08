/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package goquutil

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"time"
)

var nullTimeStringFormats = []string{
	"2006-01-02 15:04:05.99999999999999999Z07:00",
	"2006-01-02 15:04:05.99999999999999999",
	time.RFC3339,
	time.RFC3339Nano,
}

// NullTime is suitable in case of different functions working with time
// Note! It's suitable for in case you use goqu.MAX(date_column) on SQLite. It's not
// required on MySQL. MySQL can work with sql.NullTime
// sql.NullTime is not suitable in such case because on SQLite driver cannot detect
// type of MAX(date_column) expression as timestamp and handle it as a text, the problem can be
// in function (rc *SQLiteRows) declTypes() []string at github.com/mattn/go-sqlite3/sqlite3.go
type NullTime struct {
	Valid bool
	Time  time.Time
}

// NullTimeFrom creates valid NullTime from time.Time
func NullTimeFrom(t time.Time) NullTime {
	return NullTime{Time: t, Valid: true}
}

// Scan implements the Scanner interface.
func (ns *NullTime) Scan(value interface{}) error {
	var t sql.NullTime
	err := t.Scan(value)
	if err == nil {
		ns.Time, ns.Valid = t.Time, t.Valid
		return nil
	}

	var s sql.NullString
	err = s.Scan(value)
	if err != nil {
		ns.Time, ns.Valid = time.Time{}, false
		return err
	}
	parsedTime, err := ns.parseAsTimeString(s.String)
	if err != nil {
		ns.Time, ns.Valid = time.Time{}, false
		return err
	}
	ns.Time, ns.Valid = parsedTime, true
	return nil
}

func (ns *NullTime) parseAsTimeString(s string) (time.Time, error) {
	for idx := range nullTimeStringFormats {
		t, err := time.Parse(nullTimeStringFormats[idx], s)
		if err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse string '%s' as time", s)
}

// Value implements the driver Valuer interface.
func (ns NullTime) Value() (driver.Value, error) {
	if !ns.Valid {
		return nil, nil
	}
	return ns.Time, nil
}

// SetValid sets NullTime valid
func (ns *NullTime) SetValid(t time.Time) {
	ns.Time, ns.Valid = t, true
}

// SetInvalid sets NullTime invalid
func (ns *NullTime) SetInvalid() {
	ns.Time, ns.Valid = time.Time{}, false
}
