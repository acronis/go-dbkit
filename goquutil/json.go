/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package goquutil

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
)

// JSONEncoder is convenience function for writing JSON values to db
func JSONEncoder(i interface{}) driver.Valuer {
	return jsonEncoder{i}
}

// JSONDecoder is convenience function for reading JSON values from db
func JSONDecoder(i interface{}) sql.Scanner {
	return jsonDecoder{i}
}

type jsonEncoder struct {
	i interface{}
}

func (j jsonEncoder) Value() (driver.Value, error) {
	b, err := json.Marshal(j.i)
	if err != nil {
		return nil, fmt.Errorf("marshal: %w", err)
	}
	return b, nil
}

type jsonDecoder struct {
	i interface{}
}

func (j jsonDecoder) Scan(dest interface{}) error {
	if dest == nil {
		return errors.New("nil value")
	}
	var b []byte
	switch s := dest.(type) {
	case string:
		b = []byte(s)
	case []byte:
		b = s
	default:
		return fmt.Errorf("expected '[]byte' or 'string' got %T", dest)
	}
	if err := json.Unmarshal(b, &j.i); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}
	return nil
}
