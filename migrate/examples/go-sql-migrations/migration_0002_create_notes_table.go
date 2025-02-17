/*
Copyright © 2025 Acronis International GmbH.

Released under MIT license.
*/

package main

import (
	"github.com/acronis/go-dbkit"
	"github.com/acronis/go-dbkit/migrate"
)

const migration0002CreateNotesTableUpMySQL = `
CREATE TABLE notes (
    id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    content TEXT,
    user_id BIGINT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);
`

const migration0002CreateNotesTableUpPostgres = `
CREATE TABLE notes (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    content TEXT,
    user_id BIGINT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);
`

const migration0002CreateNotesTableDown = `
DROP TABLE notes;
`

func NewMigration0002CreateNotesTable(dialect dbkit.Dialect) *migrate.CustomMigration {
	var upSQL []string
	var downSQL []string
	switch dialect {
	case dbkit.DialectMySQL:
		upSQL = []string{migration0002CreateNotesTableUpMySQL}
		downSQL = []string{migration0002CreateNotesTableDown}
	case dbkit.DialectPgx, dbkit.DialectPostgres:
		upSQL = []string{migration0002CreateNotesTableUpPostgres}
		downSQL = []string{migration0002CreateNotesTableDown}
	}
	return migrate.NewCustomMigration("0002_create_notes_table", upSQL, downSQL, nil, nil)
}
