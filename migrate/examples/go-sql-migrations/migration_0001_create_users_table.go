/*
Copyright © 2025 Acronis International GmbH.

Released under MIT license.
*/

package main

import (
	"github.com/acronis/go-dbkit"
	"github.com/acronis/go-dbkit/migrate"
)

const migration0001CreateUsersTableUpMySQL = `
CREATE TABLE users (
    id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL
);
`

const migration0001CreateUsersTableUpPostgres = `
CREATE TABLE users (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);
`

const migration0001CreateUsersTableDown = `
DROP TABLE users;
`

type Migration0001CreateUsersTable struct {
	*migrate.NullMigration
}

func NewMigration0001CreateUsersTable(dialect dbkit.Dialect) *Migration0001CreateUsersTable {
	return &Migration0001CreateUsersTable{&migrate.NullMigration{Dialect: dialect}}
}

func (m *Migration0001CreateUsersTable) ID() string {
	return "0001_create_users_table"
}

func (m *Migration0001CreateUsersTable) UpSQL() []string {
	switch m.Dialect {
	case dbkit.DialectMySQL:
		return []string{migration0001CreateUsersTableUpMySQL}
	case dbkit.DialectPgx, dbkit.DialectPostgres:
		return []string{migration0001CreateUsersTableUpPostgres}
	}
	return nil
}

func (m *Migration0001CreateUsersTable) DownSQL() []string {
	switch m.Dialect {
	case dbkit.DialectMySQL, dbkit.DialectPgx, dbkit.DialectPostgres:
		return []string{migration0001CreateUsersTableDown}
	}
	return nil
}
