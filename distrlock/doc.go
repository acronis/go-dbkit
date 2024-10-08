/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

// Package distrlock contains DML (distributed lock manager) implementation (now DMLs based on MySQL and PostgreSQL are supported).
// Now only manager that uses SQL database (PostgreSQL and MySQL are currently supported) is available.
// Other implementations (for example, based on Redis) will probably be implemented in the future.
package distrlock
