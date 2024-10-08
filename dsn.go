/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package dbkit

import (
	"fmt"

	"net/url"

	"github.com/go-sql-driver/mysql"
)

// MakeMSSQLDSN makes DSN for opening MSSQL database.
func MakeMSSQLDSN(cfg *MSSQLConfig) string {
	query := url.Values{}
	query.Add("database", cfg.Database)

	u := &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(cfg.User, cfg.Password),
		Host:     fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		RawQuery: query.Encode(),
	}

	return u.String()
}

// MakeMySQLDSN makes DSN for opening MySQL database.
func MakeMySQLDSN(cfg *MySQLConfig) string {
	c := mysql.NewConfig()
	c.Net = "tcp"
	c.Addr = fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	c.User = cfg.User
	c.Passwd = cfg.Password
	c.DBName = cfg.Database
	c.ParseTime = true
	c.MultiStatements = true
	c.Params = make(map[string]string)
	c.Params["autocommit"] = "false"
	return c.FormatDSN()
}

// MakePostgresDSN makes DSN for opening Postgres database.
func MakePostgresDSN(cfg *PostgresConfig) string {
	sslMode := cfg.SSLMode
	if sslMode == "" {
		sslMode = PostgresDefaultSSLMode
	}
	connURI := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(cfg.User, cfg.Password),
		Host:     fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Path:     cfg.Database,
		RawQuery: fmt.Sprintf("sslmode=%s", url.QueryEscape(string(sslMode))),
	}
	if cfg.SearchPath != "" {
		connURI.RawQuery += fmt.Sprintf("&search_path=%s", url.QueryEscape(cfg.SearchPath))
	}
	if len(cfg.AdditionalParameters) != 0 {
		for _, p := range cfg.AdditionalParameters {
			connURI.RawQuery += fmt.Sprintf("&%s=%s", p.Name, url.QueryEscape(p.Value))
		}
	}

	return connURI.String()
}

// MakeSQLiteDSN makes DSN for opening SQLite database.
func MakeSQLiteDSN(cfg *SQLiteConfig) string {
	// Connection params will be used here in the future.
	return cfg.Path
}
