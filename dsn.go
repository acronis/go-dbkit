/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package dbkit

import (
	"fmt"
	"sort"
	"strings"

	"net/url"

	"github.com/go-sql-driver/mysql"
)

// MakeMSSQLDSN makes DSN for opening MSSQL database.
func MakeMSSQLDSN(cfg *MSSQLConfig) string {
	query := url.Values{}
	const dbKeyConfig = "database"
	query.Add(dbKeyConfig, cfg.Database)

	u := url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(cfg.User, cfg.Password),
		Host:     fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		RawQuery: query.Encode(),
	}
	if len(cfg.AdditionalParameters) == 0 {
		return u.String()
	}

	return urlWithOptionalParameters(u, cfg.AdditionalParameters,
		map[string]struct{}{
			dbKeyConfig: {},
		})
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
		Scheme:   string(DialectPostgres),
		User:     url.UserPassword(cfg.User, cfg.Password),
		Host:     fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Path:     cfg.Database,
		RawQuery: fmt.Sprintf("sslmode=%s", url.QueryEscape(string(sslMode))),
	}
	if cfg.SearchPath != "" {
		connURI.RawQuery += fmt.Sprintf("&search_path=%s", url.QueryEscape(cfg.SearchPath))
	}
	if len(cfg.AdditionalParameters) == 0 {
		return connURI.String()
	}

	ignore := map[string]struct{}{
		"sslmode": {},
	}
	if cfg.SearchPath != "" {
		ignore["search_path"] = struct{}{}
	}

	return urlWithOptionalParameters(connURI, cfg.AdditionalParameters,
		ignore)
}

// MakeSQLiteDSN makes DSN for opening SQLite database.
func MakeSQLiteDSN(cfg *SQLiteConfig) string {
	// Connection params will be used here in the future.
	return cfg.Path
}

func urlWithOptionalParameters(
	u url.URL,
	params map[string]string,
	keysToIgnore map[string]struct{},
) string {
	queryParts := make([]string, 0, len(params))
	for k, v := range params {
		if _, ok := keysToIgnore[k]; ok {
			continue
		}
		queryParts = append(queryParts, fmt.Sprintf("%s=%s", k, url.QueryEscape(v)))
	}
	sort.Strings(queryParts) // Sort to make DSN deterministic.
	u.RawQuery += "&" + strings.Join(queryParts, "&")
	return u.String()
}
