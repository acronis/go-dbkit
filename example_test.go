package dbkit_test

import (
	"context"
	"database/sql"
	"log"
	"os"
	"time"

	"github.com/acronis/go-appkit/retry"

	"github.com/acronis/go-dbkit"

	// Import the `mysql` package for registering the retryable function for MySQL transient
	// errors (like deadlocks).
	_ "github.com/acronis/go-dbkit/mysql"
)

func Example() {
	// Configure the database using the dbkit.Config struct.
	// In this example, we're using MySQL. Adjust Dialect and config fields for your target DB.
	cfg := &dbkit.Config{
		Dialect: dbkit.DialectMySQL,
		MySQL: dbkit.MySQLConfig{
			Host:     os.Getenv("MYSQL_HOST"),
			Port:     3306,
			User:     os.Getenv("MYSQL_USER"),
			Password: os.Getenv("MYSQL_PASSWORD"),
			Database: os.Getenv("MYSQL_DATABASE"),
		},
		MaxOpenConns: 16,
		MaxIdleConns: 8,
	}

	// Open the database connection.
	// The 2nd parameter is a boolean that indicates whether to ping the database.
	db, err := dbkit.Open(cfg, true)
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Execute a transaction with a custom retry policy (exponential backoff with 3 retries,
	// starting from 10ms).
	retryPolicy := retry.NewConstantBackoffPolicy(10*time.Millisecond, 3)
	if err = dbkit.DoInTx(context.Background(), db, func(tx *sql.Tx) error {
		// Execute your transactional operations here.
		// Example: _, err := tx.Exec("UPDATE users SET last_login = ? WHERE id = ?",
		// time.Now(), 1)
		return nil
	}, dbkit.WithRetryPolicy(retryPolicy)); err != nil {
		log.Fatal(err)
	}

	// Output:
}
