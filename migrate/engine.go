/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

var numberPrefixRegex = regexp.MustCompile(`^(\d+)`)

// plannedMigration is a migration step selected for execution.
type plannedMigration struct {
	id        string
	queries   []string
	disableTx bool
}

// migrationIDLess orders migrations: IDs with numeric prefix go first and are compared by the prefix value,
// other IDs are compared lexicographically.
func migrationIDLess(a, b string) bool {
	aNum, aIsNum := numericVersion(a)
	bNum, bIsNum := numericVersion(b)
	switch {
	case aIsNum && bIsNum && aNum != bNum:
		return aNum < bNum
	case aIsNum && !bIsNum:
		return true
	case !aIsNum && bIsNum:
		return false
	default:
		return a < b
	}
}

func numericVersion(id string) (int64, bool) {
	prefix := numberPrefixRegex.FindString(id)
	if prefix == "" {
		return 0, false
	}
	v, err := strconv.ParseInt(prefix, 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// planMigrations selects migrations to run.
// Migrations missing in the history but ordered before the last applied one are always planned first (catch-up),
// regardless of direction. Then at most limit migrations are planned in the requested direction (0 means no limit).
func planMigrations(migrations []*RawMigration, appliedIDs []string, direction MigrationsDirection, limit int) ([]plannedMigration, error) {
	sorted := make([]*RawMigration, len(migrations))
	copy(sorted, migrations)
	sort.SliceStable(sorted, func(i, j int) bool { return migrationIDLess(sorted[i].ID, sorted[j].ID) })

	known := make(map[string]struct{}, len(sorted))
	for _, m := range sorted {
		known[m.ID] = struct{}{}
	}
	applied := make(map[string]struct{}, len(appliedIDs))
	for _, id := range appliedIDs {
		if _, ok := known[id]; !ok {
			return nil, fmt.Errorf("unable to create migration plan because of %s: unknown migration in database", id)
		}
		applied[id] = struct{}{}
	}

	var lastApplied string
	if len(appliedIDs) > 0 {
		existing := make([]string, len(appliedIDs))
		copy(existing, appliedIDs)
		sort.SliceStable(existing, func(i, j int) bool { return migrationIDLess(existing[i], existing[j]) })
		lastApplied = existing[len(existing)-1]
	}

	plan := make([]plannedMigration, 0, len(sorted))
	if len(appliedIDs) > 0 {
		for _, m := range sorted {
			if _, ok := applied[m.ID]; !ok && migrationIDLess(m.ID, lastApplied) {
				plan = append(plan, plannedMigration{id: m.ID, queries: m.UpSQL, disableTx: m.DisableTransactionUp})
			}
		}
	}

	// Position of the last applied migration.
	idx := -1
	if lastApplied != "" {
		for i, m := range sorted {
			if m.ID == lastApplied {
				idx = i
				break
			}
		}
	}

	var toApply []plannedMigration
	switch direction {
	case MigrationsDirectionUp:
		for _, m := range sorted[idx+1:] {
			toApply = append(toApply, plannedMigration{id: m.ID, queries: m.UpSQL, disableTx: m.DisableTransactionUp})
		}
	case MigrationsDirectionDown:
		for i := idx; i >= 0; i-- {
			m := sorted[i]
			toApply = append(toApply, plannedMigration{id: m.ID, queries: m.DownSQL, disableTx: m.DisableTransactionDown})
		}
	default:
		return nil, fmt.Errorf("unknown direction %q", direction)
	}
	if limit > 0 && limit < len(toApply) {
		toApply = toApply[:limit]
	}
	return append(plan, toApply...), nil
}

// executePlan runs planned migrations one by one and returns the number of applied ones.
// Every migration and its metadata update run in a single transaction unless the transaction is disabled for it.
// A migration with disabled transaction updates metadata only after its statements succeed.
func executePlan(ctx context.Context, db *sql.DB, store *metadataStore, plan []plannedMigration, direction MigrationsDirection) (int, error) {
	applied := 0
	for _, pm := range plan {
		if err := executeMigration(ctx, db, store, pm, direction); err != nil {
			return applied, fmt.Errorf("%w handling %s", err, pm.id)
		}
		applied++
	}
	return applied, nil
}

func executeMigration(ctx context.Context, db *sql.DB, store *metadataStore, pm plannedMigration, direction MigrationsDirection) error {
	var ex execer = db
	var tx *sql.Tx
	if !pm.disableTx {
		var err error
		if tx, err = db.BeginTx(ctx, nil); err != nil {
			return err
		}
		ex = tx
	}

	err := runStatements(ctx, ex, pm.queries)
	if err == nil {
		if direction == MigrationsDirectionUp {
			err = store.insert(ctx, ex, pm.id, time.Now())
		} else {
			err = store.delete(ctx, ex, pm.id)
		}
	}
	if tx == nil {
		return err
	}
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func runStatements(ctx context.Context, ex execer, queries []string) error {
	for _, stmt := range queries {
		// Trailing line break, space and semicolon are removed since some drivers reject them.
		stmt = strings.TrimSuffix(stmt, "\n")
		stmt = strings.TrimSuffix(stmt, " ")
		stmt = strings.TrimSuffix(stmt, ";")
		if _, err := ex.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}
