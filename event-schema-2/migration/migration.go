package migration

import (
	"database/sql"
	"embed"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed sql/*.sql
var FS embed.FS

// Run runs the database migration scripts against the given database
func Run(db *sql.DB, databaseName string) error {
	d, err := iofs.New(FS, "sql")
	if err != nil {
		return fmt.Errorf("loading migrations: %w", err)
	}
	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		return fmt.Errorf("connecting to postgres: %w", err)
	}

	m, err := migrate.NewWithInstance("iofs", d, databaseName, driver)
	m.Log = logger{}
	if err != nil {
		return fmt.Errorf("new migrate instance: %w", err)
	}

	switch m.Up() {
	case nil:
		return nil
	case migrate.ErrNoChange:
		return nil
	default:
		return err
	}
}

type logger struct{}

func (l logger) Printf(format string, v ...interface{}) {
	fmt.Printf(format, v...)
}
func (l logger) Verbose() bool { return true }
