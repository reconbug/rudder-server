package httpapi

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/event-schema-2/migration"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

// Run runs the service
func Run(ctx context.Context, conf *config.Config, stat stats.Stats, log logger.Logger) error {
	remoteDB, err := SetupPostgresDB(ctx, conf.GetString("DB_DSN", "root@tcp(127.0.0.1:3306)/service"))
	if err != nil {
		return fmt.Errorf("failed to setup remote postgres db: %w", err)
	}
	if err := migration.Run(remoteDB, "event-schema"); err != nil {
		return fmt.Errorf("failed to run db schema migration: %w", err)
	}
	defer func() { _ = remoteDB.Close() }()

	dbService := &dbRepo{
		retentionDays: conf.GetInt("RETENTION_DAYS", 30),
		pageSize:      conf.GetInt("HTTP_PAGE_SIZE", 50),
		db:            remoteDB,
	}

	return StartServer(ctx, conf, stat, dbService)
}

// SetupPostgresDB sets up a postgres database based on the provided DSN.
func SetupPostgresDB(ctx context.Context, DSN string) (*sql.DB, error) {
	db, err := sql.Open("postgres", DSN)
	if err != nil {
		return nil, err
	}

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}
