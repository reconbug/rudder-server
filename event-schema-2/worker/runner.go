package worker

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/options"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/event-schema-2/migration"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"golang.org/x/sync/errgroup"
)

// Run runs the worker
func Run(mainCtx context.Context, conf *config.Config, stat stats.Stats, log logger.Logger) error {
	g, ctx := errgroup.WithContext(mainCtx)

	// pulsar client & consumer
	pulsarClient, err := NewPulsarClient(conf, log)
	if err != nil {
		return fmt.Errorf("failed to create pulsar client: %w", err)
	}
	defer pulsarClient.Close()
	pulsarConsumer, err := NewPulsarConsumer(pulsarClient, conf)
	if err != nil {
		return fmt.Errorf("failed to create pulsar consumer: %w", err)
	}
	defer pulsarConsumer.Close()

	// local badger db
	badgerDB, err := SetupBadgerDB(conf)
	if err != nil {
		return fmt.Errorf("failed to setup badger db: %w", err)
	}
	defer func() {
		if err := badgerDB.Close(); err != nil {
			log.Errorw("failed to close badger db", "error", err)
		}
	}()
	localDB := NewLocalDB(badgerDB, stat)

	// remote postgres db
	remoteDB, err := SetupPostgresDB(ctx, conf.GetString("DB_DSN", "root@tcp(127.0.0.1:3306)/service"))
	if err != nil {
		return fmt.Errorf("failed to setup remote postgres db: %w", err)
	}
	if err := migration.Run(remoteDB, "event-schema"); err != nil {
		return fmt.Errorf("failed to run db schema migration: %w", err)
	}
	defer func() { _ = remoteDB.Close() }()

	// syncer & sync loop
	syncer := NewSyncer(localDB, remoteDB, &pulsarAcker{pulsarConsumer}, log)
	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(conf.GetDuration("SYNC_INTERVAL", 60, time.Second)):
				if err := syncer.Sync(); err != nil {
					return fmt.Errorf("sync loop failed: %w", err)
				}
			}
		}
	})

	// consumer loop
	g.Go(func() error {
		if err := ConsumerLoop(ctx, pulsarConsumer, NewHandler(syncer, conf), stat, log); err != nil {
			return fmt.Errorf("consumer loop failed: %w", err)
		}
		return nil
	})

	if err := g.Wait(); err != nil && mainCtx.Err() == nil {
		return err
	}
	return nil
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

// SetupBadgerDB sets up a badger database based on the provided configuration.
// Before starting the database, it removes the database directory if it exists.
func SetupBadgerDB(conf *config.Config) (*badger.DB, error) {
	path := conf.GetString("BADGER_DB_PATH", "/tmp/badger")
	if err := os.RemoveAll(path); err != nil { // always need to start fresh due to possible scale up/down of workers (rebalancing of writeKeys)
		return nil, fmt.Errorf("failed to remove badger db path %q: %w", path, err)
	}
	badgerOpts := badger.
		DefaultOptions(path).
		WithCompression(options.None).
		WithIndexCacheSize(16 << 20). // 16mb
		WithNumGoroutines(1).
		WithNumMemtables(conf.GetInt("BADGER_DB_NUM_MEMTABLE", 1)).
		WithValueThreshold(conf.GetInt64("BADGER_DB_VALUE_THRESHOLD", 1048576)).
		WithBlockCacheSize(0).
		WithNumVersionsToKeep(1).
		WithNumLevelZeroTables(conf.GetInt("BADGER_DB_NUM_LEVEL_ZERO_TABLES", 5)).
		WithNumLevelZeroTablesStall(conf.GetInt("BADGER_DB_NUM_LEVEL_ZERO_TABLES_STALL", 15)).
		WithSyncWrites(conf.GetBool("BADGER_DB_SYNC_WRITES", false))
	db, err := badger.Open(badgerOpts)
	if err != nil {
		return nil, err
	}
	return db, nil
}
