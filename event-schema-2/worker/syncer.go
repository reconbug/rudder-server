package worker

import (
	"database/sql"
	"encoding/json"
	"fmt"
	sync "sync"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/lib/pq"
	eventschema2 "github.com/rudderlabs/rudder-server/event-schema-2"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/segmentio/ksuid"
	"google.golang.org/protobuf/encoding/protojson"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

// NewSyncer creates a new Syncer. The syncer decorates a localDB and is able to synchronise the local database with a remote database.
// Syncer is also responsible for loading the remote database's schemas & versions into the local database when keys are first accessed.
// To synchronise the local database with the remote database, the syncer's Sync method needs to be called periodically.
// Syncer's methods are not thread-safe. Only the Sync method can be called concurrently with other methods.
func NewSyncer(localDB *localDB, remoteDB *sql.DB, acker Acker, log logger.Logger) *Syncer {
	return &Syncer{
		localDB:         localDB,
		remoteDB:        remoteDB,
		acker:           acker,
		log:             log,
		loadedWriteKeys: make(map[string]struct{}),
		syncSchemaKeys:  make(map[string]struct{}),
		syncVersionKeys: make(map[string]struct{}),
	}
}

// Acker is an interface for acking correlation ids.
type Acker interface {
	// AckID acks the given correlation id.
	AckID([]byte) error
}

// Syncer is a LocalDB that synchronizes the local database with a remote database.
type Syncer struct {
	*localDB
	remoteDB        *sql.DB
	log             logger.Logger
	loadedWriteKeys map[string]struct{} // write keys that have been already loaded from the remote db into the local db

	syncMu             sync.Mutex          // mutex to synchronize access to sync maps
	syncSchemaKeys     map[string]struct{} // schemas that are meant to be synced to the remote db in the next sync
	syncVersionKeys    map[string]struct{} // versions that are meant to be synced to the remote db in the next sync
	syncCorrelationIDs [][]byte            // correlation ids to ack after sync
	acker              Acker               // acker to use for acking correlation ids after a successful sync
}

// Sync the local database with the remote database & acknowledge relevant correlation ids.
func (sd *Syncer) Sync() error {
	defer sd.stat.NewStat("event_schema_syncer_sync", stats.TimerType).RecordDuration()()

	sd.syncMu.Lock()
	// get correlation ids to ack after sync and reset
	syncCorrelationIDs := sd.syncCorrelationIDs
	var syncedCorrelationIDs int
	defer func() {
		if syncedCorrelationIDs > 0 {
			sd.stat.NewStat("event_schema_syncer_acked_correlation_ids", stats.CountType).Count(syncedCorrelationIDs)
		}
	}()
	sd.syncCorrelationIDs = nil

	// get keys to sync and reset
	schemaKeysToSync := sd.syncSchemaKeys
	var syncedSchemas int
	sd.syncSchemaKeys = make(map[string]struct{})
	versionKeysToSync := sd.syncVersionKeys
	var syncedVersions int
	sd.syncVersionKeys = make(map[string]struct{})
	sd.syncMu.Unlock()

	if len(schemaKeysToSync) == 0 && len(versionKeysToSync) == 0 {
		return nil
	}

	// sync local db with remote db
	//
	// The following steps are performed within a transaction in order:
	// 1. Create a temporary table in the remote db
	// 2. Copy the event schemas that are meant to be synced to the temporary table
	// 3. Upsert the event schemas by doing an INSERT INTO "table" SELECT * FROM "temp_table" ON CONFLICT DO UPDATE
	// 4. Drop the temporary table
	err := sd.localDB.db.View(func(localTx *badger.Txn) error {
		defer sd.stat.NewStat("event_schema_syncer_sync_db_total", stats.TimerType).RecordDuration()()
		remoteTx, err := sd.remoteDB.Begin()
		if err != nil {
			return err
		}
		defer func() { _ = remoteTx.Rollback() }()

		// sync event schemas
		if len(schemaKeysToSync) > 0 {
			if err = func() error {
				defer sd.stat.NewStat("event_schema_syncer_sync_db_schemas", stats.TimerType).RecordDuration()()

				// create temporary table
				tableName := ksuid.New().String()
				if err := sd.createTempSchemaTable(remoteTx, tableName); err != nil {
					return fmt.Errorf("failed to create temporary table for event schemas: %w", err)
				}
				// copy event schemas to temporary table
				stmt, err := remoteTx.Prepare(pq.CopyIn(tableName, "uuid", "workspace_id", "write_key", "event_type", "event_identifier", "schema", "counters", "created_at", "last_seen"))
				if err != nil {
					return fmt.Errorf("failed to prepare copy statement for event schemas: %w", err)
				}
				defer func() { _ = stmt.Close() }()
				schemasIt := localTx.NewIterator(badger.IteratorOptions{
					Prefix:         []byte(schemaPrefix),
					PrefetchValues: false,
				})
				defer schemasIt.Close()
				for schemasIt.Rewind(); schemasIt.Valid(); schemasIt.Next() {
					item := schemasIt.Item()
					if _, ok := schemaKeysToSync[string(item.Key())]; !ok {
						// skip schemas that are not meant to be synced
						continue
					}
					schema, err := sd.badgerItemToSchema(schemasIt.Item())
					if err != nil {
						return fmt.Errorf("failed to get event schema value from badgerDB: %w", err)
					}
					schemaJson, err := json.Marshal(schema.Schema)
					if err != nil {
						sd.log.Errorw("failed to marshal event schema to json", "schema", schema.Schema, "error", err)
						return fmt.Errorf("failed to marshal event schema to json: %w", err)
					}
					if _, err = stmt.Exec(schema.UID, schema.WorkspaceID, schema.Key.WriteKey, schema.Key.EventType, schema.Key.EventIdentifier, string(schemaJson), string(schema.Counters.MustProtoJSON()), schema.CreatedAt.AsTime(), schema.LastSeen.AsTime()); err != nil {
						return fmt.Errorf("failed to copy event schema in db: %w", err)
					}
					syncedSchemas++
				}
				schemasIt.Close()
				if _, err = stmt.Exec(); err != nil {
					return fmt.Errorf("failed to finish copying event schemas in db: %w", err)
				}

				// upsert event schemas
				if _, err = remoteTx.Exec(
					fmt.Sprintf(`INSERT INTO "event_schema" 
						SELECT * FROM %q 
						ON CONFLICT (uuid) 
						DO UPDATE SET schema = excluded.schema, counters = excluded.counters, last_seen = excluded.last_seen`, tableName)); err != nil {
					return fmt.Errorf("failed to upsert event schemas: %w", err)
				}

				// drop temporary table
				if _, err = remoteTx.Exec(fmt.Sprintf(`DROP TABLE %q`, tableName)); err != nil {
					return fmt.Errorf("failed to drop temporary table for event schemas: %w", err)
				}

				return nil
			}(); err != nil {
				return fmt.Errorf("failed to sync event schemas: %w", err)
			}
		}

		// sync schema versions
		if len(versionKeysToSync) > 0 {
			if err = func() error {
				defer sd.stat.NewStat("event_schema_syncer_sync_db_versions", stats.TimerType).RecordDuration()()

				// create temporary table
				tableName := ksuid.New().String()
				if err := sd.createTempVersionTable(remoteTx, tableName); err != nil {
					return fmt.Errorf("failed to create temporary table for schema versions: %w", err)
				}

				// copy schema versions to temporary table
				stmt, err := remoteTx.Prepare(pq.CopyIn(tableName, "uuid", "workspace_id", "schema_uuid", "schema_hash", "schema", "sample", "counters", "first_seen", "last_seen"))
				if err != nil {
					return fmt.Errorf("failed to prepare copy statement for schema versions: %w", err)
				}

				defer func() { _ = stmt.Close() }()
				// iterate over event schema versions
				versionsIt := localTx.NewIterator(badger.IteratorOptions{
					Prefix:         []byte(versionPrefix),
					PrefetchValues: false,
				})
				defer versionsIt.Close()
				for versionsIt.Rewind(); versionsIt.Valid(); versionsIt.Next() {
					item := versionsIt.Item()
					if _, ok := versionKeysToSync[string(item.Key())]; !ok {
						// skip schema versions that are not meant to be synced
						continue
					}
					version, err := sd.badgerItemToVersion(item)
					if err != nil {
						return fmt.Errorf("failed to get schema version value from badgerDB: %w", err)
					}
					schemaJson, err := json.Marshal(version.Schema)
					if err != nil {
						sd.log.Errorw("failed to marshal event schema to json", "schema", version.Schema, "error", err)
						return fmt.Errorf("failed to marshal event schema to json: %w", err)
					}
					sample := sql.NullString{
						String: string(version.Sample),
						Valid:  version.Sample != nil,
					}
					if _, err = stmt.Exec(version.UID, version.WorkspaceID, version.SchemaUID, version.Hash, string(schemaJson), sample, string(version.Counters.MustProtoJSON()), version.FirstSeen.AsTime(), version.LastSeen.AsTime()); err != nil {
						return fmt.Errorf("failed to copy schema version in db: %w", err)
					}
					syncedVersions++
				}
				versionsIt.Close()
				if _, err = stmt.Exec(); err != nil {
					return fmt.Errorf("failed to finish copying schema versions in db: %w", err)
				}

				// upsert schema versions
				if _, err = remoteTx.Exec(
					fmt.Sprintf(`INSERT INTO "schema_version" 
						SELECT * FROM %q 
						ON CONFLICT (uuid) 
						DO UPDATE SET schema = excluded.schema, sample = excluded.sample, counters = excluded.counters, last_seen = excluded.last_seen`, tableName)); err != nil {
					return fmt.Errorf("failed to upsert event schemas: %w", err)
				}

				// drop temporary table
				if _, err = remoteTx.Exec(fmt.Sprintf(`DROP TABLE %q`, tableName)); err != nil {
					return fmt.Errorf("failed to drop temporary table for event schemas: %w", err)
				}
				return nil
			}(); err != nil {
				return fmt.Errorf("failed to sync schema versions: %w", err)
			}
		}
		return remoteTx.Commit()
	})
	if err != nil {
		// if sync fails, put back correlation ids to unacknowledged
		sd.syncMu.Lock()
		sd.syncCorrelationIDs = append(syncCorrelationIDs, sd.syncCorrelationIDs...)
		sd.syncMu.Unlock()
		return err
	}
	sd.stat.NewStat("event_schema_syncer_synced_schemas", stats.CountType).Count(syncedSchemas)
	sd.stat.NewStat("event_schema_syncer_synced_versions", stats.CountType).Count(syncedVersions)

	// Ack correlation ids
	defer sd.stat.NewStat("event_schema_syncer_sync_ack", stats.TimerType).RecordDuration()()
	for i, correlationID := range syncCorrelationIDs {
		if err := sd.acker.AckID(correlationID); err != nil {
			// if ack fails, put back correlation ids to unacknowledged
			sd.syncMu.Lock()
			sd.syncCorrelationIDs = append(syncCorrelationIDs[i:], sd.syncCorrelationIDs...)
			sd.syncMu.Unlock()
			return fmt.Errorf("failed to ack synced correlation id: %w", err)
		}
		syncedCorrelationIDs++
	}
	return nil
}

// GetSchema decorator for localDB.GetSchema that loads the write key from the remote db if it is not already loaded.
func (sd *Syncer) GetSchema(key *eventschema2.EventSchemaKey) (*eventschema2.EventSchemaInfo, error) {
	if _, ok := sd.loadedWriteKeys[key.WriteKey]; !ok {
		if err := sd.loadWriteKey(key.WriteKey); err != nil {
			return nil, fmt.Errorf("failed to load write key from remote database: %w", err)
		}
		sd.loadedWriteKeys[key.WriteKey] = struct{}{}
	}
	return sd.localDB.GetSchema(key)
}

// GetVersion decorator for localDB.GetVersion that loads the write key from the remote db if it is not already loaded.
func (sd *Syncer) GetVersion(key *eventschema2.EventSchemaKey, versionHash string) (*eventschema2.EventSchemaVersionInfo, error) {
	if _, ok := sd.loadedWriteKeys[key.WriteKey]; !ok {
		if err := sd.loadWriteKey(key.WriteKey); err != nil {
			return nil, fmt.Errorf("failed to load write key from remote database: %w", err)
		}
		sd.loadedWriteKeys[key.WriteKey] = struct{}{}
	}
	return sd.localDB.GetVersion(key, versionHash)
}

// Set decorator for localDB.Set that keeps track of write keys and adds the correlationId to the list of unacknowledged correlation ids.
func (sd *Syncer) Set(version *eventschema2.EventSchemaVersionInfo, schema *eventschema2.EventSchemaInfo, correlationId []byte) error {
	func() {
		sd.syncMu.Lock()
		sd.syncMu.Unlock()
		if version != nil {
			sd.syncVersionKeys[string(sd.versionKeyToBadgerKey(version.SchemaKey, version.Hash))] = struct{}{}
		}
		if schema != nil {
			sd.syncSchemaKeys[string(sd.schemaKeyToBadgerKey(schema.Key))] = struct{}{}
		}
		if correlationId != nil {
			sd.syncCorrelationIDs = append(sd.syncCorrelationIDs, correlationId)
		}
	}()
	return sd.localDB.Set(version, schema, correlationId)
}

// loadWriteKey loads the write key from the remote db and stores it in the local db.
func (sd *Syncer) loadWriteKey(writeKey string) error {
	defer sd.stat.NewStat("event_schema_syncer_load", stats.TimerType).RecordDuration()()
	sd.loadedWriteKeys[writeKey] = struct{}{}
	var schemas []*eventschema2.EventSchemaInfo
	var versions []*eventschema2.EventSchemaVersionInfo
	var err error
	if schemas, err = sd.getEventSchemasFromDB(writeKey); err != nil {
		return fmt.Errorf("failed to get event schemas from database: %w", err)
	}
	if len(schemas) == 0 {
		return nil
	}
	txn := sd.localDB.db.NewTransaction(true)
	defer func() { txn.Discard() }()
	setInTx := func(key, value []byte) error { // gracefully handles txn too big errors
		if err := txn.Set(key, value); err != nil && err == badger.ErrTxnTooBig {
			if err := txn.Commit(); err != nil {
				return fmt.Errorf("failed to commit intermediate transaction: %w", err)
			}
			txn = sd.localDB.db.NewTransaction(true)
			return txn.Set(key, value)
		}
		return err
	}
	for _, schema := range schemas {
		if versions, err = sd.getEventSchemaVersionsFromDB(schema); err != nil {
			return fmt.Errorf("failed to get event schema versions from database: %w", err)
		}
		if err := setInTx(sd.schemaKeyToBadgerKey(schema.Key), schema.MustMarshal()); err != nil {
			return fmt.Errorf("failed to set event schema info: %w", err)
		}
		for _, version := range versions {
			if err := setInTx(sd.versionKeyToBadgerKey(schema.Key, version.Hash), version.MustMarshal()); err != nil {
				return fmt.Errorf("failed to set event schema version info: %w", err)
			}
		}
	}
	if err := txn.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	sd.stat.NewStat("event_schema_syncer_loaded_schemas", stats.CountType).Count(len(schemas))
	sd.stat.NewStat("event_schema_syncer_loaded_versions", stats.CountType).Count(len(versions))
	return nil
}

// getEventSchemasFromDB retrieves all relevant event schemas from the remote db for the provided write key.
func (sd *Syncer) getEventSchemasFromDB(writeKey string) ([]*eventschema2.EventSchemaInfo, error) {
	defer sd.stat.NewStat("event_schema_syncer_load_schemas", stats.TimerType).RecordDuration()()
	var schemas []*eventschema2.EventSchemaInfo
	rows, err := sd.remoteDB.Query("SELECT uuid, workspace_id, write_key, event_type, event_identifier, schema, counters, created_at, last_seen FROM event_schema WHERE write_key = $1", writeKey)
	if err != nil {
		return nil, fmt.Errorf("failed to query event schema: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		schema := &eventschema2.EventSchemaInfo{Key: &eventschema2.EventSchemaKey{}, Counters: &eventschema2.DayCounters{}}
		var createdAt, lastSeen time.Time
		var schemaRaw, counters json.RawMessage
		if err := rows.Scan(&schema.UID, &schema.WorkspaceID, &schema.Key.WriteKey, &schema.Key.EventType, &schema.Key.EventIdentifier, &schemaRaw, &counters, &createdAt, &lastSeen); err != nil {
			return nil, fmt.Errorf("failed to scan event schema row: %w", err)
		}
		schema.CreatedAt = timestamppb.New(createdAt)
		schema.LastSeen = timestamppb.New(lastSeen)
		if err := json.Unmarshal(schemaRaw, &schema.Schema); err != nil {
			sd.log.Errorw("failed to unmarshal event schema from json", "json", string(schemaRaw), "error", err)
			return nil, fmt.Errorf("failed to unmarshal schema: %w", err)
		}
		if err := protojson.Unmarshal(counters, schema.Counters); err != nil {
			sd.log.Errorw("failed to unmarshal counters from json", "json", string(counters), "error", err)
			return nil, fmt.Errorf("failed to unmarshal counters: %w", err)
		}
		schemas = append(schemas, schema)
	}

	return schemas, nil
}

// getEventSchemaVersionsFromDB retrieves all relevant event schema versions from the remote db for the provided event schema.
func (sd *Syncer) getEventSchemaVersionsFromDB(schema *eventschema2.EventSchemaInfo) ([]*eventschema2.EventSchemaVersionInfo, error) {
	defer sd.stat.NewStat("event_schema_syncer_load_schema_versions", stats.TimerType).RecordDuration()()
	var versions []*eventschema2.EventSchemaVersionInfo
	rows, err := sd.remoteDB.Query("SELECT uuid, schema_hash, schema, sample, counters, first_seen, last_seen FROM schema_version WHERE schema_uuid = $1", schema.UID)
	if err != nil {
		return nil, fmt.Errorf("failed to query schema versions: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		version := &eventschema2.EventSchemaVersionInfo{Counters: &eventschema2.DayCounters{}}
		version.Counters = &eventschema2.DayCounters{}
		version.SchemaUID = schema.UID
		version.SchemaKey = schema.Key
		version.WorkspaceID = schema.WorkspaceID
		var schema, counters json.RawMessage
		var firstSeen, lastSeen time.Time
		var sample sql.NullString
		if err := rows.Scan(&version.UID, &version.Hash, &schema, &sample, &counters, &firstSeen, &lastSeen); err != nil {
			return nil, fmt.Errorf("failed to scan schema version: %w", err)
		}
		if sample.Valid {
			version.Sample = []byte(sample.String)
		}
		if err := json.Unmarshal(schema, &version.Schema); err != nil {
			sd.log.Errorw("failed to unmarshal event schema from json", "json", string(schema), "error", err)
			return nil, fmt.Errorf("failed to unmarshal schema: %w", err)
		}
		if err := protojson.Unmarshal(counters, version.Counters); err != nil {
			sd.log.Errorw("failed to unmarshal counters from json", "json", string(counters), "error", err)
			return nil, fmt.Errorf("failed to unmarshal counters: %w", err)
		}
		version.FirstSeen = timestamppb.New(firstSeen)
		version.LastSeen = timestamppb.New(lastSeen)
		versions = append(versions, version)
	}
	return versions, nil
}

func (sd *Syncer) createTempSchemaTable(tx *sql.Tx, name string) error {
	_, err := tx.Exec(fmt.Sprintf(`CREATE TEMPORARY TABLE %q (
		uuid VARCHAR(27),
		workspace_id VARCHAR(32),
        write_key VARCHAR(32),
		event_type TEXT,
		event_identifier TEXT,
		schema JSONB,
        counters JSONB,
		created_at TIMESTAMP,
        last_seen TIMESTAMP
	)`, name))
	return err
}

func (sd *Syncer) createTempVersionTable(tx *sql.Tx, name string) error {
	_, err := tx.Exec(fmt.Sprintf(`CREATE TEMPORARY TABLE %q (
		uuid VARCHAR(27),
        workspace_id VARCHAR(32),
		schema_uuid VARCHAR(27),
		schema_hash VARCHAR(32),
		schema JSONB,
		sample TEXT,
        counters JSONB,
		first_seen TIMESTAMP,
		last_seen TIMESTAMP
	)`, name))
	return err
}
