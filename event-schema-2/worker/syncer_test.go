package worker

import (
	"database/sql"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
	eventschema2 "github.com/rudderlabs/rudder-server/event-schema-2"
	"github.com/rudderlabs/rudder-server/event-schema-2/testcommons"
	"github.com/rudderlabs/rudder-server/services/stats/memstats"
	"github.com/rudderlabs/rudder-server/testhelper/rand"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestSyncer(t *testing.T) {
	t.Run("set and sync", func(t *testing.T) {
		postgres := testcommons.PostgresqlResource(t).DB
		syncer1, _, acker := newSyncerlDB(t, postgres)
		correlationId := "correlationId-1"
		schema, version := newSchemaAndVersion()
		err := syncer1.Set(version, schema, []byte(correlationId))
		require.NoError(t, err)
		require.NoError(t, syncer1.Sync())

		var schemaCount int
		require.NoError(t, postgres.QueryRow("select count(*) from event_schema where uuid = $1", schema.UID).Scan(&schemaCount))
		require.Equal(t, 1, schemaCount)
		var versionCount int
		require.NoError(t, postgres.QueryRow("select count(*) from schema_version where uuid = $1", version.UID).Scan(&versionCount))
		require.Equal(t, 1, versionCount)
		require.Len(t, acker.acks, 1)
		require.Equal(t, correlationId, string(acker.acks[0]))
	})

	t.Run("set and partial sync", func(t *testing.T) {
		postgres := testcommons.PostgresqlResource(t).DB
		syncer1, _, acker := newSyncerlDB(t, postgres)
		correlationId := "correlationId-1"
		schema, version := newSchemaAndVersion()
		require.NoError(t, syncer1.Set(version, schema, []byte(correlationId)))
		require.NoError(t, syncer1.Sync())

		require.Len(t, acker.acks, 1)
		require.Equal(t, correlationId, string(acker.acks[0]))

		// add anoter pair and sync again. This should not sync the first pair
		correlationId2 := "correlationId-2"
		schema, version = newSchemaAndVersion()
		require.NoError(t, syncer1.Set(version, schema, []byte(correlationId2)))
		require.NoError(t, syncer1.Sync())

		var schemaCount int
		require.NoError(t, postgres.QueryRow("select count(*) from event_schema").Scan(&schemaCount))
		require.Equal(t, 2, schemaCount)
		var versionCount int
		require.NoError(t, postgres.QueryRow("select count(*) from schema_version").Scan(&versionCount))
		require.Equal(t, 2, versionCount)
		require.Len(t, acker.acks, 2)
		require.Equal(t, correlationId2, string(acker.acks[1]))

		// update first version and sync again. This should sync/update the first version in db
		correlationId3 := "correlationId-3"
		version.Counters.IncrementCounter(time.Now(), 2)
		version.Sample = []byte("sample2")
		require.NoError(t, syncer1.Set(version, nil, []byte(correlationId3)))
		require.NoError(t, syncer1.Sync())

		var dbSample sql.NullString
		require.NoError(t, postgres.QueryRow("select sample from schema_version where uuid = $1", version.UID).Scan(&dbSample))
		require.True(t, dbSample.Valid)
		require.Equal(t, "sample2", dbSample.String)
	})

	t.Run("load from remote db", func(t *testing.T) {
		postgres := testcommons.PostgresqlResource(t).DB
		syncer1, _, _ := newSyncerlDB(t, postgres) // to seed the remote db
		syncer2, _, _ := newSyncerlDB(t, postgres) // to load from the remote db

		correlationId := "correlationId-1"
		schema1, version1 := newSchemaAndVersion()
		require.NoError(t, syncer1.Set(version1, schema1, []byte(correlationId)))
		require.NoError(t, syncer1.Sync())

		schema2, version2 := newSchemaAndVersion()
		version2.Sample = []byte("sample2")
		require.NoError(t, syncer1.Set(version2, schema2, nil))
		require.NoError(t, syncer1.Sync())

		// load from remote db when calling GetSchema
		foundSchema1, err := syncer2.GetSchema(schema1.Key)
		require.NoError(t, err)
		require.Equal(t, schema1.String(), foundSchema1.String())
		foundVersion1, err := syncer2.GetVersion(schema1.Key, version1.Hash)
		require.NoError(t, err)
		require.Equal(t, version1.String(), foundVersion1.String())

		// load from remote db when calling GetVersion
		foundVersion2, err := syncer2.GetVersion(schema2.Key, version2.Hash)
		require.NoError(t, err)
		require.Equal(t, version2.String(), foundVersion2.String())
		foundSchema2, err := syncer2.GetSchema(schema2.Key)
		require.NoError(t, err)
		require.Equal(t, schema2.String(), foundSchema2.String())
	})
}

func newSyncerlDB(t *testing.T, postgres *sql.DB) (*Syncer, *badger.DB, *testAcker) {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true).WithMetricsEnabled(false).WithLoggingLevel(badger.ERROR))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	acker := &testAcker{}
	localDB := &localDB{
		db:   db,
		stat: memstats.New(),
	}
	return NewSyncer(localDB, postgres, acker, logger.NOP), db, acker
}

type testAcker struct {
	acks [][]byte
}

func (a *testAcker) AckID(id []byte) error {
	a.acks = append(a.acks, id)
	return nil
}

func newSchemaAndVersion() (*eventschema2.EventSchemaInfo, *eventschema2.EventSchemaVersionInfo) {
	key := &eventschema2.EventSchemaKey{
		WriteKey:        rand.String(10),
		EventType:       rand.String(10),
		EventIdentifier: rand.String(10),
	}
	hash := rand.String(10)
	workspaceID := rand.String(10)
	schema := &eventschema2.EventSchemaInfo{
		UID:         ksuid.New().String(),
		WorkspaceID: workspaceID,
		Key:         key,
		Schema:      map[string]string{"key": "type"},
		Counters:    &eventschema2.DayCounters{},
		CreatedAt:   timestamppb.Now(),
		LastSeen:    timestamppb.Now(),
	}
	schema.Counters.IncrementCounter(time.Now(), 1)
	version := &eventschema2.EventSchemaVersionInfo{
		UID:         ksuid.New().String(),
		WorkspaceID: workspaceID,
		SchemaUID:   schema.UID,
		SchemaKey:   key,
		Hash:        hash,
		Schema:      map[string]string{"key": "type"},
		Counters:    &eventschema2.DayCounters{},
		Sample:      nil,
		FirstSeen:   timestamppb.Now(),
		LastSeen:    timestamppb.Now(),
	}
	version.Counters.IncrementCounter(time.Now(), 1)
	return schema, version
}
