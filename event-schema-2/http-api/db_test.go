package httpapi

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	eventschema2 "github.com/rudderlabs/rudder-server/event-schema-2"
	"github.com/rudderlabs/rudder-server/event-schema-2/testcommons"
	"github.com/rudderlabs/rudder-server/event-schema-2/worker/testhelper"
	"github.com/samber/lo"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestDBRepo(t *testing.T) {
	postgres := testcommons.PostgresqlResource(t)
	refTime := time.Now().Round(24 * time.Hour)
	workspaceID1 := "ws1"
	workspaceID2 := "ws2"
	writeKey1 := "test-write-key-1"
	writeKey2 := "test-write-key-2"
	var ws1Schemas []*eventschema2.EventSchemaInfo
	var ws1Versions []*eventschema2.EventSchemaVersionInfo
	var ws2Schemas []*eventschema2.EventSchemaInfo
	var ws2Versions []*eventschema2.EventSchemaVersionInfo

	s, v := seedSchemas(t, postgres.DB, workspaceID1, writeKey1, refTime, 2, 1)
	ws1Schemas = append(ws1Schemas, s...)
	ws1Versions = append(ws1Versions, v...)
	s, v = seedSchemas(t, postgres.DB, workspaceID1, writeKey2, refTime, 2, 1)
	ws1Schemas = append(ws1Schemas, s...)
	ws1Versions = append(ws1Versions, v...)
	s, v = seedSchemas(t, postgres.DB, workspaceID2, writeKey1, refTime, 1, 2)
	ws2Schemas = append(ws2Schemas, s...)
	ws2Versions = append(ws2Versions, v...)

	repo := &dbRepo{
		pageSize:      1,
		retentionDays: 2,
		db:            postgres.DB,
	}

	t.Run("list schemas", func(t *testing.T) {
		t.Run("without filtering", func(t *testing.T) {
			totalPages := len(ws1Schemas)
			for page := 1; page <= totalPages; page++ {
				p, err := repo.ListSchemas(context.Background(), workspaceID1, SchemaFilter{}, page)
				require.NoError(t, err)
				require.Equal(t, page, p.CurrentPage)
				if page <= totalPages {
					require.Len(t, p.Results, 1)
					require.True(t, p.HasNext)
				} else {
					require.False(t, p.HasNext)
					require.Empty(t, p.Results)
					require.Equal(t, EventSchemaInfoToSchema(ws1Schemas[page-1]), p.Results[0])
				}
			}
		})
		t.Run("with filtering", func(t *testing.T) {
			schemas := lo.Filter(ws1Schemas, func(s *eventschema2.EventSchemaInfo, _ int) bool {
				return s.Key.WriteKey == writeKey1
			})
			totalPages := len(schemas)
			for page := 1; page <= totalPages; page++ {
				p, err := repo.ListSchemas(context.Background(), workspaceID1, SchemaFilter{WriteKey: writeKey1}, page)
				require.NoError(t, err)
				require.Equal(t, page, p.CurrentPage)
				if page <= totalPages {
					require.Len(t, p.Results, 1)
					require.True(t, p.HasNext)
				} else {
					require.False(t, p.HasNext)
					require.Empty(t, p.Results)
					require.Equal(t, EventSchemaInfoToSchema(ws1Schemas[page-1]), p.Results[0])
				}
			}

			t.Run("with invalid writeKey in filtering", func(t *testing.T) {
				p, err := repo.ListSchemas(context.Background(), workspaceID2, SchemaFilter{WriteKey: writeKey2}, 1)
				require.NoError(t, err)
				require.Empty(t, p.Results)
				require.False(t, p.HasNext)
			})
		})
	})

	t.Run("get schema", func(t *testing.T) {
		schemaUID := ws2Schemas[0].UID
		versions := lo.Filter(ws2Versions, func(v *eventschema2.EventSchemaVersionInfo, _ int) bool {
			return v.SchemaUID == schemaUID
		})
		slices.SortFunc(versions, func(i, j *eventschema2.EventSchemaVersionInfo) bool { // descending order
			return i.UID > j.UID
		})
		schema, err := repo.GetSchema(context.Background(), workspaceID2, schemaUID)
		require.NoError(t, err)
		expected, err := json.Marshal(EventSchemaInfoToSchemaInfo(ws2Schemas[0], versions))
		require.NoError(t, err)
		actual, err := json.Marshal(schema)
		require.Equal(t, string(expected), string(actual))

		t.Run("not found due to invalid workspaceID", func(t *testing.T) {
			schemaUID := ws2Schemas[0].UID
			schema, err := repo.GetSchema(context.Background(), workspaceID1, schemaUID)
			require.NoError(t, err)
			require.Nil(t, schema)
		})
	})

	t.Run("list versions", func(t *testing.T) {
		schemaUID := ws1Schemas[0].UID
		schemaVersions := lo.Filter(ws1Versions, func(v *eventschema2.EventSchemaVersionInfo, _ int) bool {
			return v.SchemaUID == schemaUID
		})
		totalPages := len(schemaVersions)
		for page := 1; page <= totalPages; page++ {
			p, err := repo.ListVersions(context.Background(), workspaceID1, schemaUID, 1)
			require.NoError(t, err)
			require.Equal(t, page, p.CurrentPage)
			if page <= totalPages {
				require.Len(t, p.Results, 1)
				require.True(t, p.HasNext)
			} else {
				require.False(t, p.HasNext)
				require.Empty(t, p.Results)
				require.Equal(t, EventSchemaVersionInfoToVersion(schemaVersions[page-1]), p.Results[0])
			}
		}
	})

	t.Run("get version", func(t *testing.T) {
		version := ws2Versions[0]
		v, err := repo.GetVersion(context.Background(), workspaceID2, version.SchemaUID, version.UID)
		require.NoError(t, err)
		expected, err := json.Marshal(EventSchemaVersionInfoToVersionInfo(version))
		require.NoError(t, err)
		actual, err := json.Marshal(v)
		require.Equal(t, expected, actual)

		t.Run("not found due to invalid workspaceID", func(t *testing.T) {
			version := ws2Versions[0]
			v, err := repo.GetVersion(context.Background(), workspaceID1, version.SchemaUID, version.UID)
			require.NoError(t, err)
			require.Nil(t, v)
		})
	})
}

func seedSchemas(t *testing.T, db *sql.DB, workspaceID, writeKey string, refTime time.Time, schemaCount, versionsPerSchema int) (schemas []*eventschema2.EventSchemaInfo, versions []*eventschema2.EventSchemaVersionInfo) {
	dbSeeder := testhelper.NewDBSeeder(t, db)
	for i := 0; i < schemaCount; i++ {
		schema := &eventschema2.EventSchemaInfo{
			UID:         ksuid.New().String(),
			WorkspaceID: workspaceID,
			Key: &eventschema2.EventSchemaKey{
				WriteKey:        writeKey,
				EventType:       "test-event-type",
				EventIdentifier: fmt.Sprintf("test-event-identifier-%d", i),
			},
			Schema: map[string]string{
				"test-key": "test-value",
			},
			Counters: &eventschema2.DayCounters{
				Values: []*eventschema2.DayCounter{
					{Day: timestamppb.New(refTime), Count: 1},
				},
			},
			CreatedAt: timestamppb.New(refTime),
			LastSeen:  timestamppb.New(refTime),
		}
		dbSeeder.SetSchema(schema)
		schemas = append(schemas, schema)

		for j := 0; j < versionsPerSchema; j++ {
			version := &eventschema2.EventSchemaVersionInfo{
				UID:         ksuid.New().String(),
				SchemaUID:   schema.UID,
				WorkspaceID: workspaceID,
				SchemaKey:   schema.Key,
				Hash:        fmt.Sprintf("test-hash-%d-%d", i, j),
				Sample:      []byte("test-sample"),
				Schema: map[string]string{
					"test-key": "test-value",
				},
				Counters: &eventschema2.DayCounters{
					Values: []*eventschema2.DayCounter{
						{Day: timestamppb.New(refTime), Count: 1},
					},
				},
				FirstSeen: timestamppb.New(refTime),
				LastSeen:  timestamppb.New(refTime),
			}
			dbSeeder.SetVersion(version)
			versions = append(versions, version)
		}
	}
	return
}

func EventSchemaInfoToSchema(s *eventschema2.EventSchemaInfo) *Schema {
	return &Schema{
		UID:             s.UID,
		WriteKey:        s.Key.WriteKey,
		EventType:       s.Key.EventType,
		EventIdentifier: s.Key.EventIdentifier,
		Schema:          s.Schema,
		CreatedAt:       s.CreatedAt.AsTime(),
		LastSeen:        s.LastSeen.AsTime(),
		Count:           s.Counters.Sum(10),
	}
}

func EventSchemaInfoToSchemaInfo(s *eventschema2.EventSchemaInfo, versions []*eventschema2.EventSchemaVersionInfo) *SchemaInfo {
	return &SchemaInfo{
		Schema: *EventSchemaInfoToSchema(s),
		LatestVersions: lo.Map(versions, func(v *eventschema2.EventSchemaVersionInfo, _ int) *Version {
			return &Version{
				UID:       v.UID,
				Count:     v.Counters.Sum(10),
				FirstSeen: v.FirstSeen.AsTime(),
				LastSeen:  v.LastSeen.AsTime(),
			}
		}),
	}
}

func EventSchemaVersionInfoToVersion(v *eventschema2.EventSchemaVersionInfo) *Version {
	return &Version{
		UID:             v.UID,
		SchemaUID:       v.SchemaUID,
		WriteKey:        v.SchemaKey.WriteKey,
		EventType:       v.SchemaKey.EventType,
		EventIdentifier: v.SchemaKey.EventIdentifier,
		Schema:          v.Schema,
		FirstSeen:       v.FirstSeen.AsTime(),
		LastSeen:        v.LastSeen.AsTime(),
		Count:           v.Counters.Sum(10),
	}
}

func EventSchemaVersionInfoToVersionInfo(v *eventschema2.EventSchemaVersionInfo) *VersionInfo {
	return &VersionInfo{
		Version: *EventSchemaVersionInfoToVersion(v),
		Sample:  v.Sample,
	}
}
