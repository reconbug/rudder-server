package worker

import (
	"fmt"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	eventschema2 "github.com/rudderlabs/rudder-server/event-schema-2"
	"github.com/rudderlabs/rudder-server/event-schema-2/testcommons"
	"github.com/rudderlabs/rudder-server/testhelper/rand"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestHandler(t *testing.T) {
	t.Run("sequence of messages for same key", func(t *testing.T) {
		postgres := testcommons.PostgresqlResource(t).DB
		syncer, _, acker := newSyncerlDB(t, postgres)
		conf := config.New()
		h := NewHandler(syncer, conf)

		key := &eventschema2.EventSchemaKey{
			WriteKey:        "writeKey-1",
			EventType:       "eventType-1",
			EventIdentifier: "eventIdentifier-1",
		}

		messages := genSchemaMessages(key, 100)
		for _, m := range messages {
			require.NoError(t, h.Handle(m))
		}
		require.NoError(t, syncer.Sync())
		require.Len(t, acker.acks, 100)

		var count int
		require.NoError(t, postgres.QueryRow("SELECT count(*) FROM event_schema").Scan(&count))
		require.Equal(t, 1, count)
		require.NoError(t, postgres.QueryRow("SELECT count(*) FROM schema_version").Scan(&count))
		require.Equal(t, 100, count)

		messages = genSchemaMessages(key, 200)
		for _, m := range messages {
			require.NoError(t, h.Handle(m))
		}
		require.NoError(t, syncer.Sync())
		require.Len(t, acker.acks, 300)
		require.NoError(t, postgres.QueryRow("SELECT count(*) FROM event_schema").Scan(&count))
		require.Equal(t, 1, count)
		require.NoError(t, postgres.QueryRow("SELECT count(*) FROM schema_version").Scan(&count))
		require.Equal(t, 200, count)
	})

	t.Run("handling expired message", func(t *testing.T) {
		postgres := testcommons.PostgresqlResource(t).DB
		syncer, _, acker := newSyncerlDB(t, postgres)
		conf := config.New()
		retentionDays := 10
		conf.Set("RETENTION_DAYS", retentionDays)
		h := NewHandler(syncer, conf)

		key := &eventschema2.EventSchemaKey{
			WriteKey:        "writeKey-1",
			EventType:       "eventType-1",
			EventIdentifier: "eventIdentifier-1",
		}

		messages := genSchemaMessages(key, 1)
		for _, m := range messages {
			m.ObservedAt = timestamppb.New(time.Now().AddDate(0, 0, -int(retentionDays)))
			err := h.Handle(m)
			require.Error(t, err)
			require.ErrorIs(t, ErrMessageExpired, err)
		}
		require.NoError(t, syncer.Sync())
		require.Len(t, acker.acks, 0)

		var count int
		require.NoError(t, postgres.QueryRow("SELECT count(*) FROM event_schema").Scan(&count))
		require.Equal(t, 0, count)
		require.NoError(t, postgres.QueryRow("SELECT count(*) FROM schema_version").Scan(&count))
		require.Equal(t, 0, count)

		messages = genSchemaMessages(key, 1)
		for _, m := range messages {
			m.ObservedAt = timestamppb.New(time.Now().AddDate(0, 0, 1-int(retentionDays)))
			require.NoError(t, h.Handle(m))
		}
		require.NoError(t, syncer.Sync())
		require.Len(t, acker.acks, 1)

		require.NoError(t, postgres.QueryRow("SELECT count(*) FROM event_schema").Scan(&count))
		require.Equal(t, 1, count)
		require.NoError(t, postgres.QueryRow("SELECT count(*) FROM schema_version").Scan(&count))
		require.Equal(t, 1, count)
	})
}

func genSchemaMessages(key *eventschema2.EventSchemaKey, count int) []*eventschema2.EventSchemaMessage {
	res := make([]*eventschema2.EventSchemaMessage, count)
	for i := 0; i < count; i++ {
		schema := map[string]string{}
		for j := 0; j < i+1; j++ {
			var t string
			switch j % 3 {
			case 0:
				t = "string"
			case 1:
				t = "int"
			case 2:
				t = "float"
			}
			schema[fmt.Sprintf("key-%d", j)] = t
		}
		res[i] = &eventschema2.EventSchemaMessage{
			WorkspaceID:   "workspace-id",
			Key:           key,
			ObservedAt:    timestamppb.Now(),
			Schema:        schema,
			Sample:        []byte("sample"),
			CorrelationID: []byte(rand.String(10)),
		}
	}
	return res
}
