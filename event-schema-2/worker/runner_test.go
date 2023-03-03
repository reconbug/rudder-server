package worker

import (
	"context"
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/rudderlabs/rudder-server/config"
	eventschema2 "github.com/rudderlabs/rudder-server/event-schema-2"
	"github.com/rudderlabs/rudder-server/event-schema-2/testcommons"
	"github.com/rudderlabs/rudder-server/services/stats/memstats"
	"github.com/rudderlabs/rudder-server/testhelper/rand"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Starting multiple runners and sending messages for different write keys to pulsar.
// Finally, verifying that all the messages are synced to the database properly with the expected counters.
func TestRunner(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g, ctx := errgroup.WithContext(ctx)

	pulsarContainer := testcommons.PulsarResource(t)
	postgres := testcommons.PostgresqlResource(t)
	startRunner := func(t *testing.T, ctx context.Context, g *errgroup.Group, nodeIndex, totalNodes int) {
		conf := config.New()
		conf.Set("DB_DSN", postgres.DBDsn)
		conf.Set("PULSAR_CLIENT_URL", pulsarContainer.URL)
		conf.Set("PULSAR_CONSUMER_NODE_IDX", nodeIndex)
		conf.Set("PULSAR_CONSUMER_TOTAL_NODES", totalNodes)
		conf.Set("PULSAR_CONSUMER_RECEIVER_QUEUE_SIZE", 10)
		conf.Set("SYNC_INTERVAL", "1s")
		conf.Set("RETENTION_DAYS", 2)
		conf.Set("BADGER_DB_PATH", path.Join(t.TempDir(), "badger", fmt.Sprintf("%d", nodeIndex)))
		stat := memstats.New()
		log := logger.NewFactory(conf).NewLogger()
		g.Go(func() error {
			err := Run(ctx, conf, stat, log)
			if err != nil {
				log.Errorw("runner exited with error", "nodeIndex", nodeIndex, "totalNodes", totalNodes, "error", err)
			}
			return err
		})
	}
	const (
		totalNodes     = 5
		totalWriteKeys = 100
	)

	for i := 0; i < totalNodes; i++ {
		startRunner(t, ctx, g, i, totalNodes)
	}

	conf := config.New()
	conf.Set("PULSAR_CLIENT_URL", pulsarContainer.URL)
	client, err := NewPulsarClient(conf, logger.NOP)
	require.NoError(t, err)
	defer client.Close()
	producer, err := NewPulsarProducer(client, conf)
	require.NoError(t, err)
	defer producer.Close()

	sendMessages := func(writeKey string, num int, dateOffset time.Duration, schemaKeys ...string) {
		schema := map[string]string{}
		for _, key := range schemaKeys {
			schema[key] = "string"
		}
		for i := 0; i < num; i++ {
			msg := &eventschema2.EventSchemaMessage{
				WorkspaceID: "workspace-id",
				Key: &eventschema2.EventSchemaKey{
					WriteKey:        writeKey,
					EventType:       "event-type",
					EventIdentifier: "event-identifier",
				},
				Schema:     schema,
				ObservedAt: timestamppb.New(time.Now().Add(dateOffset)),
			}
			_, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
				Key:         writeKey,
				OrderingKey: writeKey,
				Payload:     msg.MustMarshal(),
			})
			require.NoError(t, err)
		}
	}

	errg := &errgroup.Group{}
	errg.SetLimit(10)
	for i := 0; i < totalWriteKeys; i++ {
		writeKey := fmt.Sprintf("key-%s-%d", rand.String(5), i)
		errg.Go(func() error {
			// send some expired messages (should not be processed/synced to the database)
			sendMessages(writeKey, 5, -3*24*time.Hour, "expired")
			// 3 versions for each writeKey (a version is defined by the schema keys)
			// 5 messages for each version
			sendMessages(writeKey, 2, 0, "key1")
			sendMessages(writeKey, 5, 0, "key1", "key2")
			sendMessages(writeKey, 3, 0, "key1")
			sendMessages(writeKey, 5, 0, "key1", "key2", "key3")
			return nil
		})
	}
	_ = errg.Wait()
	require.Eventually(t, func() bool {
		require.Nil(t, ctx.Err())
		var schemaCount int
		require.NoError(t, postgres.DB.QueryRow("SELECT count(*) FROM event_schema").Scan(&schemaCount))
		var versionCount int
		require.NoError(t, postgres.DB.QueryRow("SELECT count(*) FROM schema_version").Scan(&versionCount))
		fmt.Printf("schemaCount: %d, versionCount: %d\n", schemaCount, versionCount)
		return schemaCount == totalWriteKeys && versionCount == 3*totalWriteKeys
	}, 10*time.Second, 1*time.Second)

	verifyCounters := func(t *testing.T, table string, totals int) {
		rows, err := postgres.DB.Query(fmt.Sprintf("SELECT counters FROM %q", table))
		require.NoError(t, err)
		defer rows.Close()
		for rows.Next() {
			var counters eventschema2.DayCounters
			var countersRaw []byte
			require.NoError(t, rows.Scan(&countersRaw))
			require.NoError(t, protojson.Unmarshal(countersRaw, &counters))
			require.EqualValues(t, totals, counters.Sum(30), "counters for table %q", table)
		}
	}

	verifyCounters(t, "schema_version", 5)
	verifyCounters(t, "event_schema", 15) // 3*5

	cancel()
	require.NoError(t, g.Wait())

	// Start again N+1 runners and send one new write key to verify that:
	//
	// 1. Older messages are not synced again (have been acked successfully)
	// 2. New messages will be synced
	// 3. The change in the number of runner nodes does not affect the sync process
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	g, ctx = errgroup.WithContext(ctx)
	for i := 0; i < totalNodes+1; i++ {
		startRunner(t, ctx, g, i, totalNodes+1)
	}
	writeKey := fmt.Sprintf("key-%s-%d", rand.String(5), 999)
	sendMessages(writeKey, 5, 0, "key1")
	sendMessages(writeKey, 5, 0, "key1", "key2")
	sendMessages(writeKey, 5, 0, "key1", "key2", "key3")
	require.Eventually(t, func() bool {
		require.Nil(t, ctx.Err())
		var schemaCount int
		require.NoError(t, postgres.DB.QueryRow("SELECT count(*) FROM event_schema").Scan(&schemaCount))
		var versionCount int
		require.NoError(t, postgres.DB.QueryRow("SELECT count(*) FROM schema_version").Scan(&versionCount))
		fmt.Printf("schemaCount: %d, versionCount: %d\n", schemaCount, versionCount)
		return schemaCount == totalWriteKeys+1 && versionCount == 3*(totalWriteKeys+1)
	}, 10*time.Second, 1*time.Second)
	verifyCounters(t, "schema_version", 5)
	verifyCounters(t, "event_schema", 15)
	cancel()
	require.NoError(t, g.Wait())
}
