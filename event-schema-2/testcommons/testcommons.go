package testcommons

import (
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/ory/dockertest/v3"
	"github.com/rudderlabs/rudder-server/event-schema-2/migration"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
	"github.com/stretchr/testify/require"
)

// NewBadgerDB returns a new badger db (in-memory)
func NewBadgerDB(t *testing.T) *badger.DB {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true).WithMetricsEnabled(false).WithLoggingLevel(badger.ERROR))
	require.NoError(t, err)
	return db
}

// PostgresqlResource returns a postgres container resource with the necessary schema migrations applied
func PostgresqlResource(t *testing.T) *destination.PostgresResource {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	postgresContainer, err := destination.SetupPostgres(pool, t)
	require.NoError(t, err)
	require.NoError(t, migration.Run(postgresContainer.DB, "event-schema"))
	return postgresContainer
}

// PulsarResource returns a pulsar container resource
func PulsarResource(t *testing.T) *destination.PulsarResource {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pulsarContainer, err := destination.SetupPulsar(pool, t)
	require.NoError(t, err)
	return pulsarContainer
}
