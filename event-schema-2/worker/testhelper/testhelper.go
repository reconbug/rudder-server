package testhelper

import (
	"database/sql"
	"testing"

	eventschema2 "github.com/rudderlabs/rudder-server/event-schema-2"
	"github.com/rudderlabs/rudder-server/event-schema-2/testcommons"
	"github.com/rudderlabs/rudder-server/event-schema-2/worker"
	"github.com/rudderlabs/rudder-server/services/stats/memstats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/stretchr/testify/require"
)

type DBSeeder interface {
	SetSchema(schema *eventschema2.EventSchemaInfo)
	SetVersion(version *eventschema2.EventSchemaVersionInfo)
}

func NewDBSeeder(t *testing.T, db *sql.DB) DBSeeder {
	badgerdb := testcommons.NewBadgerDB(t)
	syncer := worker.NewSyncer(worker.NewLocalDB(badgerdb, memstats.New()), db, noOpAcker{}, logger.NOP)

	return &dbSeeder{
		t:      t,
		syncer: syncer,
	}
}

type dbSeeder struct {
	t      *testing.T
	syncer *worker.Syncer
}

func (s *dbSeeder) SetSchema(schema *eventschema2.EventSchemaInfo) {
	require.NoError(s.t, s.syncer.Set(nil, schema, nil))
	require.NoError(s.t, s.syncer.Sync())
}

func (s *dbSeeder) SetVersion(version *eventschema2.EventSchemaVersionInfo) {
	require.NoError(s.t, s.syncer.Set(version, nil, nil))
	require.NoError(s.t, s.syncer.Sync())
}

type noOpAcker struct{}

func (noOpAcker) AckID(id []byte) error { return nil }
