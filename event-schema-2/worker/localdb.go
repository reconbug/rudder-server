package worker

import (
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v3"
	eventschema2 "github.com/rudderlabs/rudder-server/event-schema-2"
	"github.com/rudderlabs/rudder-server/services/stats"
)

const (
	schemaPrefix  = "s"
	versionPrefix = "v"
)

// NewLocalDB returns a new localDB. This localDB performs Get/Set operations for event schemas and their versions against a local badger db.
// The localDB methods are not thread-safe.
func NewLocalDB(db *badger.DB, stat stats.Stats) *localDB {
	return &localDB{
		db:   db,
		stat: stat,
	}
}

type localDB struct {
	db   *badger.DB
	stat stats.Stats
}

func (ldb *localDB) GetSchema(key *eventschema2.EventSchemaKey) (*eventschema2.EventSchemaInfo, error) {
	defer ldb.stat.NewStat("event_schema_ldb_get_schema", stats.TimerType).RecordDuration()()

	var schema *eventschema2.EventSchemaInfo
	err := ldb.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(ldb.schemaKeyToBadgerKey(key))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return fmt.Errorf("failed to get event schema info: %w", err)
		}
		if schema, err = ldb.badgerItemToSchema(item); err != nil {
			return fmt.Errorf("failed to unmarshal event schema info: %w", err)
		}
		return nil
	})
	return schema, err
}

func (ldb *localDB) GetVersion(key *eventschema2.EventSchemaKey, versionHash string) (*eventschema2.EventSchemaVersionInfo, error) {
	defer ldb.stat.NewStat("event_schema_ldb_get_version", stats.TimerType).RecordDuration()()

	var version *eventschema2.EventSchemaVersionInfo
	err := ldb.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(ldb.versionKeyToBadgerKey(key, versionHash))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return fmt.Errorf("failed to get event schema info: %w", err)
		}
		if version, err = ldb.badgerItemToVersion(item); err != nil {
			return fmt.Errorf("failed to unmarshal event schema info: %w", err)
		}
		return nil
	})
	return version, err
}

func (ldb *localDB) Set(version *eventschema2.EventSchemaVersionInfo, schema *eventschema2.EventSchemaInfo, correlationId []byte) error {
	defer ldb.stat.NewStat("event_schema_ldb_set", stats.TimerType).RecordDuration()()
	return ldb.db.Update(func(txn *badger.Txn) error {
		if schema != nil {
			schemaKey := ldb.schemaKeyToBadgerKey(schema.Key)
			if err := txn.Set(schemaKey, schema.MustMarshal()); err != nil {
				return fmt.Errorf("failed to set event schema info: %w", err)
			}
		}
		if version != nil {
			versionKey := ldb.versionKeyToBadgerKey(version.SchemaKey, version.Hash)
			if err := txn.Set(versionKey, version.MustMarshal()); err != nil {
				return fmt.Errorf("failed to set event schema version: %w", err)
			}
		}
		return nil
	})
}

func (*localDB) schemaKeyToBadgerKey(key *eventschema2.EventSchemaKey) []byte {
	return []byte(fmt.Sprintf("%s/%s", schemaPrefix, key.ToString()))
}

func (*localDB) badgerKeyToSchemaKey(raw []byte) (key *eventschema2.EventSchemaKey, err error) {
	v := string(raw)
	parts := strings.Split(v, "/")
	if len(parts) != 2 || parts[0] != schemaPrefix {
		return nil, fmt.Errorf("invalid badger event schema key: %s", v)
	}
	parts = strings.Split(parts[1], ":")
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid event schema key: %s", v)
	}
	return &eventschema2.EventSchemaKey{WriteKey: parts[0], EventType: parts[1], EventIdentifier: parts[2]}, nil
}

func (*localDB) versionKeyToBadgerKey(key *eventschema2.EventSchemaKey, versionHash string) []byte {
	return []byte(fmt.Sprintf("%s/%s/%s", versionPrefix, key.ToString(), versionHash))
}

func (*localDB) badgerKeyToVersionKey(raw []byte) (key *eventschema2.EventSchemaKey, hash string, err error) {
	v := string(raw)
	parts := strings.Split(v, "/")
	if len(parts) != 3 || parts[0] != versionPrefix {
		return nil, "", fmt.Errorf("invalid badger event schema version key: %s", v)
	}
	hash = parts[2]
	parts = strings.Split(parts[1], ":")
	if len(parts) != 3 {
		return nil, hash, fmt.Errorf("invalid event schema key: %s", v)
	}
	return &eventschema2.EventSchemaKey{WriteKey: parts[0], EventType: parts[1], EventIdentifier: parts[2]}, hash, nil
}

func (*localDB) badgerItemToSchema(item *badger.Item) (schema *eventschema2.EventSchemaInfo, err error) {
	err = item.Value(func(val []byte) error {
		var err error
		schema, err = eventschema2.UnmarshalEventSchemaInfo(val)
		return err
	})
	return
}

func (*localDB) badgerItemToVersion(item *badger.Item) (version *eventschema2.EventSchemaVersionInfo, err error) {
	err = item.Value(func(val []byte) error {
		var err error
		version, err = eventschema2.UnmarshalEventSchemaVersionInfo(val)
		return err
	})
	return
}
