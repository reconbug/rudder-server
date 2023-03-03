package worker

import (
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v3"
	eventschema2 "github.com/rudderlabs/rudder-server/event-schema-2"
	"github.com/rudderlabs/rudder-server/event-schema-2/testcommons"
	"github.com/rudderlabs/rudder-server/services/stats/memstats"
	"github.com/stretchr/testify/require"
)

func TestLocalDB(t *testing.T) {
	t.Run("set then get", func(t *testing.T) {
		t.Run("set then get schema info", func(t *testing.T) {
			db, _ := newLocalDB(t)
			key := &eventschema2.EventSchemaKey{
				WriteKey:        "writeKey-1",
				EventType:       "eventType-1",
				EventIdentifier: "eventIdentifier-1",
			}
			found, err := db.GetSchema(key)
			require.NoError(t, err)
			require.Nil(t, found)

			schema := &eventschema2.EventSchemaInfo{
				Key: key,
			}
			err = db.Set(nil, schema, nil)
			require.NoError(t, err)

			found, err = db.GetSchema(key)
			require.NoError(t, err)
			require.Equal(t, schema.String(), found.String())

			anotherKey := &eventschema2.EventSchemaKey{
				WriteKey:        "writeKey-2",
				EventType:       "eventType-1",
				EventIdentifier: "eventIdentifier-1",
			}
			found, err = db.GetSchema(anotherKey)
			require.NoError(t, err)
			require.Nil(t, found)
		})

		t.Run("set then get schema version info", func(t *testing.T) {
			db, _ := newLocalDB(t)
			key := &eventschema2.EventSchemaKey{
				WriteKey:        "writeKey-1",
				EventType:       "eventType-1",
				EventIdentifier: "eventIdentifier-1",
			}
			hash := "hash-1"
			anotherHash := "hash-2"
			found, err := db.GetVersion(key, hash)
			require.NoError(t, err)
			require.Nil(t, found)

			version := &eventschema2.EventSchemaVersionInfo{
				SchemaKey: key,
				Hash:      hash,
			}
			err = db.Set(version, nil, nil)
			require.NoError(t, err)

			found, err = db.GetVersion(key, hash)
			require.NoError(t, err)
			require.Equal(t, version.String(), found.String())

			found, err = db.GetVersion(key, anotherHash)
			require.NoError(t, err)
			require.Nil(t, found)
		})

		t.Run("set then get both", func(t *testing.T) {
			db, _ := newLocalDB(t)
			key := &eventschema2.EventSchemaKey{
				WriteKey:        "writeKey-1",
				EventType:       "eventType-1",
				EventIdentifier: "eventIdentifier-1",
			}
			hash := "hash-1"

			schema := &eventschema2.EventSchemaInfo{
				Key: key,
			}
			version := &eventschema2.EventSchemaVersionInfo{
				SchemaKey: key,
				Hash:      hash,
			}

			err := db.Set(version, schema, nil)
			require.NoError(t, err)

			foundSchema, err := db.GetSchema(key)
			require.NoError(t, err)
			require.Equal(t, schema.String(), foundSchema.String())

			foundVersion, err := db.GetVersion(key, hash)
			require.NoError(t, err)
			require.Equal(t, version.String(), foundVersion.String())
		})
	})

	t.Run("get returns error when an invalid value is stored", func(t *testing.T) {
		db, badgerDB := newLocalDB(t)
		key := &eventschema2.EventSchemaKey{
			WriteKey:        "writeKey-1",
			EventType:       "eventType-1",
			EventIdentifier: "eventIdentifier-1",
		}
		hash := "hash-1"

		err := badgerDB.Update(func(txn *badger.Txn) error {
			if err := txn.Set(db.schemaKeyToBadgerKey(key), []byte("invalid-value")); err != nil {
				return err
			}
			return txn.Set(db.versionKeyToBadgerKey(key, hash), []byte("invalid-value"))
		})
		require.NoError(t, err)

		foundSchema, err := db.GetSchema(key)
		require.Error(t, err)
		require.Nil(t, foundSchema)

		foundVersion, err := db.GetVersion(key, hash)
		require.Error(t, err)
		require.Nil(t, foundVersion)
	})

	t.Run("set returns error when db is closed", func(t *testing.T) {
		db, badgerDB := newLocalDB(t)
		key := &eventschema2.EventSchemaKey{
			WriteKey:        "writeKey-1",
			EventType:       "eventType-1",
			EventIdentifier: "eventIdentifier-1",
		}
		schema := &eventschema2.EventSchemaInfo{
			Key: key,
		}
		require.NoError(t, badgerDB.Close())
		err := db.Set(nil, schema, nil)
		require.Error(t, err)
	})

	t.Run("badger key parsing", func(t *testing.T) {
		var db localDB
		key := &eventschema2.EventSchemaKey{
			WriteKey:        "writeKey-1",
			EventType:       "eventType-1",
			EventIdentifier: "eventIdentifier-1",
		}
		hash := "hash-1"

		t.Run("schema key", func(t *testing.T) {
			parsedKey, err := db.badgerKeyToSchemaKey(db.schemaKeyToBadgerKey(key))
			require.NoError(t, err)
			require.Equal(t, key.String(), parsedKey.String())

			t.Run("invalid prefix", func(t *testing.T) {
				invalidKey := []byte(fmt.Sprintf("%s/%s:%s:%s", versionPrefix, key.WriteKey, key.EventType, key.EventIdentifier))
				parsedKey, err := db.badgerKeyToSchemaKey(invalidKey)
				require.Error(t, err)
				require.Nil(t, parsedKey)
			})
			t.Run("invalid key parts", func(t *testing.T) {
				invalidKey := []byte(fmt.Sprintf("%s/%s:%s:%s/other", schemaPrefix, key.WriteKey, key.EventType, key.EventIdentifier))
				parsedKey, err := db.badgerKeyToSchemaKey(invalidKey)
				require.Error(t, err)
				require.Nil(t, parsedKey)
			})
			t.Run("invalid key format", func(t *testing.T) {
				invalidKey := []byte(fmt.Sprintf("%s/%s:%s:%s:other", schemaPrefix, key.WriteKey, key.EventType, key.EventIdentifier))
				parsedKey, err := db.badgerKeyToSchemaKey(invalidKey)
				require.Error(t, err)
				require.Nil(t, parsedKey)
			})
		})

		t.Run("schema version key", func(t *testing.T) {
			parsedKey, parsedHash, err := db.badgerKeyToVersionKey(db.versionKeyToBadgerKey(key, hash))
			require.NoError(t, err)
			require.Equal(t, key.String(), parsedKey.String())
			require.Equal(t, hash, parsedHash)

			t.Run("invalid prefix", func(t *testing.T) {
				invalidKey := []byte(fmt.Sprintf("%s/%s:%s:%s/%s", schemaPrefix, key.WriteKey, key.EventType, key.EventIdentifier, hash))
				parsedKey, parsedHash, err := db.badgerKeyToVersionKey(invalidKey)
				require.Error(t, err)
				require.Nil(t, parsedKey)
				require.Empty(t, parsedHash)
			})

			t.Run("invalid key parts", func(t *testing.T) {
				invalidKey := []byte(fmt.Sprintf("%s/%s:%s:%s/%s/other", versionPrefix, key.WriteKey, key.EventType, key.EventIdentifier, hash))
				parsedKey, parsedHash, err := db.badgerKeyToVersionKey(invalidKey)
				require.Error(t, err)
				require.Nil(t, parsedKey)
				require.Empty(t, parsedHash)
			})
			t.Run("invalid key format", func(t *testing.T) {
				invalidKey := []byte(fmt.Sprintf("%s/%s:%s:%s:other/%s", versionPrefix, key.WriteKey, key.EventType, key.EventIdentifier, hash))
				parsedKey, parsedHash, err := db.badgerKeyToVersionKey(invalidKey)
				require.Error(t, err)
				require.Nil(t, parsedKey)
				require.Equal(t, parsedHash, hash)
			})
		})
	})
}

func newLocalDB(t *testing.T) (*localDB, *badger.DB) {
	db := testcommons.NewBadgerDB(t)
	t.Cleanup(func() { _ = db.Close() })
	return NewLocalDB(db, memstats.New()), db
}
