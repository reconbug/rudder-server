package worker

import (
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	eventschema2 "github.com/rudderlabs/rudder-server/event-schema-2"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/segmentio/ksuid"
)

var ErrMessageExpired = fmt.Errorf("Message is expired")

// Handler processes event schema messages.
type Handler interface {
	// Handle a schema message.
	Handle(msg *eventschema2.EventSchemaMessage) error
}

// NewHandler creates a new event schema message handler. The handler processes event schema messages by updating a local db.
func NewHandler(localDB LocalDB, conf *config.Config) Handler {
	return &handler{
		localDB:       localDB,
		log:           logger.NewLogger().Child("handler"),
		retentionDays: uint(conf.GetInt("RETENTION_DAYS", 30)),
	}
}

type LocalDB interface {
	// GetSchema returns the event schema info for the given key. If the schema info does not exist, it returns nil.
	GetSchema(key *eventschema2.EventSchemaKey) (*eventschema2.EventSchemaInfo, error)

	// GetVersion returns the event schema version for the given key and version hash. If the schema version does not exist, it returns nil.
	GetVersion(key *eventschema2.EventSchemaKey, versionHash string) (*eventschema2.EventSchemaVersionInfo, error)

	// Set sets the event schema and a schema version.
	Set(version *eventschema2.EventSchemaVersionInfo, schema *eventschema2.EventSchemaInfo, correlationId []byte) error
}

type handler struct {
	localDB LocalDB
	log     logger.Logger

	retentionDays uint
}

func (s *handler) Handle(msg *eventschema2.EventSchemaMessage) error {
	if s.expired(msg) {
		return ErrMessageExpired
	}

	schema, err := s.localDB.GetSchema(msg.Key)
	if err != nil {
		return fmt.Errorf("failed to get schema info from local db: %w", err)
	}
	if schema == nil { // new schema & version
		counters := &eventschema2.DayCounters{}
		counters.IncrementCounter(msg.ObservedAt.AsTime(), s.retentionDays)
		schema := &eventschema2.EventSchemaInfo{
			UID:       ksuid.New().String(),
			Key:       msg.Key,
			Schema:    msg.Schema,
			Counters:  counters,
			CreatedAt: msg.ObservedAt,
			LastSeen:  msg.ObservedAt,
		}
		version := s.newVersion(msg, schema)
		return s.set(schema, version, msg.CorrelationID)
	}
	schema.Counters.IncrementCounter(msg.ObservedAt.AsTime(), s.retentionDays)
	schema.UpdateLastSeen(msg.ObservedAt.AsTime())

	version, err := s.localDB.GetVersion(msg.Key, msg.SchemaHash())
	if err != nil {
		return fmt.Errorf("failed to get schema version from local db: %w", err)
	}
	if version == nil { // existing schema & new version
		version := s.newVersion(msg, schema)
		schema.Merge(version)
		return s.set(schema, version, msg.CorrelationID)
	}

	// existing schema & version
	version.UpdateLastSeen(msg.ObservedAt.AsTime())
	version.Counters.IncrementCounter(msg.ObservedAt.AsTime(), s.retentionDays)
	if msg.Sample != nil {
		version.Sample = msg.Sample
	}
	return s.set(schema, version, msg.CorrelationID)
}

func (s *handler) newVersion(msg *eventschema2.EventSchemaMessage, schema *eventschema2.EventSchemaInfo) *eventschema2.EventSchemaVersionInfo {
	version := &eventschema2.EventSchemaVersionInfo{
		UID:       ksuid.New().String(),
		Hash:      msg.SchemaHash(),
		SchemaUID: schema.UID,
		SchemaKey: schema.Key,
		Schema:    msg.Schema,
		Sample:    msg.Sample,
		Counters:  &eventschema2.DayCounters{},
		FirstSeen: msg.ObservedAt,
		LastSeen:  msg.ObservedAt,
	}
	version.Counters.IncrementCounter(msg.ObservedAt.AsTime(), s.retentionDays)
	return version
}

func (s *handler) set(schema *eventschema2.EventSchemaInfo, version *eventschema2.EventSchemaVersionInfo, correlationId []byte) error {
	if err := s.localDB.Set(version, schema, correlationId); err != nil {
		return fmt.Errorf("failed to set schema version and info to local db: %w", err)
	}
	return nil
}

func (s *handler) expired(msg *eventschema2.EventSchemaMessage) bool {
	minDate := time.Now().Round(24*time.Hour).AddDate(0, 0, 1-int(s.retentionDays))
	return msg.ObservedAt.AsTime().Round(24 * time.Hour).Before(minDate)
}
