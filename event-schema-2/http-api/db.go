package httpapi

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	eventschema2 "github.com/rudderlabs/rudder-server/event-schema-2"
	"google.golang.org/protobuf/encoding/protojson"
)

type DBRepo interface {
	ListSchemas(ctx context.Context, workspaceID string, filter SchemaFilter, page int) (*PageInfo[*Schema], error)
	GetSchema(ctx context.Context, workspaceID, schemaUid string) (*SchemaInfo, error)
	ListVersions(ctx context.Context, workspaceID, schemaUid string, page int) (*PageInfo[*Version], error)
	GetVersion(ctx context.Context, workspaceID, schemaUid, versionUid string) (*VersionInfo, error)
}

type dbRepo struct {
	retentionDays int
	pageSize      int
	db            *sql.DB
}

func (s *dbRepo) ListSchemas(ctx context.Context, workspaceID string, filter SchemaFilter, page int) (*PageInfo[*Schema], error) {
	offset := (page - 1) * s.pageSize
	params := []interface{}{workspaceID, s.pageSize, offset}
	if filter.WriteKey != "" {
		params = append(params, filter.WriteKey)
	}
	var filterPredicate string
	if filter.WriteKey != "" {
		filterPredicate = "AND schema.write_key = $4"
	}
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`SELECT 
			schema.uuid, 
			schema.write_key, 
			schema.event_type, 
			schema.event_identifier, 
			schema.schema, 
			schema.counters, 
			schema.created_at, 
			schema.last_seen 
		FROM event_schema schema 
		WHERE schema.workspace_id = $1 
		%s 
		ORDER BY uuid ASC 
		LIMIT $2 OFFSET $3`, filterPredicate), params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var schemas []*Schema
	for rows.Next() {
		var schema Schema
		var counters eventschema2.DayCounters
		var countersBytes []byte
		var schemaBytes []byte
		if err := rows.Scan(&schema.UID, &schema.WriteKey, &schema.EventType, &schema.EventIdentifier, &schemaBytes, &countersBytes, &schema.CreatedAt, &schema.LastSeen); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(schemaBytes, &schema.Schema); err != nil {
			return nil, err
		}
		if err := protojson.Unmarshal(countersBytes, &counters); err != nil {
			return nil, err
		}
		schema.Count = counters.Sum(s.retentionDays)
		schemas = append(schemas, &schema)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return &PageInfo[*Schema]{
		Results:     schemas,
		CurrentPage: page,
		HasNext:     len(schemas) == s.pageSize,
	}, nil
}

func (s *dbRepo) GetSchema(ctx context.Context, workspaceID, schemaUid string) (*SchemaInfo, error) {
	row := s.db.QueryRow(`SELECT 
			schema.uuid, 
			schema.write_key, 
			schema.event_type, 
			schema.event_identifier, 
			schema.schema, 
			schema.counters, 
			schema.created_at, 
			schema.last_seen 
		FROM event_schema schema 
		WHERE schema.workspace_id = $1 AND schema.uuid = $2`, workspaceID, schemaUid)
	var schema SchemaInfo
	var counters eventschema2.DayCounters
	var countersBytes []byte
	var schemaBytes []byte
	if err := row.Scan(&schema.UID, &schema.WriteKey, &schema.EventType, &schema.EventIdentifier, &schemaBytes, &countersBytes, &schema.CreatedAt, &schema.LastSeen); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	if err := json.Unmarshal(schemaBytes, &schema.Schema.Schema); err != nil {
		return nil, err
	}
	if err := protojson.Unmarshal(countersBytes, &counters); err != nil {
		return nil, err
	}
	schema.Count = counters.Sum(s.retentionDays)

	rows, err := s.db.QueryContext(ctx, `SELECT 
			version.uuid, 
			version.counters, 
			version.first_seen, 
			version.last_seen 
		FROM schema_version version
		WHERE version.schema_uuid = $1 
		ORDER BY version.uuid DESC 
		lIMIT 10`, schemaUid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var version Version
		var counters eventschema2.DayCounters
		var countersBytes []byte
		if err := rows.Scan(&version.UID, &countersBytes, &version.FirstSeen, &version.LastSeen); err != nil {
			return nil, err
		}
		if err := protojson.Unmarshal(countersBytes, &counters); err != nil {
			return nil, err
		}
		version.Count = counters.Sum(s.retentionDays)
		schema.LatestVersions = append(schema.LatestVersions, &version)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return &schema, nil
}

func (s *dbRepo) ListVersions(ctx context.Context, workspaceID, schemaUid string, page int) (*PageInfo[*Version], error) {
	offset := (page - 1) * s.pageSize
	rows, err := s.db.QueryContext(ctx, `SELECT 
			version.uuid, 
			version.schema_uuid, 
			schema.write_key,
			schema.event_type, 
			schema.event_identifier, 
			version.schema, 
			version.counters, 
			version.first_seen, 
			version.last_seen 
		FROM event_schema schema 
		JOIN schema_version version on version.schema_uuid = schema.uuid
		WHERE schema.workspace_id = $1 AND schema.uuid = $2
		ORDER BY uuid DESC 
		LIMIT $3 OFFSET $4`, workspaceID, schemaUid, s.pageSize, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var versions []*Version
	for rows.Next() {
		var version Version
		var counters eventschema2.DayCounters
		var countersBytes []byte
		var schemaBytes []byte
		if err := rows.Scan(&version.UID, &version.SchemaUID, &version.WriteKey, &version.EventType, &version.EventIdentifier, &schemaBytes, &countersBytes, &version.FirstSeen, &version.LastSeen); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(schemaBytes, &version.Schema); err != nil {
			return nil, err
		}
		if err := protojson.Unmarshal(countersBytes, &counters); err != nil {
			return nil, err
		}
		version.Count = counters.Sum(s.retentionDays)
		versions = append(versions, &version)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return &PageInfo[*Version]{
		Results:     versions,
		CurrentPage: page,
		HasNext:     len(versions) == s.pageSize,
	}, nil
}

func (s *dbRepo) GetVersion(ctx context.Context, workspaceID, schemaUid, versionUid string) (*VersionInfo, error) {
	row := s.db.QueryRow(`SELECT 
			version.uuid, 
			version.schema_uuid, 
			schema.write_key,
			schema.event_type, 
			schema.event_identifier, 
			version.schema, 
			version.counters, 
			version.first_seen, 
			version.last_seen,
			version.sample
		FROM event_schema schema 
		JOIN schema_version version on version.schema_uuid = schema.uuid
		WHERE schema.workspace_id = $1 AND schema.uuid = $2 AND version.uuid = $3`, workspaceID, schemaUid, versionUid)
	var version VersionInfo
	var counters eventschema2.DayCounters
	var countersBytes []byte
	var schemaBytes []byte
	var sample sql.NullString
	if err := row.Scan(&version.UID, &version.SchemaUID, &version.WriteKey, &version.EventType, &version.EventIdentifier, &schemaBytes, &countersBytes, &version.FirstSeen, &version.LastSeen, &sample); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	if sample.Valid {
		version.Sample = []byte(sample.String)
	}
	if err := json.Unmarshal(schemaBytes, &version.Schema); err != nil {
		return nil, err
	}
	if err := protojson.Unmarshal(countersBytes, &counters); err != nil {
		return nil, err
	}
	version.Count = counters.Sum(s.retentionDays)
	return &version, nil
}
