CREATE TABLE IF NOT EXISTS event_schema (
		uuid VARCHAR(27) PRIMARY KEY,
		workspace_id VARCHAR(32) NOT NULL,
        write_key VARCHAR(32) NOT NULL,
		event_type TEXT NOT NULL,
		event_identifier TEXT NOT NULL DEFAULT '',
		schema JSONB NOT NULL,
        counters JSONB NOT NULL,
		created_at TIMESTAMP NOT NULL DEFAULT NOW(),
        last_seen TIMESTAMP NOT NULL DEFAULT NOW());
CREATE INDEX IF NOT EXISTS idx_event_schema_ws_id ON event_schema (workspace_id);
CREATE INDEX IF NOT EXISTS idx_event_schema_wk ON event_schema (write_key);
-- leave 30% of block space unused when inserting data for updates
ALTER TABLE event_schema SET (fillfactor = 70);

CREATE TABLE IF NOT EXISTS schema_version (
		uuid VARCHAR(27) PRIMARY KEY,
        workspace_id VARCHAR(32) NOT NULL,
		schema_uuid VARCHAR(27) NOT NULL,
		schema_hash VARCHAR(32) NOT NULL,
		schema JSONB NOT NULL,
		sample TEXT,
        counters JSONB NOT NULL,
		first_seen TIMESTAMP NOT NULL DEFAULT NOW(),
		last_seen TIMESTAMP NOT NULL DEFAULT NOW());
CREATE INDEX IF NOT EXISTS idx_schema_version_ws_id ON schema_version (workspace_id);
CREATE INDEX IF NOT EXISTS idx_schema_version_s_uuid ON schema_version (schema_uuid);
CREATE INDEX IF NOT EXISTS idx_schema_version_hash ON schema_version (schema_hash);
ALTER TABLE schema_version SET (fillfactor = 70);