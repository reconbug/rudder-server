package httpapi

import "time"

type SchemaFilter struct {
	WriteKey string
}

type PageInfo[T any] struct {
	Results     []T  `json:"results,omitempty"`
	CurrentPage int  `json:"currentPage,omitempty"`
	HasNext     bool `json:"hasNext,omitempty"`
}

type Schema struct {
	UID             string            `json:"uid,omitempty"`
	WriteKey        string            `json:"writeKey,omitempty"`
	EventType       string            `json:"eventType,omitempty"`
	EventIdentifier string            `json:"eventIdentifier,omitempty"`
	Schema          map[string]string `json:"schema,omitempty"`
	CreatedAt       time.Time         `json:"createdAt,omitempty"`
	LastSeen        time.Time         `json:"lastSeen,omitempty"`
	Count           int64             `json:"count,omitempty"`
}
type SchemaInfo struct {
	Schema
	LatestVersions []*Version `json:"latestVersions,omitempty"` // last 5 versions
}
type Version struct {
	UID             string            `json:"uid,omitempty"`
	SchemaUID       string            `json:"schemaUid,omitempty"`
	WriteKey        string            `json:"writeKey,omitempty"`
	EventType       string            `json:"eventType,omitempty"`
	EventIdentifier string            `json:"eventIdentifier,omitempty"`
	Schema          map[string]string `json:"schema,omitempty"`
	FirstSeen       time.Time         `json:"createdAt,omitempty"`
	LastSeen        time.Time         `json:"lastSeen,omitempty"`
	Count           int64             `json:"count,omitempty"`
}
type VersionInfo struct {
	Version
	Sample []byte `json:"sample,omitempty"`
}

type HttpError struct {
	StatusCode int
	Message    string
}
