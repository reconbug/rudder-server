package eventschema2_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	eventschema2 "github.com/rudderlabs/rudder-server/event-schema-2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestEventSchemaKey(t *testing.T) {
	t.Run("Marshal and Unmarshal", func(t *testing.T) {
		key := eventschema2.EventSchemaKey{
			WriteKey:        "write-key",
			EventType:       "event-type",
			EventIdentifier: "event-identifier",
		}
		marshalled := key.MustMarshal()
		unmarshalled, err := eventschema2.UnmarshalEventSchemaKey(marshalled)
		require.NoError(t, err)
		require.Equal(t, key.String(), unmarshalled.String())
	})

	t.Run("Unmarshal failure", func(t *testing.T) {
		marshalled := []byte(`invalid`)
		unmarshalled, err := eventschema2.UnmarshalEventSchemaKey(marshalled)
		require.Error(t, err)
		require.Nil(t, unmarshalled)
	})

	t.Run("ToString", func(t *testing.T) {
		key := eventschema2.EventSchemaKey{
			WriteKey:        "write-key",
			EventType:       "event-type",
			EventIdentifier: "event-identifier",
		}
		expected := "write-key:event-type:event-identifier"
		actual := key.ToString()
		require.Equal(t, expected, actual)
	})
}

func TestEventSchemaMessage(t *testing.T) {
	t.Run("Marshal and Unmarshal", func(t *testing.T) {
		msg := eventschema2.EventSchemaMessage{
			WorkspaceID: "workspace-id",
			Key: &eventschema2.EventSchemaKey{
				WriteKey:        "write-key",
				EventType:       "event-type",
				EventIdentifier: "event-identifier",
			},
			Schema:        map[string]string{"key": "string"},
			Sample:        nil,
			ObservedAt:    timestamppb.New(time.Now()),
			CorrelationID: []byte("correlation-id"),
		}
		marshalled := msg.MustMarshal()
		unmarshalled, err := eventschema2.UnmarshalEventSchemaMessage(marshalled)
		require.NoError(t, err)
		require.Equal(t, msg.String(), unmarshalled.String())
	})

	t.Run("Unmarshal failure", func(t *testing.T) {
		marshalled := []byte(`invalid`)
		unmarshalled, err := eventschema2.UnmarshalEventSchemaMessage(marshalled)
		require.Error(t, err)
		require.Nil(t, unmarshalled)
	})

	t.Run("SchemaHash", func(t *testing.T) {
		msg1 := eventschema2.EventSchemaMessage{
			Schema: map[string]string{
				"key":  "string",
				"key2": "string",
				"key3": "string",
			},
		}
		msg2 := eventschema2.EventSchemaMessage{
			Schema: map[string]string{
				"key":  "string",
				"key2": "string",
				"key3": "string",
			},
		}
		require.Len(t, msg1.SchemaHash(), 32)
		require.Equal(t, msg1.SchemaHash(), msg2.SchemaHash())

		t.Run("nil schema", func(t *testing.T) {
			msg3 := eventschema2.EventSchemaMessage{}
			require.Len(t, msg3.SchemaHash(), 32)
			require.Equal(t, "d41d8cd98f00b204e9800998ecf8427e", msg3.SchemaHash())
		})
	})
}

func TestEventSchemaInfo(t *testing.T) {
	t.Run("Marshal and Unmarshal", func(t *testing.T) {
		si := eventschema2.EventSchemaInfo{
			UID:         "uid",
			WorkspaceID: "workspace-id",
			Key: &eventschema2.EventSchemaKey{
				WriteKey:        "write-key",
				EventType:       "event-type",
				EventIdentifier: "event-identifier",
			},
			Schema: map[string]string{"key": "string,number"},
			Counters: &eventschema2.DayCounters{
				Values: []*eventschema2.DayCounter{{
					Day:   timestamppb.New(time.Now()),
					Count: 10,
				}},
			},
			CreatedAt: timestamppb.New(time.Now()),
			LastSeen:  timestamppb.New(time.Now()),
		}
		marshalled := si.MustMarshal()
		unmarshalled, err := eventschema2.UnmarshalEventSchemaInfo(marshalled)
		require.NoError(t, err)
		require.Equal(t, si.String(), unmarshalled.String())
	})

	t.Run("Marshal empty", func(t *testing.T) {
		si := eventschema2.EventSchemaInfo{}
		marshaled := si.MustMarshal()
		si2, err := eventschema2.UnmarshalEventSchemaInfo(marshaled)
		require.NoError(t, err)
		require.Equal(t, si.String(), si2.String())
	})

	t.Run("Unmarshal failure", func(t *testing.T) {
		marshalled := []byte(`invalid`)
		unmarshalled, err := eventschema2.UnmarshalEventSchemaInfo(marshalled)
		require.Error(t, err)
		require.Nil(t, unmarshalled)
	})

	t.Run("merging", func(t *testing.T) {
		t.Run("merging different keys", func(t *testing.T) {
			si := &eventschema2.EventSchemaInfo{}

			v1 := &eventschema2.EventSchemaVersionInfo{
				Schema: map[string]string{
					"key1": "string",
					"key2": "number",
				},
			}
			si.Merge(v1)
			expected := v1.Schema
			require.Equal(t, expected, si.Schema)

			v2 := &eventschema2.EventSchemaVersionInfo{
				Schema: map[string]string{
					"key3": "string",
					"key4": "string",
				},
			}
			si.Merge(v2)
			expected = map[string]string{
				"key1": "string",
				"key2": "number",
				"key3": "string",
				"key4": "string",
			}
			require.Equal(t, expected, si.Schema)
		})

		t.Run("merging same keys", func(t *testing.T) {
			si := &eventschema2.EventSchemaInfo{}

			v1 := &eventschema2.EventSchemaVersionInfo{
				Schema: map[string]string{
					"key1": "string",
					"key2": "number",
				},
			}
			si.Merge(v1)
			expected := v1.Schema
			require.Equal(t, expected, si.Schema)

			si.Merge(v1)
			require.Equal(t, si.Schema, si.Schema)
		})

		t.Run("merging same keys with different types", func(t *testing.T) {
			si := &eventschema2.EventSchemaInfo{}

			v1 := &eventschema2.EventSchemaVersionInfo{
				Schema: map[string]string{
					"key1": "string",
					"key2": "number",
				},
			}
			v2 := &eventschema2.EventSchemaVersionInfo{
				Schema: map[string]string{
					"key2": "string",
					"key3": "number",
				},
			}
			si.Merge(v1, v2)
			expected := map[string]string{
				"key1": "string",
				"key2": "number,string",
				"key3": "number",
			}
			require.Equal(t, expected, si.Schema)
		})
	})

	t.Run("UpdateLastSeen", func(t *testing.T) {
		t.Run("consecutive timestamps", func(t *testing.T) {
			si := &eventschema2.EventSchemaInfo{}
			d1 := time.Now()
			d2 := d1.Add(time.Second)

			si.UpdateLastSeen(d1)
			require.True(t, d1.Equal(si.LastSeen.AsTime()))
			si.UpdateLastSeen(d2)
			require.True(t, d2.Equal(si.LastSeen.AsTime()))
		})

		t.Run("non-consecutive timestamps", func(t *testing.T) {
			si := &eventschema2.EventSchemaInfo{}
			d1 := time.Now()
			d2 := d1.Add(-time.Second)

			si.UpdateLastSeen(d1)
			require.True(t, d1.Equal(si.LastSeen.AsTime()))
			si.UpdateLastSeen(d2)
			require.True(t, d1.Equal(si.LastSeen.AsTime()))
		})
	})
}

func TestEventSchemaVersionInfo(t *testing.T) {
	t.Run("Marshal and Unmarshal", func(t *testing.T) {
		si := eventschema2.EventSchemaVersionInfo{
			UID:         "uid",
			WorkspaceID: "workspace-id",
			SchemaUID:   "schema-uid",
			SchemaKey: &eventschema2.EventSchemaKey{
				WriteKey:        "write-key",
				EventType:       "event-type",
				EventIdentifier: "event-identifier",
			},
			Hash:   "hash",
			Schema: map[string]string{"key": "string,number"},
			Counters: &eventschema2.DayCounters{
				Values: []*eventschema2.DayCounter{{
					Day:   timestamppb.New(time.Now()),
					Count: 10,
				}},
			},
			Sample:    nil,
			FirstSeen: timestamppb.New(time.Now()),
			LastSeen:  timestamppb.New(time.Now()),
		}
		marshalled := si.MustMarshal()
		unmarshalled, err := eventschema2.UnmarshalEventSchemaVersionInfo(marshalled)
		require.NoError(t, err)
		require.Equal(t, si.String(), unmarshalled.String())
	})

	t.Run("Marshal empty", func(t *testing.T) {
		si := eventschema2.EventSchemaVersionInfo{}
		marshaled := si.MustMarshal()
		si2, err := eventschema2.UnmarshalEventSchemaVersionInfo(marshaled)
		require.NoError(t, err)
		require.Equal(t, si.String(), si2.String())
	})

	t.Run("Unmarshal failure", func(t *testing.T) {
		marshalled := []byte(`invalid`)
		unmarshalled, err := eventschema2.UnmarshalEventSchemaVersionInfo(marshalled)
		require.Error(t, err)
		require.Nil(t, unmarshalled)
	})

	t.Run("UpdateLastSeen", func(t *testing.T) {
		t.Run("consecutive timestamps", func(t *testing.T) {
			si := &eventschema2.EventSchemaVersionInfo{}
			d1 := time.Now()
			d2 := d1.Add(time.Second)

			si.UpdateLastSeen(d1)
			require.True(t, d1.Equal(si.LastSeen.AsTime()))
			si.UpdateLastSeen(d2)
			require.True(t, d2.Equal(si.LastSeen.AsTime()))
		})

		t.Run("non-consecutive timestamps", func(t *testing.T) {
			si := &eventschema2.EventSchemaVersionInfo{}
			d1 := time.Now()
			d2 := d1.Add(-time.Second)

			si.UpdateLastSeen(d1)
			require.True(t, d1.Equal(si.LastSeen.AsTime()))
			si.UpdateLastSeen(d2)
			require.True(t, d1.Equal(si.LastSeen.AsTime()))
		})
	})
}

func TestDayCounters(t *testing.T) {
	t.Run("to/from json", func(t *testing.T) {
		today := time.Now()

		v := &eventschema2.DayCounters{}
		v.IncrementCounter(today, 10)

		// marshal and unmarshal to/from protobuf to ensure all struct fields are filled
		marshaled, err := proto.Marshal(v)
		require.NoError(t, err)
		require.NoError(t, proto.Unmarshal(marshaled, v))

		json := string(v.MustProtoJSON())
		require.Equal(t, fmt.Sprintf(`{"values":[{"day":"%s","count":"1"}]}`, today.Round(24*time.Hour).UTC().Format(time.RFC3339)), strings.ReplaceAll(json, " ", ""))

		v2 := &eventschema2.DayCounters{}
		err = protojson.Unmarshal(v.MustProtoJSON(), v2)
		require.NoError(t, err)
		require.Equal(t, v.String(), v2.String())

		t.Run("empty", func(t *testing.T) {
			v := &eventschema2.DayCounters{}
			json := string(v.MustProtoJSON())
			require.Equal(t, "{}", json)
		})
	})

	t.Run("increment today", func(t *testing.T) {
		today := time.Now()

		v := &eventschema2.DayCounters{}
		for i := 0; i < 10; i++ {
			v.IncrementCounter(today, 10)
			require.Len(t, v.Values, 1)
			require.True(t, today.Round(24*time.Hour).Equal(v.Values[0].Day.AsTime()))
			require.EqualValues(t, i+1, v.Values[0].Count)
			require.EqualValues(t, i+1, v.Sum(1))
		}
	})

	t.Run("increment yesterday then today", func(t *testing.T) {
		today := time.Now()
		yesterday := today.Add(-24 * time.Hour)

		v := &eventschema2.DayCounters{}

		v.IncrementCounter(yesterday, 10)
		require.Len(t, v.Values, 1)
		require.True(t, yesterday.Round(24*time.Hour).Equal(v.Values[0].Day.AsTime()))
		require.EqualValues(t, 1, v.Values[0].Count)

		v.IncrementCounter(today, 10)
		require.Len(t, v.Values, 2)
		require.True(t, yesterday.Round(24*time.Hour).Equal(v.Values[0].Day.AsTime()))
		require.True(t, today.Round(24*time.Hour).Equal(v.Values[1].Day.AsTime()))
		require.EqualValues(t, 1, v.Values[0].Count)
		require.EqualValues(t, 1, v.Values[1].Count)

		require.EqualValues(t, 1, v.Sum(1))
		require.EqualValues(t, 2, v.Sum(2))
	})

	t.Run("increment today then yesterday", func(t *testing.T) {
		today := time.Now()
		yesterday := today.Add(-24 * time.Hour)

		v := &eventschema2.DayCounters{}

		v.IncrementCounter(today, 10)
		require.Len(t, v.Values, 1)
		require.True(t, today.Round(24*time.Hour).Equal(v.Values[0].Day.AsTime()))
		require.EqualValues(t, 1, v.Values[0].Count)

		v.IncrementCounter(yesterday, 10)
		v.IncrementCounter(yesterday, 10)
		require.Len(t, v.Values, 2)
		require.True(t, yesterday.Round(24*time.Hour).Equal(v.Values[0].Day.AsTime()))
		require.EqualValues(t, 2, v.Values[0].Count)
		require.True(t, today.Round(24*time.Hour).Equal(v.Values[1].Day.AsTime()))
		require.EqualValues(t, 1, v.Values[1].Count)
	})

	t.Run("increment with gaps between days", func(t *testing.T) {
		today := time.Now()
		yesterday := today.Add(-24 * time.Hour)
		beforeYesterday := yesterday.Add(-24 * time.Hour)

		v := &eventschema2.DayCounters{}

		v.IncrementCounter(beforeYesterday, 10)
		require.Len(t, v.Values, 1)
		require.True(t, beforeYesterday.Round(24*time.Hour).Equal(v.Values[0].Day.AsTime()))
		require.EqualValues(t, 1, v.Values[0].Count)

		v.IncrementCounter(today, 10)
		v.IncrementCounter(today, 10)
		v.IncrementCounter(today, 10)
		require.Len(t, v.Values, 2)
		require.True(t, beforeYesterday.Round(24*time.Hour).Equal(v.Values[0].Day.AsTime()))
		require.EqualValues(t, 1, v.Values[0].Count)
		require.True(t, today.Round(24*time.Hour).Equal(v.Values[1].Day.AsTime()))
		require.EqualValues(t, 3, v.Values[1].Count)

		v.IncrementCounter(yesterday, 10)
		v.IncrementCounter(yesterday, 10)
		require.Len(t, v.Values, 3)
		require.True(t, beforeYesterday.Round(24*time.Hour).Equal(v.Values[0].Day.AsTime()))
		require.EqualValues(t, 1, v.Values[0].Count)
		require.True(t, yesterday.Round(24*time.Hour).Equal(v.Values[1].Day.AsTime()))
		require.EqualValues(t, 2, v.Values[1].Count)
		require.True(t, today.Round(24*time.Hour).Equal(v.Values[2].Day.AsTime()))
		require.EqualValues(t, 3, v.Values[2].Count)

		require.EqualValues(t, 0, v.Sum(0))
		require.EqualValues(t, 3, v.Sum(1))
		require.EqualValues(t, 5, v.Sum(2))
		require.EqualValues(t, 6, v.Sum(3))
		require.EqualValues(t, 6, v.Sum(4))
	})

	t.Run("increment with cleanup", func(t *testing.T) {
		today := time.Now()
		yesterday := today.Add(-24 * time.Hour)

		v := &eventschema2.DayCounters{}

		v.IncrementCounter(yesterday, 1)
		require.Len(t, v.Values, 0, "incrementing a counter for a day that is older than the max number of days should not add a counter")

		v.IncrementCounter(yesterday, 2)
		require.Len(t, v.Values, 1)
		require.True(t, yesterday.Round(24*time.Hour).Equal(v.Values[0].Day.AsTime()))
		require.EqualValues(t, 1, v.Values[0].Count)

		v.IncrementCounter(today, 1)
		require.Len(t, v.Values, 1, "the counter for yesterday should be cleaned up")
		require.True(t, today.Round(24*time.Hour).Equal(v.Values[0].Day.AsTime()))
		require.EqualValues(t, 1, v.Values[0].Count)
	})
}
