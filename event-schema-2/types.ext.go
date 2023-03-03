package eventschema2

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

// ToString representation of the event schema key.
func (sk *EventSchemaKey) ToString() string {
	return fmt.Sprintf("%s:%s:%s", sk.WriteKey, sk.EventType, sk.EventIdentifier)
}

// UnmarshalEventSchemaKey creates a new event schema key from the provided protobuf bytes.
func UnmarshalEventSchemaKey(raw []byte) (*EventSchemaKey, error) {
	p := &EventSchemaKey{}
	if err := proto.Unmarshal(raw, p); err != nil {
		return nil, fmt.Errorf("failed to unmarshal event schema key: %w", err)
	}
	return p, nil
}

// MustMarshal the event schema key into bytes in protobuf format. Panics on error.
func (sk *EventSchemaKey) MustMarshal() []byte {
	m, err := proto.MarshalOptions{}.Marshal(sk)
	if err != nil {
		panic(fmt.Errorf("failed to marshal event schema key: %w", err))
	}
	return m
}

// UnmarshalEventSchemaMessage creates a new event schema message from the provided protobuf bytes.
func UnmarshalEventSchemaMessage(raw []byte) (*EventSchemaMessage, error) {
	p := &EventSchemaMessage{}
	if err := proto.Unmarshal(raw, p); err != nil {
		return nil, fmt.Errorf("failed to unmarshal event schema message: %w", err)
	}
	return p, nil
}

// MustMarshal the event schema message into bytes in protobuf format. Panics on error.
func (sm *EventSchemaMessage) MustMarshal() []byte {
	m, err := proto.MarshalOptions{}.Marshal(sm)
	if err != nil {
		panic(fmt.Errorf("failed to marshal event schema message: %w", err))
	}
	return m
}

// SchemaHash returns a hash of the schema. Keys are sorted lexicographically during hashing.
func (sm *EventSchemaMessage) SchemaHash() string {
	keys := make([]string, 0, len(sm.Schema))
	for k := range sm.Schema {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	for _, k := range keys {
		sb.WriteString(k)
		sb.WriteString(":")
		sb.WriteString(sm.Schema[k])
		sb.WriteString(",")
	}
	schemaHash := misc.GetMD5Hash(sb.String())
	return schemaHash
}

// UnmarshalEventSchemaInfo creates a new event schema info from the provided protobuf bytes.
func UnmarshalEventSchemaInfo(raw []byte) (*EventSchemaInfo, error) {
	p := &EventSchemaInfo{}
	if err := proto.Unmarshal(raw, p); err != nil {
		return nil, fmt.Errorf("failed to unmarshal event schema info: %w", err)
	}
	return p, nil
}

// MustMarshal the event schema info into bytes in protobuf format. Panics on error.
func (si *EventSchemaInfo) MustMarshal() []byte {
	bytes, err := proto.MarshalOptions{}.Marshal(si)
	if err != nil {
		panic(fmt.Errorf("failed to marshal event schema info: %w", err))
	}
	return bytes
}

// Merge the schema of the given versions into the aggregated schema.
func (si *EventSchemaInfo) Merge(versions ...*EventSchemaVersionInfo) {
	if si.Schema == nil {
		si.Schema = make(map[string]string)
	}
	for _, version := range versions {
		for k := range version.Schema {
			t, ok := si.Schema[k]
			if !ok {
				si.Schema[k] = version.Schema[k]
				continue
			}
			if !strings.Contains(t, version.Schema[k]) {
				si.Schema[k] = fmt.Sprintf("%s,%s", t, version.Schema[k])
			}
		}
	}
}

// UpdateLastSeen updates the last seen timestamp of the event schema, as long as the observation timestamp is after the current last seen timestamp.
func (si *EventSchemaInfo) UpdateLastSeen(observedAt time.Time) {
	if si.LastSeen.AsTime().Before(observedAt) {
		si.LastSeen = timestamppb.New(observedAt)
	}
}

// UnmarshalEventSchemaVersionInfo creates a new event schema version info from the provided protobuf bytes.
func UnmarshalEventSchemaVersionInfo(raw []byte) (*EventSchemaVersionInfo, error) {
	p := &EventSchemaVersionInfo{}
	if err := proto.Unmarshal(raw, p); err != nil {
		return nil, fmt.Errorf("failed to unmarshal event schema version: %w", err)
	}
	return p, nil
}

// MustMarshal the event schema version info into bytes in protobuf format. Panics on error.
func (svi *EventSchemaVersionInfo) MustMarshal() []byte {
	bytes, err := proto.MarshalOptions{}.Marshal(svi)
	if err != nil {
		panic(fmt.Errorf("failed to marshal event schema version info: %w", err))
	}
	return bytes
}

// UpdateLastSeen updates the last seen timestamp of the event schema version, as long as the observation timestamp is after the current last seen timestamp.
func (svi *EventSchemaVersionInfo) UpdateLastSeen(observedAt time.Time) {
	if svi.LastSeen.AsTime().Before(observedAt) {
		svi.LastSeen = timestamppb.New(observedAt)
	}
}

// MustProtoJSON returns the day counters as a JSON byte array. Panics on error.
func (dcs *DayCounters) MustProtoJSON() []byte {
	bytes, err := protojson.MarshalOptions{}.Marshal(dcs)
	if err != nil {
		panic(fmt.Errorf("failed to protojson day counters: %w", err))
	}
	return bytes
}

// Increment increments the counter for the requested day by one. If the counter for this day does not exist, it will be created.
// Any counters corresponding to days before [maxDaysToKeep] will be removed.
func (dcs *DayCounters) IncrementCounter(day time.Time, maxDaysToKeep uint) {
	today := dcs.today()
	dcs.cleanupCounters(today, maxDaysToKeep)
	day = day.Round(24 * time.Hour)
	dayCounter := &DayCounter{Day: timestamppb.New(day), Count: 1}

	if day.Before(today.AddDate(0, 0, 1-int(maxDaysToKeep))) {
		return
	}

	if len(dcs.Values) == 0 || dcs.Values[len(dcs.Values)-1].Day.AsTime().Before(day) {
		dcs.Values = append(dcs.Values, dayCounter)
		return
	}
	if day.Equal(today) {
		dcs.Values[len(dcs.Values)-1].Count++
		return
	}
	for i := range dcs.Values {
		if dcs.Values[i].Day.AsTime().Equal(day) {
			dcs.Values[i].Count++
			return
		}
		if dcs.Values[i].Day.AsTime().After(day) {
			dcs.Values = append(dcs.Values[:i], append([]*DayCounter{dayCounter}, dcs.Values[i:]...)...)
			return
		}
	}
}

// Sum returns the sum of the counters for the past [days] days.
func (dcs *DayCounters) Sum(days int) int64 {
	today := dcs.today()
	var sum int64
	for _, counter := range dcs.Values {
		if today.AddDate(0, 0, -days+1).After(counter.Day.AsTime()) {
			continue
		}
		sum += counter.Count
	}
	return sum
}

func (dcs *DayCounters) today() time.Time {
	return time.Now().Round(24 * time.Hour)
}

func (dcs *DayCounters) cleanupCounters(today time.Time, daysToKeep uint) {
	var minIdx int
	for i := range dcs.Values {
		if dcs.Values[i].Day.AsTime().Before(today.AddDate(0, 0, 1-int(daysToKeep))) {
			minIdx = i + 1
		} else {
			break
		}
	}
	if minIdx > 0 {
		dcs.Values = dcs.Values[minIdx:]
	}
}
