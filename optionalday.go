package kafkaavro

import (
	"encoding/json"
	"time"

	"github.com/pkg/errors"
)

type OptionalDay struct {
	Valid bool
	Time  time.Time
}

func NewOptionalDay(t time.Time, valid bool) OptionalDay {
	return OptionalDay{
		Valid: valid,
		Time:  t,
	}
}

func (od *OptionalDay) FromNative(data interface{}) error {
	// value is null
	if data == nil {
		od.Valid = false
		od.Time = time.Unix(0, 0)
		return nil
	}

	// otherwise it is a record with a field "int"
	m, ok := data.(map[string]interface{})

	if !ok {
		return errors.New("OptionalDay data not a record")
	}

	val, ok := m["int.date"]
	if !ok {
		return errors.New("OptionalDay record missing int field")
	}
	t, ok := val.(time.Time)
	if !ok {
		return errors.New("OptionalDay value type mismatch")
	}

	od.Valid = true
	od.Time = t
	return nil
}

func (od OptionalDay) MarshalJSON() ([]byte, error) {
	if !od.Valid {
		return []byte("null"), nil
	}
	return json.Marshal(od.Time.Format("2006-01-02"))
}
