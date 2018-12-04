package kafkaavro

import (
	"github.com/pkg/errors"
	"gopkg.in/guregu/null.v3"
)

type OptionalString struct {
	null.String
}

func NewOptionalString(s string, valid bool) OptionalString {
	return OptionalString{
		String: null.NewString(s, valid),
	}
}

func (s *OptionalString) FromNative(data interface{}) error {
	// value is null
	if data == nil {
		s.NullString.Valid = false
		s.NullString.String = ""
		return nil
	}

	// otherwise it is a record with a field "string"
	m, ok := data.(map[string]interface{})

	if !ok {
		return errors.New("OptionalString data not a record")
	}

	val, ok := m["string"]
	if !ok {
		return errors.New("OptionalString record missing string field")
	}
	str, ok := val.(string)
	if !ok {
		return errors.New("OptionalString string type mismatch")
	}

	s.NullString.Valid = true
	s.NullString.String = str
	return nil
}
