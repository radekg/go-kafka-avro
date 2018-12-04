package kafkaavro

import (
	"github.com/pkg/errors"
	"gopkg.in/guregu/null.v3"
)

type OptionalInt struct {
	null.Int
}

func NewOptionalInt(i int64, valid bool) OptionalInt {
	return OptionalInt{
		Int: null.NewInt(i, valid),
	}
}

func (i *OptionalInt) FromNative(data interface{}) error {
	// value is null
	if data == nil {
		i.Int.Valid = false
		i.Int.Int64 = 0
		return nil
	}

	// otherwise it is a record with a field "int"
	m, ok := data.(map[string]interface{})

	if !ok {
		return errors.New("OptionalInt data not a record")
	}

	val, ok := m["int"]
	if !ok {
		return errors.New("OptionalInt record missing int field")
	}
	intVal, ok := val.(int32)
	if !ok {
		return errors.New("OptionalInt int type mismatch")
	}

	i.Int.Valid = true
	i.Int.Int64 = int64(intVal)
	return nil
}
