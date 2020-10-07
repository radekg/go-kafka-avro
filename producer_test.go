package kafkaavro_test

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	kafkaavro "github.com/mycujoo/go-kafka-avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestNewProducer(t *testing.T) {
	kp := &mockKafkaProducer{}

	srClient := &mockSchemaRegistryClient{}

	p, err := kafkaavro.NewProducer(
		"topic",
		`"string"`,
		`"string"`,
		kafkaavro.WithKafkaProducer(kp),
		kafkaavro.WithSchemaRegistryClient(srClient),
	)
	if err != nil {
		t.Fatalf("Error creating producer: %+v", err.Error())
	}

	kp.On("Produce", mock.AnythingOfType("*kafka.Message"), mock.Anything).Return(nil)

	err = p.Produce("key", "value", nil)
	require.NoError(t, err)

	err = p.Produce("key", struct{ ID string }{ID: "id"}, nil)
	require.Error(t, err)
	assert.EqualError(t, err, "avro: struct { ID string } is unsupported for Avro string")

	kp.On("Close").Return()
	p.Close()
}

type mockKafkaProducer struct {
	mock.Mock
}

func (m *mockKafkaProducer) Close() {
	m.Called()
}

func (m *mockKafkaProducer) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	ret := m.Called(msg, deliveryChan)
	go func(deliveryChan chan kafka.Event) {
		if deliveryChan != nil {
			deliveryChan <- &kafka.Message{}
		}
	}(deliveryChan)
	return ret.Error(0)
}
