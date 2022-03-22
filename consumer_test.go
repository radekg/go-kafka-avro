package kafkaavro_test

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	kafkaavro "github.com/mycujoo/go-kafka-avro/v2"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestConsumer_SubscribeTopics(t *testing.T) {
	kc := &mockKafkaConsumer{}
	srClient := &mockSchemaRegistryClient{}

	c, err := kafkaavro.NewConsumer(
		nil,
		func(topic string) interface{} {
			return nil
		},
		kafkaavro.WithKafkaConsumer(kc),
		kafkaavro.WithSchemaRegistryClient(srClient),
	)
	if err != nil {
		t.Errorf("Create failed: %s", err)
	}

	kc.On("SubscribeTopics", []string{"topic1"}, mock.AnythingOfType("kafka.RebalanceCb")).Return(nil)

	err = c.SubscribeTopics([]string{"topic1"}, nil)
	require.NoError(t, err)
	kc.AssertExpectations(t)
}

type mockKafkaConsumer struct {
	mock.Mock
}

func (m *mockKafkaConsumer) Close() error {
	return nil
}

func (m *mockKafkaConsumer) CommitMessage(msg *kafka.Message) ([]kafka.TopicPartition, error) {
	ret := m.Called(msg)
	return ret.Get(0).([]kafka.TopicPartition), ret.Error(1)
}

func (m *mockKafkaConsumer) SubscribeTopics(topics []string, rebalanceCb kafka.RebalanceCb) (err error) {
	ret := m.Called(topics, rebalanceCb)
	return ret.Error(0)
}

func (m *mockKafkaConsumer) Poll(timeoutMs int) kafka.Event {
	ret := m.Called(timeoutMs)
	return ret.Get(0).(kafka.Event)
}

func (m *mockKafkaConsumer) GetMetadata(topic *string, allTopics bool, timeoutMs int) (*kafka.Metadata, error) {
	ret := m.Called(topic, allTopics, timeoutMs)
	return ret.Get(0).(*kafka.Metadata), ret.Error(1)
}
