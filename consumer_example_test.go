package kafkaavro_test

import (
	"log"
	"net/url"

	"github.com/confluentinc/confluent-kafka-go/kafka"

	kafkaavro "github.com/mycujoo/go-kafka-avro"
)

func ExampleNewConsumer() {
	srURL, err := url.Parse("http://localhost:8081")
	if err != nil {
		log.Fatal(err)
	}

	type val struct {
		FieldName string `avro:"field_name"`
	}

	kafkaavro.NewConsumer(
		[]string{"topic1"},
		func(topic string) interface{} {
			return val{}
		},
		kafkaavro.WithKafkaConfig(&kafka.ConfigMap{
			"bootstrap.servers":        "localhost:29092",
			"security.protocol":        "ssl",
			"socket.keepalive.enable":  true,
			"enable.auto.commit":       false,
			"ssl.key.location":         "/path/to/service.key",
			"ssl.certificate.location": "/path/to/service.cert",
			"ssl.ca.location":          "/path/to/ca.pem",
			"group.id":                 "some-group-id",
			"session.timeout.ms":       6000,
			"auto.offset.reset":        "earliest",
		}),
		kafkaavro.WithSchemaRegistryURL(srURL),
	)
}
