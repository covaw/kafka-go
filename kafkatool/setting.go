package kafkatool

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	kafkatool "github.com/covaw/kafka-go/kafkatool"
	"github.com/mitchellh/mapstructure"
)

func AddKafka(configuration map[string]interface{}, provider string) {
	if len(provider) == 0 {
		provider := "Kafka"
		fmt.Println(provider)
	}

	var configurations = make(map[string]string)
	mapstructure.Decode(configuration[provider], &configurations)
	kafkaConfig := &kafka.ConfigMap{
		"bootstrap.servers":        configurations["broker"],
		"broker.address.family":    "v4",
		"group.id":                 configurations["group"],
		"session.timeout.ms":       6000,
		"security.protocol":        configurations["protocol"],
		"auto.offset.reset":        "earliest",
		"ssl.certificate.location": configurations["certificate"]}

	kafkatool.SetConfig(kafkaConfig)
}
