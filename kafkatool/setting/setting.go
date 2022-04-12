package kafkatool

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/mitchellh/mapstructure"
)

var (
	_config *kafka.ConfigMap
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
	fmt.Println(kafkaConfig)
	_config := kafkaConfig

	fmt.Println("Kafka-Config:", _config)
}

func GetConfig() *kafka.ConfigMap {
	return _config
}
