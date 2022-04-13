package kafkatool

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/mitchellh/mapstructure"
)

type Config[K any] struct {
	cfg *kafka.ConfigMap
}

func (k *Config[K]) AddKafka(configuration map[string]interface{}, provider string) {
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
	k.cfg = kafkaConfig

	fmt.Println("Kafka-Config:", k.cfg)
}

func (k *Config[K]) GetConfig() *kafka.ConfigMap {
	return k.cfg
}
