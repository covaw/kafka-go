package main

import "github.com/confluentinc/confluent-kafka-go/kafka"

var (
	_config *kafka.ConfigMap
)

func GetConfig() *kafka.ConfigMap {
	return _config
}

func SetConfig(config *kafka.ConfigMap) {
	_config := config
}
