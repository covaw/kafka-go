package producer

import (
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func Producer(
	event []byte,
	broker string,
	topic string,
	certificate string,
	protocol string,
	remitente string,
) bool {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":        broker,
		"broker.address.family":    "v4",
		"security.protocol":        protocol,
		"ssl.certificate.location": certificate})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Producer %v\n", p)

	deliveryChan := make(chan kafka.Event)

	p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          event,
		Headers:        []kafka.Header{{Key: "remitente", Value: []byte(remitente)}},
	}, deliveryChan)

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
		return false
	} else {
		fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}

	close(deliveryChan)

	return true
}
