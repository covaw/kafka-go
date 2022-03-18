package producer

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
)

func Producer(
	event []byte,
	// schema string,
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

	id := uuid.New()
	uuid := strings.Replace(id.String(), "-", "", -1)
	byteId, err := json.Marshal(uuid)
	fmt.Println(byteId)

	// var bin []byte
	// bin = append(bin, 0)
	// bin = append(bin, 0)
	// bin = append(bin, 0)
	// bin = append(bin, 0)
	// bin = append(bin, 223)

	// codec, errr := goavro.NewCodec(schema)

	// if errr != nil {
	// 	fmt.Println(errr)
	// }

	// fmt.Println(codec)

	// native, _, err := codec.NativeFromTextual(event)
	// if err != nil {
	// 	fmt.Println(err)
	// }

	// binary, errr := codec.BinaryFromNative(nil, native)
	// if errr != nil {
	// 	fmt.Println(errr)
	// }

	// for index, element := range binary {
	// 	bin = append(bin, element)
	// 	fmt.Println(index)
	// }

	// fmt.Println("binary: ", binary)
	// fmt.Println("bin: ", bin)

	p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          event,
		Key:            byteId,
		Timestamp:      time.Time{},
		TimestampType:  0,
		Opaque:         nil,
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
