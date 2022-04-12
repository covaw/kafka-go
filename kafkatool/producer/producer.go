package consumer

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	"github.com/linkedin/goavro/v2"
)

func Producer(
	event interface{},
	broker string,
	topic string,
	certificate string,
	protocol string,
	remitente string,
) bool {
	types := reflect.TypeOf(event)
	funcMarshal, _ := types.MethodByName("MarshalJSON")
	funcSchema, _ := types.MethodByName("Schema")
	inP := make([]reflect.Value, funcMarshal.Type.NumIn())
	inP[0] = reflect.ValueOf(event)
	eventBytes := funcMarshal.Func.Call(inP)[0].Interface().([]byte)
	eventSchema := funcSchema.Func.Call(inP)[0].Interface().(string)

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

	codec, errr := goavro.NewCodec(eventSchema)

	if errr != nil {
		fmt.Println(errr)
	}

	fmt.Println(codec)

	native, _, err := codec.NativeFromTextual(eventBytes)
	if err != nil {
		fmt.Println(err)
	}

	var bin []byte
	bin = append(bin, 0)
	bin = append(bin, 0)
	bin = append(bin, 0)
	bin = append(bin, 0)
	bin = append(bin, 223)
	binary, errr := codec.BinaryFromNative(nil, native)
	if errr != nil {
		fmt.Println(errr)
	}

	for index, element := range binary {
		bin = append(bin, element)
		fmt.Println(index)
	}

	fmt.Println(binary)
	fmt.Println(bin)

	p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          bin,
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
