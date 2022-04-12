package consumer

import (
	"fmt"
	"os"
	"os/signal"
	"reflect"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/covaw/kafka-go/kafkatool/variables"
	"github.com/linkedin/goavro/v2"
)

func Consumer[K any](
	broker string,
	group string,
	topics []string,
	certificate string,
	protocol string,
	timeout int) interface{} {
    var eventType K
	typeOfEvent := reflect.TypeOf(eventType)
	funcSchema, _ := typeOfEvent.MethodByName("Schema")
	inP := make([]reflect.Value, funcSchema.Type.NumIn())
	inP[0] = reflect.ValueOf(eventType)
	eventSchema := funcSchema.Func.Call(inP)[0].Interface().(string)

	var result []byte
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// c, err := kafka.NewConsumer(&kafka.ConfigMap{
	// 	"bootstrap.servers":        broker,
	// 	"broker.address.family":    "v4",
	// 	"group.id":                 group,
	// 	"session.timeout.ms":       6000,
	// 	"security.protocol":        protocol,
	// 	"auto.offset.reset":        "earliest",
	// 	"ssl.certificate.location": certificate})
	fmt.Println(variables.GetConfig())
	// router :=  mux.NewRouter().StrictSlash(true)
	c, err := kafka.NewConsumer(variables.GetConfig())

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Consumer %v\n", c)

	err = c.SubscribeTopics(topics, nil)

	run := true

	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := c.Poll(timeout)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				fmt.Printf("%% Message on %s:\n%s\n",
					e.TopicPartition, string(e.Value))
				if e.Headers != nil {
					fmt.Printf("%% Headers: %v\n", e.Headers)
				}

				result = e.Value
				run = false
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}

	fmt.Printf("Closing consumer\n")
	c.Close()

	codec, errr := goavro.NewCodec(eventSchema)

	if errr != nil {
		fmt.Println(errr)
	}

	decoded, _, errr := codec.NativeFromBinary(result[5:])
	if errr != nil {
		fmt.Println(errr)
	}

	fmt.Println(fmt.Sprintf("%s", decoded))

	return decoded
}
