package main

import (
	"flag"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	inputTopic  = "input"
	outputTopic = "output"

	defaultTimeout = time.Second * 10
)

var (
	bootstrapServers = flag.String("bootstrapServers", "localhost:9092", "bootstrap server url")
	groupId          = flag.String("groupId", "test", "consumer group id")
)

func main() {
	flag.Parse()
	cm := &kafka.ConfigMap{
		"bootstrap.servers":        *bootstrapServers,
		"group.id":                 *groupId,
		"auto.offset.reset":        "earliest",
		"enable.auto.offset.store": false,
		"enable.auto.commit":       false,
	}
	consumer, err := kafka.NewConsumer(cm)
	if err != nil {
		log.Fatalf("create consumer %v", err)
	}
	if err := consumer.Subscribe(inputTopic, func(_ *kafka.Consumer, event kafka.Event) error {
		log.Printf("rebalance has happend %v\n", event)
		return nil
	}); err != nil {
		log.Fatalf("subcribe to topic %v", err)
	}
}
