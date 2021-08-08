package main

import (
	"flag"
	"log"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	bootstrapServers = flag.String("bootstrapServers", "localhost:9092", "bootstrap server url")
	inputTopic       = flag.String("inputTopic", "input", "input topic name")
	messageSize      = flag.Int("messageSize", 1<<10, "size of test message")
	keySize          = flag.Int("keySize", 1<<5, "size of test ket")
	messageCount     = flag.Int64("messageCount", 1<<20, "number of messages to generate")
)

func main() {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": *bootstrapServers,
		"log_level":         5,
	})
	if err != nil {
		log.Fatalf("create producer %v", err)
	}
	testMessage := []byte("hello")
	testMessage = append(testMessage, make([]byte, *messageSize - len(testMessage))...)
	testKey := make([]byte, *keySize)
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic: inputTopic,
		},
		Key:   testKey,
		Value: testMessage,
	}
	var (
		total            int64
		successSent      int64
		successDelivered int64
		errDelivery      int64
		errSent          int64
	)
	t := time.NewTicker(time.Second)
	go func() {
		for event := range producer.Events() {
			switch ev := event.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					atomic.AddInt64(&errDelivery, 1)
				} else {
					atomic.AddInt64(&successDelivered, 1)
				}
			}
		}
	}()
	for i := int64(0); i < *messageCount; i++ {
		select {
		case <-t.C:
			log.Printf("total %d success sent %d success delivered %d err sent %d err delivered %d",
				total, successSent, successDelivered, errSent, errDelivery)
		default:
			if err := producer.Produce(msg, nil); err != nil {
				errSent++
			} else {
				successSent++
			}
		}
	}
	producer.Flush(10000)
}
