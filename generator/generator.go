package main

import (
	"flag"
	"log"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	bootstrapServers = flag.String("bootstrapServers", "localhost:9092", "bootstrap server url")
	inputTopic       = flag.String("inputTopic", "inbound", "input topic name")
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
	testMessage = append(testMessage, make([]byte, *messageSize-len(testMessage))...)
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
					log.Printf("partition %d", ev.TopicPartition.Partition)
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
			keyBytes, _ := GenRandomBytes(64)
			msg := &kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     inputTopic,
					Partition: rand.Int31() % 2,
				},
				Key:   keyBytes,
				Value: testMessage,
			}
			if err := producer.Produce(msg, nil); err != nil {
				errSent++
			} else {
				successSent++
			}
		}
	}
	producer.Flush(10000)
}

func GenRandomBytes(size int) (blk []byte, err error) {
	blk = make([]byte, size)
	_, err = rand.Read(blk)
	return
}
