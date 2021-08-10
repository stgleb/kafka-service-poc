package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	flushInterval  = time.Second * 10
	defaultTimeout = time.Second
)

var (
	bootstrapServers = flag.String("bootstrapServers", "localhost:9092", "bootstrap server url")
	groupId          = flag.String("groupId", "manualGroupId", "consumer group id")

	inputTopic  = flag.String("inputTopic", "inbound", "input topic name")
	outputTopic = flag.String("outputTopic", "outbound", "output topic name")
)

func monitor(_ context.Context, p *kafka.Producer) {
	for range p.Events() {
		// Skip events to prevent producer queue from oversizing.
	}
}

func pSync(p *kafka.Producer, maxRetries int) {
	start := time.Now()
	defer func() {
		log.Printf("time spent for sync %v", time.Now().Sub(start))
	}()
	for i := 0; i < maxRetries; i++ {
		if remains := p.Flush(int(defaultTimeout/time.Millisecond) * 1 << i); remains != 0 {
			log.Printf("message remains to flush %d, retry", remains)
			continue
		}
		return
	}

	log.Printf("flush was not completed after %d retries", maxRetries)
}

func processor(ctx context.Context, c *kafka.Consumer, p *kafka.Producer) {
	t := time.NewTicker(flushInterval)
	var totalCount int64
	startTime := time.Now()
	for {
		select {
		case <-ctx.Done():
			pSync(p, 5)
		case <-t.C:
			log.Printf("producer queue len: %d", p.Len())
			pSync(p, 5)
			tps, err := c.Commit()
			if err != nil {
				log.Printf("error committing offset %v", err)
				continue
			}
			log.Printf("Commit results:\n")
			for _, tp := range tps {
				log.Printf("\tcommit topic:partition:offset %s %d %d\n", *tp.Topic, tp.Partition, tp.Offset)
			}
			rps := float64(totalCount) / float64((time.Now().Sub(startTime).Milliseconds() / 1000))
			log.Printf("rps %v\n", rps)
		default:
			totalCount++
			msg, err := c.ReadMessage(defaultTimeout)
			if err != nil {
				//log.Printf("error receive message %v", err)
				continue
			}
			// process message
			// TBD
			// produce output message
			outputMsg := &kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic: outputTopic,
				},
				Key:   msg.Key,
				Value: msg.Value,
			}
			// produce message to downstream topic.
			if err := p.Produce(outputMsg, nil); err != nil {
				log.Printf("produce message %v", err)
				continue
			}
			// store offset for message that was read.
			if tps, err := c.StoreOffsets([]kafka.TopicPartition{msg.TopicPartition}); err != nil {
				log.Printf("store offsets %v\n", err)
			} else {
				if len(tps) != 1 {
					log.Fatalln(1)
				}
			}
		}
	}
}

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
	meta, err := consumer.GetMetadata(inputTopic, true, int(defaultTimeout.Milliseconds()))
	if err != nil {
		log.Fatalf("get metadata %v", err)
	}
	var tps []kafka.TopicPartition
	for _, partitionMeta := range meta.Topics[*inputTopic].Partitions {
		tp := kafka.TopicPartition{
			Topic:     inputTopic,
			Partition: partitionMeta.ID,
			Offset:    kafka.OffsetBeginning,
		}
		tps = append(tps, tp)
	}
	if err := consumer.Assign(tps); err != nil {
		log.Fatalf("assign topic partitions %v", err)
	}
	tps, err = consumer.Assignment()
	if err != nil {
		log.Fatalf("error getting list of topic partitions assigned %v", tps)
	}
	for _, tp := range tps {
		log.Printf("topic partitions offset %s %d %d", *tp.Topic, tp.Partition, tp.Offset)
	}
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":      *bootstrapServers,
		"log_level":              5,
		"statistics.interval.ms": 1000,
	})
	if err != nil {
		log.Fatalf("create producer %v", err)
	}

	//testKey := make([]byte, keySize)
	//testMessage := make([]byte, messageSize)
	ctx, cancel := context.WithCancel(context.Background())
	go monitor(ctx, producer)
	go processor(ctx, consumer, producer)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	cancel()
}
