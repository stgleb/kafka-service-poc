package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	flushInterval  = time.Second * 10
	defaultTimeout = time.Second * 10
)

var (
	bootstrapServers = flag.String("bootstrapServers", "localhost:9092", "bootstrap server url")
	groupId          = flag.String("groupId", "test", "consumer group id")

	inputTopic  = flag.String("inputTopic", "input", "input topic name")
	outputTopic = flag.String("outputTopic", "output", "output topic name")

	keySize     = 1 << 4
	messageSize = 1 << 10
)

func monitor(ctx context.Context, p *kafka.Producer) {
	for event := range p.Events() {
		select {
		case <-ctx.Done():
			return
		default:
			switch e := event.(type) {
			case *kafka.Stats:
				// Stats events are emitted as JSON (as string).
				// Either directly forward the JSON to your
				// statistics collector, or convert it to a
				// map to extract fields of interest.
				// The definition of the statistics JSON
				// object can be found here:
				// https://github.com/edenhill/librdkafka/blob/master/STATISTICS.md
				var stats map[string]interface{}
				if err := json.Unmarshal([]byte(e.String()), &stats); err != nil {
					log.Printf("unmarshall %v", err)
				}
				fmt.Printf("Stats:"+
					"\t%v messages (%v bytes) messages consumed\n"+
					"\t%v producer queue len %v producer queue bytes len\n",
					stats["rxmsgs"], stats["rxmsg_bytes"], stats["msg_cnt"], stats["msg_size"])
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}

	}
}

func pSync(p *kafka.Producer, maxRetries int) {
	for i := 0; i < maxRetries; i++ {
		if remains := p.Flush(int(defaultTimeout / time.Millisecond)); remains != 0 {
			log.Printf("message remains to flush %d, retry", remains)
			continue
		}
		return
	}
	log.Printf("flush was not completed agter %d retries", maxRetries)
}

func processor(ctx context.Context, c *kafka.Consumer, p *kafka.Producer) {
	t := time.NewTicker(flushInterval)
	for {
		select {
		case <-ctx.Done():
			pSync(p, 3)
		case <-t.C:
			pSync(p, 3)
			tps, err := c.Commit()
			if err != nil {
				log.Printf("error commitig offset %v", err)
				continue
			}
			log.Printf("Commit results:\n")
			for _, tp := range tps {
				log.Printf("\tcommit topc:partition:offset %s %d %d\n", *tp.Topic, tp.Partition, tp.Offset)
			}
		default:
			msg, err := c.ReadMessage(defaultTimeout)
			if err != nil {
				log.Printf("error receive message %v", err)
				continue
			}
			// process message
			// TBD
			// produce output message
			outputMsg := &kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic: inputTopic,
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
			if _, err := c.StoreOffsets([]kafka.TopicPartition{msg.TopicPartition}); err != nil {
				log.Printf("store offsets %v\n", err)
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
	if err := consumer.Subscribe(*inputTopic, func(_ *kafka.Consumer, event kafka.Event) error {
		log.Printf("rebalance has happend %v\n", event)
		return nil
	}); err != nil {
		log.Fatalf("subcribe to topic %v", err)
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
