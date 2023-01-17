package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
	"strconv"
)

func main() {

	kafka_host := os.Getenv("KAFKA_HOSTS")
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafka_host,
		//"host.docker.internal:9092",
		"client.id": "abc",
		"acks":      "all"})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}
	topic := "confluent-topic-3"
	delivery_chan := make(chan kafka.Event, 10000)
	for i := 1; i <= 50; i++ {
		err = p.Produce(&kafka.Message{
			// Partition: kafka.PartitionAny to choose randomly using range, round-robin
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(strconv.Itoa(i))},
			delivery_chan,
		)
		e := <-delivery_chan
		m := e.(*kafka.Message)

		if m.TopicPartition.Error != nil {
			fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
		} else {
			fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
				*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
		}
	}

	close(delivery_chan)
}
