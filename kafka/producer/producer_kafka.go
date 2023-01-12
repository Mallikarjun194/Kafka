package main

import (
	"context"
	"github.com/segmentio/kafka-go"
	"log"
	"time"
)

func main() {
	topic := "my-topic"
	partition := 1

	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err = conn.WriteMessages(
		kafka.Message{Value: []byte("Four!")},
		kafka.Message{Value: []byte("Five!")},
		kafka.Message{Value: []byte("Six!")},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	if err := conn.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}

}
