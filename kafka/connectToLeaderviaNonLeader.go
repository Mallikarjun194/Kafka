package main

import (
	"github.com/segmentio/kafka-go"
	"net"
	"strconv"
)

func main() {
	conn, err := kafka.Dial("tcp", "localhost:9092")
	if err != nil {
		panic(err.Error())
	}
	defer conn.Close()
	controller, err := conn.Controller()
	if err != nil {
		panic(err.Error())
	}
	var connLeader *kafka.Conn
	connLeader, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		panic(err.Error())
	}
	defer connLeader.Close()
	topicConfigs := []kafka.TopicConfig{
		{Topic: "some-topic",
			NumPartitions:     3,
			ReplicationFactor: 2},
	}

	err = connLeader.CreateTopics(topicConfigs...)
	if err != nil {
		panic(err.Error())
	}
}
