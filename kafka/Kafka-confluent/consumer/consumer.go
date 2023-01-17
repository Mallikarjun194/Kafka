package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
)

func main() {
	fmt.Println("Staring consumer...!")
	//startTime := time.Now()
	kafka_host := os.Getenv("KAFKA_HOSTS")
	//fmt.Printf("start-time: %s", startTime)
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               kafka_host,
		"group.id":                        "MyGroup",
		"enable.auto.commit":              false,
		"auto.offset.reset":               "smallest",
		"go.application.rebalance.enable": true,
		//"auto.commit.interval.ms": 0,
	},
	)
	topic := "confluent-topic-3"
	if err != nil {

	}
	consumer.SubscribeTopics([]string{topic}, nil)
	//defer consumer.Close()

	//consumer.Assign([]kafka.TopicPartition{
	//	{
	//		Partition: 2,
	//		Topic:     &topic,
	//	},
	//})
	//defer consumer.Close()
	msg_count := 1
	run := true
	for run {
		ev := consumer.Poll(100)
		switch e := ev.(type) {
		case *kafka.Message:

			//if msg_count%MIN_COMMIT_COUNT == 0 {
			//	consumer.Commit()
			//}
			fmt.Printf("%% Message on %s:\n%s\n",
				e.TopicPartition, string(e.Value))

			_, err = consumer.Commit()
			if err != nil {
				return
			}
			if msg_count == 10 {
				fmt.Println("msg_count", msg_count)
				//abc := time.Now().Sub(startTime)
				//fmt.Println(abc.String())
				consumer.Close()
				os.Exit(0)
			}

			msg_count += 1

		case kafka.PartitionEOF:
			fmt.Printf("%% Reached %v\n", e)
		case kafka.Error:
			fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			run = false
		default:
			// fmt.Printf("Ignored %v\n", e)
		}
	}
}
