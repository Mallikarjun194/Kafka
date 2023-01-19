package main

import (
	"Kafka_POC/kafka"
	"flag"
	"github.com/Shopify/sarama"
)

func main() {
	broker := "localhost:9092"
	topic := "EVENTS"
	kafkaClient := kafka.NewKafkaClient(broker)
	quit := flag.Bool("quit", false, "")
	//TestProducer(kafkaClient, topic)
	TestConsumer(kafkaClient, topic, *quit)
}

func TestProducer(client kafka.IKafkaClient, topic string) {
	producer, e := client.SyncConnector()
	if e != nil {
		panic(e)
	}
	defer func(producer sarama.SyncProducer) {
		err := producer.Close()
		if err != nil {

		}
	}(producer)
	for i := 1; i <= 1000; i++ {
		err := client.SendMessage(topic, string(i), "Message "+string(i))
		if err != nil {
			return
		}
	}
}

func TestConsumer(client kafka.IKafkaClient, topic string, quit bool) {
	//consumerClient, e := client.KafkaConsumer()
	//if e != nil {
	//	return
	//}
	//topics, e1 := client.Topics()
	//fmt.Printf("---> %+v", topics)
	//if e1 != nil {
	//	return
	//}
	_, e2 := client.ConsumerGroup("grp2")
	if e2 != nil {
		return
	}
	client.ReadMessage([]string{topic})
}
