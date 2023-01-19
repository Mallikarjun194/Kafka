package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"golang.org/x/net/context"
	"log"
)

func (k *kafkaClient) KafkaConsumer() (consumerClient sarama.Client, err error) {
	consumerClient, err = sarama.NewClient(k.kafkaBrokers, k.kafkaConfig)
	if err != nil {
		log.Println(err)
	}
	return
}

func (k *kafkaClient) ConsumerGroup(consumerGroupName string) (consumer *sarama.ConsumerGroup, err error) {
	k.Consumer, err = sarama.NewConsumerGroup(k.kafkaBrokers, consumerGroupName, k.kafkaConfig)
	consumer = &k.Consumer
	if err != nil {
		log.Println(err)
		panic(err)
	}
	return
}

func (k *kafkaClient) ReadMessage(topics []string) {
	defer func(Consumer sarama.ConsumerGroup) {
		err := Consumer.Close()
		if err != nil {
			panic(err)
		}
	}(k.Consumer)
	go func() {
		for err := range k.Consumer.Errors() {
			fmt.Println("ERROR", err)
		}
	}()

	ctx := context.Background()
	for {
		handler := ConsumerGroupHandler{}
		err := k.Consumer.Consume(ctx, topics, &handler)
		if err != nil {
			panic(err)
		}
	}
}
