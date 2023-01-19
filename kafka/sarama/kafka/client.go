package kafka

import (
	"github.com/Shopify/sarama"
	"strings"
)

type kafkaClient struct {
	kafkaConfig  *sarama.Config
	kafkaBrokers []string
	syncProducer sarama.SyncProducer
	Consumer     sarama.ConsumerGroup
}

func NewKafkaClient(broker string) IKafkaClient {
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Version = sarama.V0_10_2_0

	//Producer Config
	kafkaConfig.Producer.Partitioner = sarama.NewRandomPartitioner
	kafkaConfig.Producer.Return.Errors = false
	kafkaConfig.Producer.Return.Successes = true
	kafkaConfig.Producer.Retry.Max = 5

	//Consumer Config
	kafkaConfig.ClientID = "kafka-consumer"
	kafkaConfig.Consumer.Return.Errors = false
	kafkaConfig.Consumer.Group.ResetInvalidOffsets = true
	kafkaConfig.Consumer.IsolationLevel = sarama.ReadUncommitted
	kafkaConfig.Consumer.Offsets.AutoCommit.Enable = false
	kafkaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	kafkaConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategySticky}

	return &kafkaClient{
		kafkaConfig:  kafkaConfig,
		kafkaBrokers: strings.Split(broker, ","),
	}
}

type IKafkaClient interface {
	SyncConnector() (syncProducer sarama.SyncProducer, err error)
	SendMessage(topicName, key string, msg string) (err error)
	KafkaConsumer() (consumerClient sarama.Client, err error)
	ConsumerGroup(consumerGroupName string) (consumer *sarama.ConsumerGroup, err error)
	ReadMessage(topics []string)
}
