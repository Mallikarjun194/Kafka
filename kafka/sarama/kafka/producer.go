package kafka

import (
	"github.com/Shopify/sarama"
	"log"
	"time"
)

func (k *kafkaClient) SyncConnector() (syncProducer sarama.SyncProducer, err error) {
	if syncProducer == nil {
		k.kafkaConfig.Producer.Return.Errors = true
		syncProducer, err = sarama.NewSyncProducer(k.kafkaBrokers, k.kafkaConfig)
	}
	return
}

func (k *kafkaClient) SendMessage(topicName, key string, msg string) error {
	var err error
	k.syncProducer, err = k.SyncConnector()
	if err != nil {
		log.Printf("Error while connecting to kafka %s : %s : %s", topicName, key, err.Error())
		return err
	}
	if k.syncProducer != nil {
		defer k.syncProducer.Close()

		if err == nil {
			message := &sarama.ProducerMessage{
				Topic:     topicName,
				Key:       sarama.StringEncoder(key),
				Value:     sarama.StringEncoder(msg),
				Timestamp: time.Now(),
			}

			partition, offset, err := k.syncProducer.SendMessage(message)
			if err != nil {
				log.Printf("Could not save message topic %s to kafka key: %s err: %s", topicName, key, err.Error())
				return err
			}
			log.Printf("Message %v sent successfully to topic %v . Partition : %v , Offset : %v", key, topicName, partition, offset)
		}
	}
	return err
}
