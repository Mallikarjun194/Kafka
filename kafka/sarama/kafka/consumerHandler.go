package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"time"
)

type ConsumerGroupHandler struct{}

func (c *ConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	fmt.Printf("Setup : %+v\n", session.Claims())
	return nil
}

func (c *ConsumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (c *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		fmt.Printf("Message:%s topic:%q partition:%d offset:%d\n", string(msg.Value), msg.Topic, msg.Partition, msg.Offset)
		session.MarkMessage(msg, "")
		session.MarkOffset(msg.Topic, msg.Partition, msg.Offset, "")
		session.Commit()
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}
