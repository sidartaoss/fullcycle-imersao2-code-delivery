package kafka

import (
	"log"
	"os"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaConsumer struct {
	MsgChan chan *ckafka.Message
}

func NewKafkaConsumer(msgChan chan *ckafka.Message) *KafkaConsumer {
	return &KafkaConsumer{
		MsgChan: msgChan,
	}
}

func (kc *KafkaConsumer) Consume() {
	configMap := &ckafka.ConfigMap{
		"bootstrap.servers": os.Getenv("KafkaBootstrapServers"),
		"group.id":          os.Getenv("KafkaConsumerGroupId"),
		"security.protocol": os.Getenv("security.protocol"),
		"sasl.mechanisms":   os.Getenv("sasl.mechanisms"),
		"sasl.username":     os.Getenv("sasl.username"),
		"sasl.password":     os.Getenv("sasl.password"),
	}
	c, err := ckafka.NewConsumer(configMap)
	if err != nil {
		log.Fatalf("error consuming kafka message: %v\n", err)
	}
	topics := []string{os.Getenv("KafkaReadTopic")}
	err = c.SubscribeTopics(topics, nil)
	if err != nil {
		log.Fatalf("error subscribing topic: %v\n", err)
	}
	log.Println("Kafka consumer has been started")
	for {
		msg, err := c.ReadMessage(-1)
		if err != nil {
			log.Fatalf("error for consumer reading message: %v\n", err)
		}
		log.Println("sending message to go channel (KafkaConsumer.MsgChan)")
		kc.MsgChan <- msg
	}
}
