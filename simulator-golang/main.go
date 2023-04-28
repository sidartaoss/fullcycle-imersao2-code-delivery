package main

import (
	"log"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/joho/godotenv"
	application_kafka "github.com/sidartaoss/imersao2-simulator/application/kafka"
	"github.com/sidartaoss/imersao2-simulator/infra/kafka"
)

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("error .env file %v\n", err)
	}
}

func main() {
	msgChan := make(chan *ckafka.Message)

	consumer := kafka.NewKafkaConsumer(msgChan)
	go consumer.Consume()

	for msg := range msgChan {
		log.Println(string(msg.Value))
		go application_kafka.Produce(msg)
	}

}
