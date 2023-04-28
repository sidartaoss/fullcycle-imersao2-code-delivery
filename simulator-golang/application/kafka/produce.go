package kafka

import (
	"encoding/json"
	"log"
	"os"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/sidartaoss/imersao2-simulator/application/route"
	"github.com/sidartaoss/imersao2-simulator/infra/kafka"
)

func Produce(mesg *ckafka.Message) {
	producer := kafka.NewKafkaProducer()
	r := route.NewRoute()
	err := json.Unmarshal(mesg.Value, &r)
	if err != nil {
		log.Fatalf("error unmarshalling message: %v\n", err)
	}
	r.LoadPositions()
	ps, err := r.ExportJsonPositions()
	if err != nil {
		log.Fatalf("error exporting json positions: %v\n", err)
	}
	for _, p := range ps {
		kafka.Publish(p, os.Getenv("KafkaProduceTopic"), producer)
		time.Sleep(500 * time.Millisecond)
	}
}
