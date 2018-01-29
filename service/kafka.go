package service

import (
	"log"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/mhseptiadi/blackweb/lib/kafka"
)

var kafkaHost string = "127.0.0.1:9092,localhost:9092"

func KafkaConsumer() {
	brokers := strings.Split(kafkaHost, ",")

	consumerConfig := sarama.NewConfig()
	// consumerConfig.Consumer.
	consumer, err := sarama.NewConsumer(brokers, consumerConfig)
	if err != nil {
		log.Printf("failed to create consumer: %s", err.Error())
	} else {
		kafka.InitCons(&consumer)

		go kafka.Consume("coba", kafka.Coba)
		go kafka.Consume("test", kafka.Test)
	}
}

func KafkaProducer() {
	producerConcifg := sarama.NewConfig()
	producerConcifg.Producer.RequiredAcks = sarama.WaitForAll
	producerConcifg.Producer.Retry.Max = 10
	producerConcifg.Producer.Return.Successes = true
	brokers := strings.Split(kafkaHost, ",")
	producer, err := sarama.NewSyncProducer(brokers, producerConcifg)
	if err != nil {
		log.Printf("failed to create producer: %s", err.Error())
	} else {
		kafka.Init(&producer)
	}
}
