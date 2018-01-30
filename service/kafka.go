package service

import (
	"log"

	"github.com/Shopify/sarama"
	"github.com/mhseptiadi/blackweb/lib/kafka"
	"github.com/spf13/viper"
)

func KafkaConsumer() {
	brokers := viper.GetStringSlice("kafka.host")

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
	brokers := viper.GetStringSlice("kafka.host")

	producerConcifg := sarama.NewConfig()
	producerConcifg.Producer.RequiredAcks = sarama.WaitForAll
	producerConcifg.Producer.Retry.Max = 10
	producerConcifg.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(brokers, producerConcifg)
	if err != nil {
		log.Printf("failed to create producer: %s", err.Error())
	} else {
		kafka.Init(&producer)
	}
}
