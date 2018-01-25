package kafka

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func Produce(broker string, topic string, message string) (status bool, err error){

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": broker})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		return false, err
	}else{

		fmt.Printf("Created Producer %v\n", p)

		doneChan := make(chan bool)

		go func() {
			defer close(doneChan)
			for e := range p.Events() {
				switch ev := e.(type) {
				case *kafka.Message:
					m := ev
					if m.TopicPartition.Error != nil {
						fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
					} else {
						fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
							*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
					}
					return

				default:
					fmt.Printf("Ignored event: %s\n", ev)
				}
			}
		}()

		p.ProduceChannel() <- &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}, Value: []byte(message)}

		// wait for delivery report goroutine to finish
		_ = <-doneChan
	}
	p.Close()
	return true, nil
}

func Consume(broker string, topic string) (status bool, err error){

	sigchan := make(chan bool)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               broker,
		"group.id":                        topic,
		"session.timeout.ms":              6000,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
		"default.topic.config":            kafka.ConfigMap{"auto.offset.reset": "earliest"}})

	if err != nil {
		fmt.Printf("Failed to create consumer: %s\n", err)
		return false, err
	}else{
		fmt.Printf("Created Consumer %v\n", c)
		topics := []string{topic}
		err = c.SubscribeTopics(topics, nil)

		run := true

		for run == true {
			select {
			case sig := <-sigchan:
				fmt.Printf("Caught signal %v: terminating\n", sig)
				run = false

			case ev := <-c.Events():
				switch e := ev.(type) {
				case kafka.AssignedPartitions:
					fmt.Printf("%% %v\n", e)
					c.Assign(e.Partitions)
				case kafka.RevokedPartitions:
					fmt.Printf("%% %v\n", e)
					c.Unassign()
				case *kafka.Message:
					fmt.Printf("%% Message on %s:\n%s\n",
						e.TopicPartition, string(e.Value))
				case kafka.PartitionEOF:
					fmt.Printf("%% Reached %v\n", e)
				case kafka.Error:
					fmt.Printf("%% Error: %v\n", e)
					run = false
				}
			}
		}

		fmt.Printf("Closing consumer\n")
	}
	c.Close()
	return true, nil
}