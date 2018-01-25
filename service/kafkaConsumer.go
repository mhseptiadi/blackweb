package service

import (
	"../lib/kafka"
)

func KafkaConsumer(){
	kafka.Consume("127.0.0.1:9092,localhost:9092","test")
	kafka.Consume("127.0.0.1:9092,localhost:9092","coba")

}