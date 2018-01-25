package routes

import (
	"github.com/gin-gonic/gin"
	"../lib/kafka"
)

type QueueContent struct {
    Topic string `json:"topic" binding:"required"`
    Message string `json:"message" binding:"required"`
}

func Producer(c *gin.Context) {
	var content QueueContent
	c.BindJSON(&content)

	status,err := kafka.Produce("127.0.0.1:9092,localhost:9092",content.Topic, content.Message)

	c.JSON(200, gin.H{
		"topic":    content.Topic,
		"message":    content.Message,
		"status":    status,
		"error":    err,
	})
}


// func Consumer(c *gin.Context) {
// 	topic := c.DefaultQuery("firstname", "test")

// 	kafka.Consume("127.0.0.1:9092,localhost:9092",topic)

// 	c.JSON(200, gin.H{
// 		"topic":    topic,
// 	})
// }