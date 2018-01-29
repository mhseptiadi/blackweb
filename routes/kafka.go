package routes

import (
	"github.com/gin-gonic/gin"
	"github.com/mhseptiadi/blackweb/lib/kafka"
)

type QueueContent struct {
	Topic   string `json:"topic" binding:"required"`
	Message string `json:"message" binding:"required"`
}

func Producer(c *gin.Context) {
	message := kafka.Message{}
	c.BindJSON(&message)
	err := kafka.Publish(message)
	c.JSON(200, gin.H{
		"topic":   message.Topic,
		"Content": message.Content,
		"error":   err,
	})
}
