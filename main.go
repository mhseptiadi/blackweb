package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/gin-gonic/gin"
	"github.com/mhseptiadi/blackweb/routes"
	"github.com/mhseptiadi/blackweb/service"
)

func main() {
	service.ConfigInit()
	service.KafkaProducer()
	service.KafkaConsumer()
	service.CouchbaseConn()

	ginChan := make(chan error)
	go func() {
		r := gin.Default()
		r.GET("/ping", func(c *gin.Context) {
			c.JSON(200, gin.H{
				"message": "pong",
			})
		})
		r.GET("/test", routes.Run)
		r.POST("/kafka", routes.Producer)
		ginChan <- r.Run() // listen and serve on 0.0.0.0:8080
	}()

	term := make(chan os.Signal, 1)
	signal.Notify(term, os.Interrupt, syscall.SIGTERM)
	select {
	case err := <-ginChan:
		log.Printf("\nPrgoram exited because: %s", err.Error())
	case <-term:
		log.Print("\nSignal termination detected")
	}
}
