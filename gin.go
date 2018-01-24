package main

import "github.com/gin-gonic/gin"
import "./routes"

func main() {
	r := gin.Default()
	r.GET("/ping", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "pong",
		})
	})
	r.GET("/test", test.Run)
	r.Run() // listen and serve on 0.0.0.0:8080
}