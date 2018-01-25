package routes

import "github.com/gin-gonic/gin"
// import "fmt"

func Run(c *gin.Context) {
	c.JSON(200, gin.H{
		"message": "pong",
	})
}