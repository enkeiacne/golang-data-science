package main

import (
	"github.com/gin-gonic/gin"
	"import/configs"
	"import/database"
	leads_module "import/modules/leads-module"
	"log"
)

func main() {
	configs.LoadEnv()
	database.Connect()

	r := gin.Default()
	if err := leads_module.Run(); err != nil {
		log.Fatalln(err)
	}
	if err := r.Run(":" + configs.AppPort); err != nil {
		log.Fatalf("Failed to run server: %v", err)
	}
}
