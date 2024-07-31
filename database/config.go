package database

import (
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"import/configs"
	"import/database/entities"
	"log"
)

var DB *gorm.DB

func Connect() {
	dsn := "host=" + configs.DatabaseHost + " " + "port=" + configs.DatabasePort + " " + "user=" + configs.DatabaseUser + " " + "password=" + configs.DatabasePassword + " " +
		"dbname=" + configs.DatabaseName + " " + "sslmode=" + configs.DatabaseSSL
	db, err := gorm.Open(postgres.New(postgres.Config{
		DSN:                  dsn,
		PreferSimpleProtocol: true,
	}), &gorm.Config{})

	if err != nil {
		panic("failed to connect database error: " + err.Error())
	}
	log.Println("Database connection established")
	if err := db.AutoMigrate(&entities.Lead{}, entities.LeadFileHistory{}); err != nil {
		panic("Database migrate error: " + err.Error())
	}
	DB = db
}
