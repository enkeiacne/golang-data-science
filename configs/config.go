package configs

import (
	"github.com/joho/godotenv"
	"os"
)

var (
	AppPort                  = getEnv("APP_PORT", "8080")
	DatabaseHost             = getEnv("DATABASE_HOST", "localhost")
	DatabasePort             = getEnv("DATABASE_PORT", "5432")
	DatabaseUser             = getEnv("DATABASE_USERNAME", "postgres")
	DatabaseName             = getEnv("DATABASE_NAME", "postgres")
	DatabasePassword         = getEnv("DATABASE_PASSWORD", "postgres")
	DatabaseSSL              = getEnv("DATABASE_SSL", "disable")
	DatabaseSecretKey        = getEnv("DATABASE_SECRET_KEY", "your-secret-key")
	JwtSecret                = getEnv("JWT_SECRET", "your-secret-key")
	JwtTTL                   = getEnv("JWT_TTL", "3600")
	RabbitHost               = getEnv("RABBIT_HOST", "localhost")
	RabbitPort               = getEnv("RABBIT_PORT", "5672")
	RabbitUser               = getEnv("RABBIT_USER", "user")
	RabbitPassword           = getEnv("RABBIT_PASSWORD", "password")
	SmtpHost                 = getEnv("SMTP_HOST", "localhost")
	SmtpPort                 = getEnv("SMTP_PORT", "25")
	SmtpUser                 = getEnv("SMTP_USERNAME", "user")
	SmtpPassword             = getEnv("SMTP_PASSWORD", "password")
	TokenTtl                 = getEnv("TOKEN_TTL", "3600")
	FileServerUrl            = getEnv("FILE_SERVER_URL", "localhost:8080/files")
	FileServerUrlUsername    = getEnv("FILE_SERVER_URL_USERNAME", "user")
	FileServerUrlPassword    = getEnv("FILE_SERVER_URL_PASSWORD", "password")
	VoipSignalWireUrlHost    = getEnv("VOIP_SIGNAL_WIRE_URL_HOST", "http://localhost")
	VoipSignalWirePrivateKey = getEnv("VOIP_SIGNAL_WIRE_PRIVATE_KEY", "")
	VoipSignalWirePublicKey  = getEnv("VOIP_SIGNAL_WIRE_PUBLIC_KEY", "")
)

func getEnv(key, fallback string) string {
	LoadEnv()
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
func LoadEnv() {
	if err := godotenv.Load(); err != nil {
		panic("No .env file found")
	}
}
