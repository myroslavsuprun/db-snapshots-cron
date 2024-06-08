package config

import (
	"fmt"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

type AppConfig struct {
	DatabaseURL        string
	AWSRegion          string
	AmazonS3Bucket     string
	DumpExpirationDays int
}

func New() (AppConfig, error) {
	os.Setenv("TZ", "UTC")

	err := loadAndValidate()
	if err != nil {
		return AppConfig{}, err
	}

	dumpExpirationDays, err := strconv.Atoi(os.Getenv("DUMP_EXPIRATION_DAYS"))
	if err != nil {
		return AppConfig{}, err
	}

	return AppConfig{
		DatabaseURL:        os.Getenv("DATABASE_URL"),
		AWSRegion:          os.Getenv("AWS_REGION"),
		AmazonS3Bucket:     os.Getenv("AMAZON_S3_BUCKET"),
		DumpExpirationDays: dumpExpirationDays,
	}, nil
}

var vars = []string{
	"DATABASE_URL",
	"AWS_REGION",
	"AMAZON_S3_BUCKET",
	"DUMP_EXPIRATION_DAYS",
}

func loadAndValidate() error {
	err := godotenv.Load()
	if err != nil {
		return err
	}

	for _, v := range vars {
		if _, ok := os.LookupEnv(v); !ok {
			return fmt.Errorf("missing env var %s", v)
		}
	}

	return nil
}
