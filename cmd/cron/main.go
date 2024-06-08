package main

import (
	"context"
	"log/slog"
	"os"

	"db-snapshot/internal/as3"
	appconfig "db-snapshot/internal/config"
	"db-snapshot/internal/dumper"
	"db-snapshot/internal/handler"
	"db-snapshot/internal/tempdir"

	"github.com/robfig/cron/v3"
)

func main() {
	log := logger()

	config, err := appconfig.New()
	if err != nil {
		log.Error("Failed to load config", "error", err)
		os.Exit(1)
	}

	ctx := context.Background()

	tmp := tempdir.New(log)
	err = tmp.Create()
	if err != nil {
		log.Error("Failed to create temp dir", "error", err)
		os.Exit(1)
	}
	defer tmp.Remove()

	s3 := as3.New(log, config)
	err = s3.Init(ctx)
	if err != nil {
		log.Error("Failed to init S3 config", "error", err)
		os.Exit(1)
	}

	dumpInstance := dumper.New(log, config.DatabaseURL)

	c := cron.New()
	c.AddFunc("@every 10m", handler.GetDumpHandler(tmp, log, dumpInstance, c, s3))
	c.AddFunc("@every 1h", handler.GetCleanUpHandler(log, s3, c, config))

	log.Info("Cron job started.")
	c.Run()
}

func logger() *slog.Logger {
	return slog.Default()
}
