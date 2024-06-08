package handler

import (
	"db-snapshot/internal/as3"
	"db-snapshot/internal/config"
	"log/slog"
	"time"

	"github.com/robfig/cron/v3"
)

func GetCleanUpHandler(
	log *slog.Logger,
	s3 *as3.AS3,
	c *cron.Cron,
	appConf config.AppConfig,
) func() {
	var failCount int
	return func() {
		deleteBefore := time.Now().Add(-time.Hour * 24 * time.Duration(appConf.DumpExpirationDays))
		log.Info("Cleaning up S3", "bofore", deleteBefore.Format(time.RFC3339))

		err := s3.CleanUp(deleteBefore)
		if err != nil {
			log.Error("Failed to cleanup S3", "error", err)

			failCount++
			if failCount >= maxFailed {
				log.Info("Stopping clean up cron due to error threshold exceed", "errors", failCount)
				c.Stop()
			}
			return
		}

		log.Info("Cleaned up S3")
		failCount = 0
	}
}
