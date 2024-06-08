package handler

import (
	"db-snapshot/internal/as3"
	"db-snapshot/internal/compressor"
	"db-snapshot/internal/dumper"
	"db-snapshot/internal/tempdir"
	"log/slog"
	"os"

	"github.com/robfig/cron/v3"
)

func GetDumpHandler(
	tmp *tempdir.TempDir,
	log *slog.Logger,
	dump *dumper.Dumper,
	c *cron.Cron,
	s3 *as3.AS3,
) func() {
	failedCount := 0
	return func() {
		log.Info("Creating DB snapshot")
		dumpPath, err := dump.Dump(tmp.Dname)
		if err != nil {
			log.Error("Failed to dump database", "error", err)

			handleFailed(&failedCount, c, log)
			return
		}
		defer os.Remove(dumpPath)

		compr := compressor.New(dumpPath, log)
		compressedPath, err := compr.Compress()
		if err != nil {
			log.Error("Failed to compress file", "error", err)

			handleFailed(&failedCount, c, log)
			return
		}
		defer os.Remove(compressedPath)

		objectKey := as3.GetObjectKeyFromPath(compressedPath)

		err = s3.Upload(compressedPath, objectKey)
		if err != nil {
			log.Error("Failed to upload file to S3", "error", err)

			handleFailed(&failedCount, c, log)
			return
		}

		log.Info("Created DB snapshot", "objKey", objectKey)
		failedCount = 0
	}
}

func handleFailed(fCount *int, c *cron.Cron, log *slog.Logger) {
	*fCount++

	if *fCount >= maxFailed {
		log.Info("Stopping dump cron due to error count exceed", "errors", *fCount)
		c.Stop()
	}
}
