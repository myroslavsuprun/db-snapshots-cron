package dumper

import (
	"fmt"
	"log/slog"
	"os/exec"
	"path/filepath"
	"time"
)

type Dumper struct {
	log    *slog.Logger
	pgConn string
}

func New(log *slog.Logger, pgConn string) *Dumper {
	return &Dumper{
		log:    log,
		pgConn: pgConn,
	}
}

func (d *Dumper) Dump(dir string) (string, error) {
	time := getTime()
	filename := getFilename(time)
	filePath := getFilePath(dir, filename)

	d.log.Info("Dumping database", "file", filePath)

	cmd := exec.Command("pg_dump", d.pgConn, "-f", filePath)
	out, err := cmd.CombinedOutput()
	if string(out) != "" {
		d.log.Info("Dump output", "output", string(out))
	}
	if err != nil {
		return "", err
	}

	return filePath, nil
}

func getFilename(time string) string {
	return fmt.Sprintf("dump-%s.sql", time)
}

func getFilePath(dir, filename string) string {
	return filepath.Join(dir, filename)
}

func getTime() string {
	return time.Now().Format(time.RFC3339)
}
