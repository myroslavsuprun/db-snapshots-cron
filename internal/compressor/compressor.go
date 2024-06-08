package compressor

import (
	"compress/gzip"
	"fmt"
	"io"
	"log/slog"
	"os"
)

type Compressor struct {
	log      *slog.Logger
	filePath string
}

func New(filePath string, log *slog.Logger) *Compressor {
	return &Compressor{
		log:      log,
		filePath: filePath,
	}
}

func (c *Compressor) Compress() (string, error) {
	f, err := os.Open(c.filePath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	filePathGzip := getCompressedPath(c.filePath)
	fGzip, err := os.Create(filePathGzip)
	if err != nil {
		return "", nil
	}
	defer fGzip.Close()

	gzipWriter := gzip.NewWriter(fGzip)
	defer gzipWriter.Close()

	_, err = io.Copy(gzipWriter, f)
	if err != nil {
		return "", err
	}

	gzipWriter.Flush()

	return filePathGzip, nil
}

func getCompressedPath(filePath string) string {
	return fmt.Sprintf("%s.gzip", filePath)

}
