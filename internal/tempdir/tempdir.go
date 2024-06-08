package tempdir

import (
	"log/slog"
	"os"
	"syscall"
)

const tmpDir = "./tmp"

type TempDir struct {
	log   *slog.Logger
	Dname string
}

func New(log *slog.Logger) *TempDir {
	return &TempDir{
		log: log,
	}
}

func (td *TempDir) Create() error {
	err := os.RemoveAll(tmpDir)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	syscall.Umask(0)
	err = os.Mkdir(tmpDir, 0777)
	if err != nil && !os.IsExist(err) {
		return err
	}

	dname, err := os.MkdirTemp(tmpDir, "*")
	if err != nil {
		return err
	}

	td.Dname = dname

	return nil
}

func (td *TempDir) Remove() error {
	return os.RemoveAll(td.Dname)
}
