package sql

import (
	"os"
)

func IsNamedPipe(file string) (bool, error) {
	finfo, err := os.Stat(file)
	if err != nil {
		return false, err
	}
	return (finfo.Mode() & os.ModeNamedPipe) != 0, nil
}
