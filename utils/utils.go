package utils

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

func IsPathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
