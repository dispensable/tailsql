package utils

import (
	"os"
	"syscall"
	"testing"
)

func TestIsNamedPipe(t *testing.T) {
	pipeFile := "/tmp/__test_pipe.pipe"
	err := syscall.Mkfifo(pipeFile, 0666)
	if err != nil {
		t.Fatalf("create %s fifo failed: %s", pipeFile, err)
	}
	defer os.Remove(pipeFile)
	isP, err := IsNamedPipe(pipeFile)
	if err != nil {
		t.Fatalf("check %s pipe failed: %s", pipeFile, err)
	}
	if !isP {
		t.Fatalf("file %s shoule be a namedpipe but got false", pipeFile)
	}

	f, err := os.CreateTemp("", "test_regular_file")
	if err != nil {
		t.Fatalf("craete temp file failed: %s", err)
	}
	defer os.Remove(f.Name())
	isRP, err := IsNamedPipe(f.Name())
	if err != nil || isRP {
		t.Fatalf("check regular file %s, err: %s, is pipe: %t", f.Name(), err, isRP)
	}
}
