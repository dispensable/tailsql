package source

import (
	"context"

	"github.com/tenebris-tech/tail"
	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
)

type FileSource struct {
	fileName string
	in chan any
	cfg *tail.Config
	Ctx *context.Context
}

func NewFileSource(fileName string, cfg *tail.Config, ctx context.Context) (*FileSource, error) {
	source := &FileSource{
		fileName: fileName,
		in: make(chan any),
		cfg: cfg,
	}
	err := source.init(ctx)
	if err != nil {
		return nil, err
	}
	return source, nil
}

func (s *FileSource) init(ctx context.Context) error {
	t, err := tail.TailFile(s.fileName, *s.cfg)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case line := <- t.Lines:
				if line == nil {
					continue
				}
				s.in <- line.Text
			case <- ctx.Done():
				t.Cleanup()
				return
			}
		}
	}()
	return nil
}

// Via streams data to a specified operator and returns it.
func (fs *FileSource) Via(operator streams.Flow) streams.Flow {
	flow.DoStream(fs, operator)
	return operator
}

// Out returns the output channel of the FileSource connector.
func (fs *FileSource) Out() <-chan any {
	return fs.in

}
