package source

import (
	"bufio"
	"context"
	"os"

	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
)

type NamedPipeSrc struct {
	fileName string
	in chan any
	Ctx *context.Context
}

func NewNamedPipeSrc(fileName string, ctx context.Context) (*NamedPipeSrc, error) {
	source := &NamedPipeSrc{
		fileName: fileName,
		in: make(chan any),
	}
	err := source.init(ctx)
	if err != nil {
		return nil, err
	}
	return source, nil
}

func (s *NamedPipeSrc) init(ctx context.Context) error {
	file, err := os.OpenFile(s.fileName, os.O_RDWR, os.ModeNamedPipe)
	if err != nil {
		return err
	}

	go func() {
		scanner := bufio.NewScanner(file)
		defer file.Close()
		for {
			select {
			case <- ctx.Done():
				return
			default:
				if scanner.Scan() {
					s.in <- scanner.Text()
				} else {
					if scanner.Err() != nil {
						return
					}
				}
			}
		}
	}()
	return nil
}

// Via streams data to a specified operator and returns it.
func (fs *NamedPipeSrc) Via(operator streams.Flow) streams.Flow {
	flow.DoStream(fs, operator)
	return operator
}

// Out returns the output channel of the NamedPipeSrc connector.
func (fs *NamedPipeSrc) Out() <-chan any {
	return fs.in

}
