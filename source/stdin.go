package source

import (
	"bufio"
	"context"
	"os"

	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
)

type StdinSrc struct {
	in chan any
	Ctx *context.Context
}

func NewStdinSrc(ctx context.Context) (*StdinSrc, error) {

	source := &StdinSrc{
		in: make(chan any),
	}
	err := source.init(ctx)
	if err != nil {
		return nil, err
	}
	return source, nil
}

func (s *StdinSrc) init(ctx context.Context) error {
	scanner := bufio.NewScanner(os.Stdin)
	go func() {
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
func (fs *StdinSrc) Via(operator streams.Flow) streams.Flow {
	flow.DoStream(fs, operator)
	return operator
}

// Out returns the output channel of the StdinSrc connector.
func (fs *StdinSrc) Out() <-chan any {
	return fs.in

}
