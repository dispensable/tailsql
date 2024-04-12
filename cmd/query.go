package cmd

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/dispensable/tailsql/sql"
	"github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
)

func parseWinOpt(s string) (*sql.WindowOpt, error) {
	splited := strings.Split(s, ":")
	if len(splited) != 3 {
		return nil, fmt.Errorf("failed to parse %s to winopt, need 3 fields", s)
	}

	size, err := strconv.ParseUint(splited[0], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("failed to parse windows size duration: %s", splited[0])
	}

	slideInterval, err := strconv.ParseInt(splited[1], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("failed to parse slide interval as seconds: %s", splited[1])
	}

	idxOfTs, err := strconv.ParseInt(splited[2], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("failed to parse idx of ts: %s", splited[2])
	}

	var tsExtracF func(sql.LRow) int64
	if idxOfTs >= 0 {
		tsExtracF = func(row sql.LRow) int64 {
			f := row[idxOfTs]
			if v, ok := f.(time.Time); ok {
				return int64(v.Nanosecond())
			} else {
				panic(fmt.Sprintf("idx %d of row is not a date type", idxOfTs))
			}
		}
	}

	return &sql.WindowOpt{
		Size: time.Duration(size * uint64(time.Second)),
		SlidingInterval: time.Duration(slideInterval * int64(time.Second)),
		TsExtractor: tsExtracF,
	}, nil
}

func parseThrottleOpt(s string) (*sql.ThrottlerOpt, error) {
	if s == "" {
		return nil, nil
	}
	splited := strings.Split(s, ":")
	if len(splited) != 3 {
		return nil, fmt.Errorf("failed to parse %s to throttler, need 3 fields", s)
	}

	maxEles, err := strconv.ParseUint(splited[0], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("failed to parse max eles: %s", splited[0])
	}

	interval, err := strconv.ParseInt(splited[1], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("failed to parse throttle interval as seconds: %s", splited[1])
	}

	buffSize, err := strconv.ParseInt(splited[2], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("failed to parse buffsize: %s", splited[2])
	}

	return &sql.ThrottlerOpt{
		MaxEles: int(maxEles),
		Period: time.Duration(interval * int64(time.Second)),
		BuffSize: int(buffSize),
	}, nil
}

func init() {
	queryCmd := &cobra.Command{
		Use: "query",
		Short: "run query on logs",
		Long: `read logs from stdin and parse as table, then run sql query on it`,
		Args: cobra.ExactArgs(1),
	}

	flag := queryCmd.Flags()
	var filesToFollow  *[]string = flag.StringSliceP(
		"files-to-follow", "f", []string{"/dev/stdin"},
		"file paths to follow",
	)
	var regexSlice *[]string = flag.StringSliceP(
		"regexes", "r", []string{},
		"table regex array to extract fields with named capture group syntax",
	)
	var filters *[]string = flag.StringSliceP(
		"filters", "F", []string{},
		"filter to keep when eval to true(sql where syntax)",
	)
	var winOpts *string = flag.StringP(
		"winopt", "w", "",
		"window option for streaming, eg: SIZE:SLIDE:IDX_OF_TS where SIZE is the windows size, unit seconds, SLIDE can be zero means tumbling window, unit seconds, IDX_OF_TS is table col num of ts field. when use join query, you can't use IDX_OF_TS opt unless the schema of thoese table is the same",
	)

	var throttlers *[]string = flag.StringSliceP(
		"throttlers", "t", []string{},
		"throttlers for input speed throttle, eg: MAX_ELE:PERIOD_SEC:BUFF_SIZE where MAX_ELE is the max elements allowd, PERIOD_SEC is time period unit seconds and BUFF_SIZE is the buffer size for throttle",
	)

	// TODO: support different sink
	// may be asciigraph like p95
	var sinkTo *string = flag.StringP(
		"sink", "s", "stdout", "sinkto dst, now support stdout",
	)

	var doNotTail *bool = flag.BoolP(
		"do-not-tail", "T", false, "process from file start not tail -f",
	)

	var dbEngine *string = flag.StringP(
		"db-engine", "d", "duckdb", "db engine for OLAP: sqlite/duckdb/qlbridge",
	)

	var formatter *string = flag.StringP(
		"formatter", "o", "raw", "formatter for result show: raw(just print) rawv(like \\G) table(pretty print as table)",
	)
	
	var logLevel *string = flag.StringP(
		"log-level", "l", "info", "log level: info warn error fatal debug trace",
	)

	queryCmd.RunE = func(cmd *cobra.Command, args []string) error {
		logger := logrus.New()
		logger.SetFormatter(&logrus.TextFormatter{
			FullTimestamp: true,
		})
		logger.SetOutput(os.Stdout)
		l, err := logrus.ParseLevel(*logLevel)
		if err != nil {
			logger.Warnf("parse log level failed, use info level")
		}
		logger.SetLevel(l)

		if len(*regexSlice) != len(*filters) {
			return fmt.Errorf("regex args number must match with filter")
		}

		if len(*filesToFollow) != len(*regexSlice) {
			return fmt.Errorf("files to follow number must match with regex def")
		}
		
		squeryer, err := sql.NewStreamQueryer(*filesToFollow, logger)
		if err != nil {
			logger.Errorf("init stream failed: %s", err)
			return err
		}

		win, err := parseWinOpt(*winOpts)
		if err != nil {
			logger.Errorf("win opt parse failed: %s", err)
			return err
		}

		ths := make([]*sql.ThrottlerOpt, len(*throttlers))
		for tidx, t := range *throttlers {
			if t == "" {
				ths[tidx] = nil
			} else {
				th, err := parseThrottleOpt(t)
				if err != nil {
					logger.Errorf("parse throttle opt failed: %s", err)
					return err
				}
				ths[tidx] = th
			}
		}

		logger.Debugf("win opts: %v", win)
		logger.Infof("Wait for logs to parse and analytics ...")
		squeryer.JoinRun(
			*regexSlice, *filters, win,
			args[0], *sinkTo, *formatter, *dbEngine,
			*doNotTail, ths,
		)
		return nil
	}
	rootCmd.AddCommand(queryCmd)
}
