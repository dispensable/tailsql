package cmd

import (
	"os"

	"github.com/dispensable/tailsql/config"
	"github.com/dispensable/tailsql/sql"
	"github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
)

func init() {
	queryCmd := &cobra.Command{
		Use: "query",
		Short: "run query on logs",
		Long: `read logs from stdin and parse as table, then run sql query on it`,
		Args: cobra.ExactArgs(1),
	}

	flag := queryCmd.Flags()
	var configFile *string = flag.StringP(
		"config", "c", "", "config file for tailsql",
	)
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
	var winOpt *string = flag.StringP(
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
		"sink", "s", "", "sinkto dst, now support stdout",
	)

	var doNotTail *bool = flag.BoolP(
		"do-not-tail", "T", false, "process from file start not tail -f",
	)

	var dbEngine *string = flag.StringP(
		"db-engine", "d", "", "db engine for OLAP: sqlite/duckdb/qlbridge",
	)

	var formatter *string = flag.StringP(
		"formatter", "o", "", "formatter for result show: raw(just print) rawv(like \\G) table(pretty print as table)",
	)

	var logLevel *string = flag.StringP(
		"log-level", "l", "", "log level: info warn error fatal debug trace",
	)

	queryCmd.RunE = func(cmd *cobra.Command, args []string) error {
		var cfg *config.TailSqlCfg
		var err error
		if *configFile != "" {
			// parse config
			cfg, err = config.NewCfgFromFile(*configFile)
			if err != nil {
				return err
			}
			// allow user override thoes cfgs when pass -c
			if *formatter != "" {
				cfg.Sink.Formatter = *formatter
			}
			if *dbEngine != "" {
				cfg.DBEngine = *dbEngine
			}
			if *sinkTo != "" {
				cfg.Sink.To = *sinkTo
			}
			if *logLevel != "" {
				cfg.LoggerCfg.Level = *logLevel
			}
		} else {
			cfg, err = config.NewCfg(
				*filesToFollow, *regexSlice, *filters,
				*winOpt, *throttlers, *sinkTo, *doNotTail,
				*dbEngine, *formatter, *logLevel,
			)
		}
		
		// update args to cfg
		logger := logrus.New()
		logger.SetFormatter(&logrus.TextFormatter{
			FullTimestamp: true,
		})
		logger.SetOutput(os.Stdout)

		l, err := logrus.ParseLevel(cfg.LoggerCfg.Level)
		if err != nil {
			logger.Warnf("parse log level failed, use info level")
		}
		logger.SetLevel(l)
		logger.Debugf("cfg: %+v", cfg)

		queryer, err := sql.NewStreamQueryerFromCfg(cfg, logger)
		if err != nil {
			logger.Errorf("create queryer failed: %s", err)
			return err
		}
		err = queryer.RunAnalysisFromCfg(args[0])
		if err != nil {
			logger.Errorf("run analysis job failed: %s", err)
		}
		return err
	}
	rootCmd.AddCommand(queryCmd)
}
