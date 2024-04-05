package cmd

import (
	"bufio"
	"os"

	"github.com/dispensable/tailsql/sink"
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
	var regexForRow *string = flag.StringP("regex", "r", "", "regex to extrace fields from line")
	var maxRows *int = flag.IntP("max-rows", "m", 1000, "max rows to buffer")

	queryCmd.RunE = func(cmd *cobra.Command, args []string) error {
		sqlText := args[0]

		logger := logrus.New()
		logger.SetFormatter(&logrus.TextFormatter{
			FullTimestamp: true,
		})
		logger.SetOutput(os.Stdout)
		tParser, err := sql.NewTableParser("user_table", *regexForRow, logger)
		if err != nil {
			return err
		}

		scanner := bufio.NewScanner(os.Stdin)
		stdSink := sink.StdOutSink{}

		logBuffer := []string{}
		for scanner.Scan() {
			text := scanner.Text()
			logBuffer = append(logBuffer, text)
			if (len(logBuffer) > *maxRows) {
				err := tParser.CreateDB(logBuffer)
				if err != nil {
					return err
				}
				result, err := tParser.RunSql(sqlText)
				if err != nil {
					return err
				}
				err = stdSink.HandelRows(sqlText, result, tParser, logger)
				if err != nil {
					return err
				}
				logBuffer = []string{}
			}
		}

		if (len(logBuffer) > 0) {
			err := tParser.CreateDB(logBuffer)
			if err != nil {
				logger.Errorf("eeee: %s", err)
				return err
			}
			result, err := tParser.RunSql(sqlText)
			if err != nil {
				return err
			}
			err = stdSink.HandelRows(sqlText, result, tParser, logger)
			if err != nil {
				return err
			}
		}

		return nil
	}
	rootCmd.AddCommand(queryCmd)
}
