package sink

import (
	"database/sql"
	"fmt"
	"strings"

	tailSql "github.com/dispensable/tailsql/sql"
	"github.com/sirupsen/logrus"
)

type SqlSink interface {
	HandleRows(sqlText string, rows *sql.Rows, cols []string) error
}

type StdOutSink struct {}

func (s *StdOutSink) HandelRows(sqlText string, rows *sql.Rows, parser *tailSql.TableParser, logger *logrus.Logger) error {
	if rows == nil {
		logger.Warnf("No valide rows parsed in this round, continue ...")
		return nil
	}
	defer rows.Close()
	qcols, err := rows.Columns()
	if err != nil {
		return err
	}

	fmt.Printf(
		"\n\nRun qeury\n%s\n\n on logs:\n (%v)\n\n",
		sqlText,
		strings.Join(qcols, "\t"),
	)

	readCols := make([]interface{}, len(qcols))
	writeCols := make([]string, len(qcols))
	for i := range writeCols {
		readCols[i] = &writeCols[i]
	}
	for rows.Next() {
		rows.Scan(readCols...)
		fmt.Println(strings.Join(writeCols, ", "))
	}
	fmt.Println("")
	return nil
}
