package sink

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"
)

type SqlSink interface {
	HandleRows(sqlText string, rows *sql.Rows, cols []string) error
}

type StdOutSink struct {}

func (s *StdOutSink) HandelRows(sqlText string, rows *sql.Rows, logger *logrus.Logger) error {
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

type StdOutSqlRowsSink struct {
	in chan any
	sql string
	logger *logrus.Logger
	formatter RowsFormatter
}

func NewStdOutSqlRowsSink(sqlText string, formatter RowsFormatter,
	logger *logrus.Logger) *StdOutSqlRowsSink {
	sink := &StdOutSqlRowsSink{
		in: make(chan any),
		sql: sqlText,
		logger: logger,
		formatter: formatter,
	}
	sink.init()

	return sink
}

func (s *StdOutSqlRowsSink) init() {
	go func() {
		for v := range s.in {
			if v == nil {
				s.logger.Warnf("get nil from upstream")
				continue
			}
			s.logger.Infof(">> query result <<\n")
			s.logger.Debugf("got msg: %v", v)
			rows, ok := v.(*sql.Rows)
			if !ok {
				s.logger.Errorf("wrong result returned: %v\n", rows)
				continue
			}
			defer rows.Close()
			err := s.formatter.Print(s.sql, rows)
			if err != nil {
				s.logger.Errorf("print rows failed: %s", err)
			}
		}
	}()
	s.logger.Debugf("inited")
}

func (s *StdOutSqlRowsSink) In() chan<- any {
	return s.in
}
