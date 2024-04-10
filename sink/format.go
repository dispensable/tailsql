package sink

import (
	"database/sql"
	"fmt"
	"os"
	"strings"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/sirupsen/logrus"
)

type RowsFormatter interface {
	Print(sqlText string, rows *sql.Rows) error
}

type PTableFormatter struct {
	Writer table.Writer
	logger *logrus.Logger
}

func NewPtableFormatter(logger *logrus.Logger) (*PTableFormatter, error) {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)

	return &PTableFormatter{
		Writer: t,
		logger: logger,
	}, nil
}

func (p *PTableFormatter) Print(sqlText string, rows *sql.Rows) error {
	p.Writer.ResetRows()

	if rows == nil {
		return fmt.Errorf("got nil row")
	}
	qcols, err := rows.Columns()
	if err != nil {
		return err
	}

	headerRow := table.Row{}
	for _, h := range qcols {
		headerRow = append(headerRow, h)
	}
	p.Writer.ResetHeaders()
	p.Writer.AppendHeader(headerRow)

	fmt.Printf("Run sql `%s`:\n", sqlText)
	qlen := len(qcols)
	for rows.Next() {
		readCols := make([]interface{}, qlen)
		writeCols := make([]sql.NullString, qlen)
		for i := range writeCols {
			readCols[i] = &writeCols[i]
		}
		err := rows.Scan(readCols...)
		if err != nil {
			p.logger.Errorf("scan rows to cols failed: %s", err)
			return err
		}
		srow := make(table.Row, qlen)
		for j, v := range writeCols {
			if v.Valid {
				srow[j] = fmt.Sprintf("%s", v.String)
			} else {
				srow[j] = "NULL"
			}
		}
		p.Writer.AppendRow(srow)
	}
	p.Writer.Render()
	return nil
}

type RawPrintFormatter struct {
	logger *logrus.Logger
	vertical bool
}

func NewRawPrintFormatter(logger *logrus.Logger, vertical bool) (*RawPrintFormatter, error) {
	return &RawPrintFormatter{
		logger: logger,
		vertical: vertical,
	}, nil
}

func (p *RawPrintFormatter) Print(sqlText string, rows *sql.Rows) error {
	qcols, err := rows.Columns()
	if err != nil {
		return err
	}

	readCols := make([]interface{}, len(qcols))
	writeCols := make([]string, len(qcols))
	for i := range writeCols {
		readCols[i] = &writeCols[i]
	}
	if !p.vertical {
		fmt.Println(strings.Join(qcols, ", "))
		fmt.Print("-------------------------------\n")
	}

	for rows.Next() {
		rows.Scan(readCols...)
		if p.vertical {
			fmt.Println("*********************")
			for idx, col := range qcols {
				fmt.Printf("%s: %s\n", col, writeCols[idx])
			}
			fmt.Println("*********************")			
		} else {
			fmt.Println(strings.Join(writeCols, ", "))
		}
	}
	fmt.Println("")
	return nil
}
