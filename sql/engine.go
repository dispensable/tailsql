package sql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/araddon/qlbridge/datasource/membtree"
	"github.com/araddon/qlbridge/expr/builtins"
	"github.com/araddon/qlbridge/lex"
	"github.com/araddon/qlbridge/schema"
	"github.com/sirupsen/logrus"
	_ "modernc.org/sqlite"
)

type SQLEngine interface {
	// run sql query on db
	Query(string) (*sql.Rows, error)
	// insert rows to db
	Insert(string, []LRow) error
	// drop exists data
	Clean() error
	// close db
	Close() error
}

type SQLiteEngine struct {
	LineParsers map[string]*LineParser
	DB *sql.DB
	logger *logrus.Logger
}

type QLMemDBEngine struct {
	DS *membtree.StaticDataSource
	logger *logrus.Logger
	dsn string
	cols []string
	DB *sql.DB
}

func NewSQLiteEngine(lparsers map[string]*LineParser, logger *logrus.Logger) (*SQLiteEngine, error) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		return nil, err
	}

	// first we create db/tables per line parser
	for tname, lp := range lparsers {
		logger.Debugf("create table %s: %s", tname, lp.Tschema)
		r, err := db.ExecContext(
			context.Background(),
			lp.Tschema,
		)
		if err != nil {
			logger.Errorf("create table schema %s err: %s", lp.Tschema, err)
			return nil, err
		}
		logger.Debugf("create schema result: %s", r)
	}

	return &SQLiteEngine{
		LineParsers: lparsers,
		DB: db,
		logger: logger,
	}, nil
}

func (e *SQLiteEngine) Insert(tname string, rawRows []LRow) error {
	e.logger.Debugf("sqlite: insert %s to %s", rawRows, tname)
	// prepare some meta info
	cols := e.LineParsers[tname].Cols
	vsmt := []string{}
	ecolsIdx := len(cols) - 2
	for i := 0; i < len(cols) - 2; i++ {
		vsmt = append(vsmt, "?")
	}

	insertSmt := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s);",
		tname, strings.Join(cols[0:ecolsIdx], ", "),
		strings.Join(vsmt, ", "),
	)
	smt, err := e.DB.Prepare(insertSmt)
	if err != nil {
		e.logger.Errorf("prepare smterr: %s", err)
		return err
	}
	for _, r := range rawRows {
		rany := make([]any, ecolsIdx)
		for idx, v := range r[:ecolsIdx] {
			rany[idx] = v
		}
		result, err := smt.Exec(
			rany...,
		)
		if err != nil {
			e.logger.Debugf("INSERT data err: %s", err)
			return err
		}
		rowsEffected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		e.logger.Debugf("insert succ, rows effected: %d", rowsEffected)
	}
	return nil
}

func (e *SQLiteEngine) Query(sqlText string) (*sql.Rows, error) {
	return e.DB.Query(sqlText)
}

func (e *SQLiteEngine) Clean() error {
	e.logger.Debugf("run clean task on sqlite")
	for tname := range e.LineParsers {
		_, err := e.DB.ExecContext(
			context.Background(),
			fmt.Sprintf("DELETE FROM %s", tname),
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *SQLiteEngine) Close() error {
	e.logger.Debugf("close sqllite")
	return e.DB.Close()
}

func NewQLMembtreeDSEngine(ds *membtree.StaticDataSource, tname string, cols []string, logger *logrus.Logger) (*QLMemDBEngine, error) {
	err := schema.RegisterSourceAsSchema(tname, ds)
	if err != nil {
		logger.Errorf("registe schema err: %s", err)
		return nil, err
	}

	db, err := sql.Open("qlbridge", tname)
	if err != nil {
		logger.Errorf("open %s db failed: %s", tname, err)
		return nil, err
	}

	return &QLMemDBEngine{
		DS: ds,
		logger: logger,
		dsn: tname,
		DB: db,
		cols: cols,
	}, nil
}

func (q *QLMemDBEngine) Query(sqlText string) (*sql.Rows, error) {
	builtins.LoadAllBuiltins()
	return q.DB.Query(sqlText)
}

func (q *QLMemDBEngine) Insert(tname string, rows []LRow) error {
	q.DS = membtree.NewStaticDataSource(tname, len(q.cols)-1, rows, q.cols)
	return schema.RegisterSourceAsSchema(tname, q.DS)
}

func (q *QLMemDBEngine) Clean() error {
	err := q.DS.Close()
	if err != nil {
		return err
	}
	return schema.DefaultRegistry().SchemaDrop(q.dsn, q.dsn, lex.TokenSchema)
}

func (q *QLMemDBEngine) Close() error {
	return q.DS.Close()
}
