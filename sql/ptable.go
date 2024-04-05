package sql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/araddon/dateparse"
	"github.com/araddon/qlbridge/datasource/membtree"
	"github.com/araddon/qlbridge/lex"
	_ "github.com/araddon/qlbridge/qlbdriver"

	// "github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/expr/builtins"
	"github.com/araddon/qlbridge/schema"

	// "github.com/araddon/qlbridge/value"
	"github.com/sirupsen/logrus"
)

type ColUdt struct {
	Name string
	TypeName string
}

type TableParser struct {
	logger *logrus.Logger
	lineRegex *regexp.Regexp
	Rows [][]driver.Value
	Cols []string
	StaticDS *membtree.StaticDataSource
	Name string
	ColUdts []*ColUdt
	schemaRegisted bool
}

func NewTableParser(name string, rowRe string, logger *logrus.Logger) (*TableParser, error) {
	re, err := regexp.Compile(rowRe)
	if err != nil {
		return nil, fmt.Errorf("parse row re error: %s", err)
	}

	coludts := []*ColUdt{}
	cols := []string{"_id"}
	for i, cname := range re.SubexpNames() {
		if i == 0 || name == "" {
			continue
		}
		metadata := strings.Split(cname, "_")
		fname := metadata[0]
		fieldTName := metadata[1]
		coludts = append(coludts, &ColUdt{Name: fname, TypeName: fieldTName})
		cols = append(cols, fname)
	}

	logger.Infof("cols: %+v", cols)
	
	return &TableParser{
		Name: name,
		logger: logger,
		lineRegex: re,
		Rows: [][]driver.Value{},
		ColUdts: coludts,
		Cols: cols,
	}, nil
}

func (p *TableParser) CreateDB(rawRows []string) error {
	rows := [][]driver.Value{}

	for idx, rawRow := range rawRows {
		match := p.lineRegex.FindStringSubmatch(rawRow)
		var dv []driver.Value
		if len(match) == len(p.Cols) {
			dv = []driver.Value{idx}
		} else {
			continue
		}

		for i, colT := range p.ColUdts {
			p.logger.Debugf(
				"colT: %s %s | matched %d: %v",
				colT.Name, colT.TypeName,
				i+1, match[i+1],
			)
			rawV := match[i+1]
			switch colT.TypeName {
			case "bool":
				boolValue, err := strconv.ParseBool(rawV)
				if err != nil {
					return fmt.Errorf("parse bool value on %s err: %s", rawV, err)
				}
				dv = append(dv, boolValue)
			case "int":
				intValue, err := strconv.ParseInt(rawV, 10, 64)
				if err != nil {
					return fmt.Errorf("parse int value on: %s err: %s", rawV, err)
				}
				dv = append(dv, intValue)
			case "date":
				dateValue, err := dateparse.ParseAny(rawV)
				if err != nil {
					return fmt.Errorf("paser date value on: %s err: %s", rawV, err)
				}
				dv = append(dv, dateValue)
			default:
				dv = append(dv, rawV)
			}
		}
		rows = append(rows, dv)
	}

	p.Rows = rows

	// create a memdb
	p.logger.Infof("rows: %s", p.Rows)

	mb := membtree.NewStaticDataSource(p.Name, 0, p.Rows, p.Cols)
	p.StaticDS = mb
	// init db query
	builtins.LoadAllBuiltins()

	return nil
}

func (p *TableParser) RunSql(query string) (*sql.Rows, error) {
	if len(p.Rows) == 0 {
		return nil, nil
	}

	if p.schemaRegisted {
		err := schema.DefaultRegistry().SchemaDrop(p.Name, p.Name, lex.TokenSchema)
		if err != nil {
			return nil, err
		}
	}
	err := schema.RegisterSourceAsSchema(p.Name, p.StaticDS)
	if err != nil {
		p.logger.Errorf("registe schema err: %s", err)
		return nil, err
	}
	p.schemaRegisted = true

	db, err := sql.Open("qlbridge", p.Name)
	if err != nil {
		return nil, err
	}

	defer db.Close()

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}

	return rows, nil
}
