package sql

import (
	"database/sql/driver"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/araddon/dateparse"
	_ "github.com/araddon/qlbridge/qlbdriver"

	"github.com/sirupsen/logrus"
)

type (
	LRow = []driver.Value
)

type LineParser struct {
	logger *logrus.Logger
	lineRegex *regexp.Regexp
	Cols []string
	ColUdts []*ColUdt
	Tname string
	Tschema string
	atLeastMatchFileds int
}

func NewLineParser(rowRe, tname string, logger *logrus.Logger) (*LineParser, error) {
	re, err := regexp.Compile(rowRe)
	if err != nil {
		return nil, fmt.Errorf("compile row regex error: %s", err)
	}

	coludts := []*ColUdt{}
	cols := []string{}
	fieldsSchema := []string{
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (", tname),
	}
	for i, cname := range re.SubexpNames() {
		if i == 0 || cname == "" {
			continue
		}
		metadata := strings.Split(cname, "__")
		fname := metadata[0]
		fieldTName := metadata[1]
		switch fieldTName {
		case "bool":
			fieldsSchema = append(fieldsSchema, fmt.Sprintf("%s BOOLEAN NOT NULL,", fname))
		case "int":
			fieldsSchema = append(fieldsSchema, fmt.Sprintf("%s INTEGER NOT NULL,", fname))
		case "float":
			fieldsSchema = append(fieldsSchema, fmt.Sprintf("%s FLOAT NOT NULL,", fname))
		case "date":
			fieldsSchema = append(fieldsSchema, fmt.Sprintf("%s DATETIME NOT NULL,", fname))
		default:
			fieldsSchema = append(fieldsSchema, fmt.Sprintf("%s TEXT NOT NULL,", fname))
		}
		coludts = append(coludts, &ColUdt{Name: fname, TypeName: fieldTName})
		cols = append(cols, fname)
	}

	for _, metafield := range []string{"__tname", "__id"} {
		coludts = append(coludts, &ColUdt{Name: metafield, TypeName: metafield})
		cols = append(cols, metafield)
	}

	// drop the last comma for sytax correctness
	lastFieldIdx := len(fieldsSchema) - 1
	lastField := fieldsSchema[lastFieldIdx]
	fieldsSchema[lastFieldIdx] = lastField[:len(lastField)-1]

	fieldsSchema = append(fieldsSchema, ");")

	logger.Debugf("cols: %+v", cols)
	
	return &LineParser{
		logger: logger,
		lineRegex: re,
		ColUdts: coludts,
		Cols: cols,
		Tname: tname,
		Tschema: strings.Join(fieldsSchema, "\n"),
		atLeastMatchFileds: len(cols) - 2,
	}, nil
}

func (p *LineParser) parse(rawRow string) (LRow, error) {
	// ignore empty line
	if rawRow == "" {
		return nil, nil
	}

	dv := []driver.Value{}
	match := p.lineRegex.FindStringSubmatch(rawRow)

	if len(match) <= p.atLeastMatchFileds {
		return nil, fmt.Errorf("not match %s", rawRow)
	}

	rawV := ""
	for i, colT := range p.ColUdts {
		if colT.TypeName == "__tname" || colT.TypeName == "__id" {
			rawV = ""
		} else {
			rawV = match[i+1]
			p.logger.Debugf(
				"colT: %s %s | matched %d: %v",
				colT.Name, colT.TypeName,
				i, rawV,
			)
		}
		switch colT.TypeName {
		case "bool":
			boolValue, err := strconv.ParseBool(rawV)
			if err != nil {
				return nil, fmt.Errorf("parse bool value on %s err: %s", rawV, err)
			}
			dv = append(dv, boolValue)
		case "int":
			intValue, err := strconv.ParseInt(rawV, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("parse int value on: %s err: %s", rawV, err)
			}
			dv = append(dv, intValue)
		case "float":
			floatV, err := strconv.ParseFloat(rawV, 10)
			if err != nil {
				return nil, fmt.Errorf("parse float value on: %s err: %s", rawV, err)
			}
			dv = append(dv, floatV)
		case "date":
			dateValue, err := dateparse.ParseAny(rawV)
			if err != nil {
				return nil, fmt.Errorf("paser date value on: %s err: %s", rawV, err)
			}
			dv = append(dv, dateValue)
		case "__tname":
			dv = append(dv, p.Tname)
		case "__id":
			dv = append(dv, 0)
		default:
			dv = append(dv, rawV)
		}
	}
	p.logger.Debugf("parse line get: %v", dv)
	return dv, nil
}
