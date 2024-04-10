package sql

import (
	"context"
	"fmt"
	"sync"
	"time"

	"database/sql"
	"os"
	"os/signal"
	"strings"
	"syscall"

	_ "modernc.org/sqlite"

	"github.com/sirupsen/logrus"
	"github.com/tenebris-tech/tail"

	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/datasource/membtree"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/expr/builtins"
	"github.com/araddon/qlbridge/lex"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/vm"

	tailsink "github.com/dispensable/tailsql/sink"
	tailsrc "github.com/dispensable/tailsql/source"
)

var ALL_DONE = make(chan os.Signal, 1)

func init() {
	signal.Notify(ALL_DONE, syscall.SIGINT, syscall.SIGTERM)
}

type LineParseFunc func(string) LRow

type WindowOpt struct {
	Size time.Duration
	SlidingInterval time.Duration
	TsExtractor func(LRow) int64
}

type LTable struct {
	Name string
	IdxCol int
	Rows []LRow
	Cols []string
}

type StreamQueryer struct {
	logger *logrus.Logger
	FileNames []string
	filterExprs map[string]expr.Node
	lineParsers []*LineParser
	tableNames []string
	engineLock *sync.Mutex
}

func NewStreamQueryer(filesToFollow []string, logger *logrus.Logger) (*StreamQueryer, error) {
	if len(filesToFollow) == 0 {
		filesToFollow = append(filesToFollow, "/dev/stdin")
	}
	return &StreamQueryer{
		logger: logger,
		FileNames: filesToFollow,
		engineLock: &sync.Mutex{},
	}, nil
}

func (s *StreamQueryer) GenerateFilesSource() (context.CancelFunc, []*tailsrc.FileSource, error) {
	r := []*tailsrc.FileSource{}
	tailCfg := &tail.Config{
		Location: &tail.SeekInfo{Offset: 0, Whence: os.SEEK_END},
		ReOpen: true,
		MustExist: true,
		Follow: true,
	}
	ctx, cfunc := context.WithCancel(context.Background())
	for _, f := range s.FileNames {
		s, err := tailsrc.NewFileSource(f, tailCfg, ctx)
		if err != nil {
			defer cfunc()
			return nil, nil, err
		}
		r = append(r, s)
	}
	return cfunc, r, nil
}

func (s *StreamQueryer) getLineMapFunc(re string, tname string) (flow.MapFunction[string, LRow], error) {
	p, err := NewLineParser(re, tname, s.logger)
	if err != nil {
		return nil, fmt.Errorf("generate line parser err: %s", err)
	}
	s.lineParsers = append(s.lineParsers, p)
	return func(line string) LRow {
		lr, err := p.parse(line)
		if err != nil {
			s.logger.Warnf("parse line %s err: %s", line, err)
			return nil
		}
		return lr
	}, nil
}

func (s *StreamQueryer) getFilterFunc(filter string, cols []string) (flow.FilterPredicate[LRow], error) {
	if s.filterExprs == nil {
		s.filterExprs = make(map[string]expr.Node)
	}
	if _, ok := s.filterExprs[filter]; !ok {
		exprAst, err := expr.ParseExpression(filter)
		if err != nil {
			s.logger.Errorf("parse filter %s err: %s", filter, err)
			return nil, err
		}
		s.filterExprs[filter] = exprAst
	}

	f := func(l LRow) bool {
		evalCtx := datasource.NewSqlDriverMessageMapVals(0, l, cols)
		ast := s.filterExprs[filter]
		s.logger.Debugf("expr: %v", ast)
		val, _ := vm.Eval(evalCtx, ast)
		v := val.Value()
		value, ok := v.(bool)
		s.logger.Debugf("filter %s on %v: %t", filter, l, value)
		if !ok {
			s.logger.Errorf("filter %s not return bool value", filter)
			return false
		}
		return value
	}

	return f, nil
}

func (s *StreamQueryer) getWinByOpt(opt *WindowOpt) streams.Flow {
	if opt.SlidingInterval != 0 {
		if opt.TsExtractor == nil {
			return flow.NewSlidingWindow[LRow](opt.Size, opt.SlidingInterval)
		} else {
			return flow.NewSlidingWindowWithExtractor[LRow](opt.Size, opt.SlidingInterval, opt.TsExtractor)
		}
	} else {
		return flow.NewTumblingWindow[LRow](opt.Size)
	}
}

func (s *StreamQueryer) getLRowsToTableFunc(name string,
	cols []string) flow.MapFunction[[]LRow, *membtree.StaticDataSource] {
	colsTotal := len(cols)
	idxRow := colsTotal - 1

	f := func(rows []LRow) *membtree.StaticDataSource {
		// update records idx in the window
		for idx, r := range rows {
			r[idxRow] = idx
		}
		sd := membtree.NewStaticDataSource(name, idxRow, rows, cols)
		return sd
	}
	return f
}

// tableMata: key is table name []string value contains cols name
func (s *StreamQueryer) getLRowsToTableByTnameFunc(
	tableMeta map[string][]string) (flow.MapFunction[[]LRow, SQLEngine], error) {
	tableColMeta := make(map[string][]int, len(tableMeta))
	for name, cols := range tableMeta {
		tableColMeta[name] = []int{len(cols)-1, len(cols)-2}
	}

	lineParsersMap := make(map[string]*LineParser, len(s.lineParsers))
	for _, l := range s.lineParsers {
		lineParsersMap[l.Tname] = l
	}

	sqlE, err := NewSQLiteEngine(lineParsersMap, s.logger)
	if err != nil {
		s.logger.Errorf("create sqlite engine failed: %s", err)
		return nil, err
	}

	// group by __tname then update idx and update datasource and registe
	f := func(rows []LRow) SQLEngine {
		rowsByTname := map[string][]LRow{}

		// update idx and group by tname
		for _, r := range rows {
			var v string
			var ok bool
			s.logger.Debugf("++ raw rows: %v", r)
			if v, ok = r[len(r)-2].(string); !ok {
				s.logger.Errorf("parse tname failed: %v", r)
				continue
			}

			if _, ok = rowsByTname[v]; !ok {
				rowsByTname[v] = []LRow{r}
			} else {
				rowsByTname[v] = append(rowsByTname[v], r)
			}
		}

		// we need clean it first for new windows data ingest
		s.engineLock.Lock()
		err := sqlE.Clean()
		if err != nil {
			s.logger.Errorf("clean db failed: %s", err)
		}

		// insert data into sqlite memdb
		for tname, trows := range rowsByTname {
			s.logger.Debugf(">> ds table: %s -> %v", tname, trows)
			err = sqlE.Insert(tname, trows)
			if err != nil {
				s.logger.Errorf("insert into %s failed: %s", tname, err)
			}
		}
		return sqlE
	}
	return f, nil
}

func (s *StreamQueryer) getRunSqlMapF(tname, sqlText string) flow.MapFunction[*membtree.StaticDataSource, *sql.Rows] {
	f := func(ds *membtree.StaticDataSource) *sql.Rows {
		err := schema.DefaultRegistry().SchemaDrop(tname, tname, lex.TokenSchema)
		if err != nil {
			s.logger.Warnf("drop schema for %s err: %s", tname, err)
		}
		err = schema.RegisterSourceAsSchema(tname, ds)
		if err != nil {
			s.logger.Errorf("registe schema err: %s", err)
			return nil
		}
		db, err := sql.Open("qlbridge", tname)
		if err != nil {
			s.logger.Errorf("open %s db failed: %s", tname, err)
			return nil
		}
		defer db.Close()

		rows, err := db.Query(sqlText)
		if err != nil {
			s.logger.Errorf("run sql %s failed on %s err: %s", sqlText, tname, err)
			return nil
		}

		return rows
	}
	return f
}

func (s *StreamQueryer) QueryOnStaticDS(sqlText string, dsn string,
	dss map[string]*membtree.StaticDataSource) *sql.Rows {

	builtins.LoadAllBuiltins()
	// registe ds to our db
	for tname, ds := range dss {
		s.logger.Debugf(">> add records %s -> %v", tname, ds)
		err := schema.DefaultRegistry().SchemaDrop(tname, tname, lex.TokenSchema)
		if err != nil {
			s.logger.Warnf("drop schema for %s err: %s", tname, err)
		}
		err = schema.RegisterSourceAsSchema(tname, ds)
		if err != nil {
			s.logger.Errorf("registe schema err: %s", err)
			return nil
		}
	}
	// now we can run sql on it
	db, err := sql.Open("qlbridge", dsn)
	if err != nil {
		s.logger.Errorf("open %s db failed: %s", dsn, err)
		return nil
	}
	defer db.Close()
	
	rows, err := db.Query(sqlText)
	if err != nil {
		s.logger.Errorf("run sql %s failed on %s err: %s", sqlText, dsn, err)
		return nil
	}
	rcols, err := rows.Columns()
	if err != nil {
		s.logger.Errorf("parse rows result cols failed: %s", err)
	}
	s.logger.Debugf("query `%s` finished, rows cols: %v", sqlText, rcols)
	// s.PrintRows(rows, false)
	return rows
}

func (s *StreamQueryer) getRunSqlOnMultiTableMapF(
	sqlText string) flow.MapFunction[SQLEngine, *sql.Rows] {
	f := func(se SQLEngine) *sql.Rows {
		// do not forget to unlock engine for other windows use
		defer s.engineLock.Unlock()

		r, err := se.Query(sqlText)
		if err != nil {
			s.logger.Errorf("run sql %s failed: %s", sqlText, err)
			return nil
		}
		return r
	}
	return f
}

func (s *StreamQueryer) waitForQuit() {
	s.logger.Infof("Streaming sql analytics started, use CTRL+C to quit ...")
	<- ALL_DONE
	s.logger.Infof("User ask quit ...")
	os.Exit(0)
}

func (s *StreamQueryer) getFormatter(format string) (tailsink.RowsFormatter, error) {
	switch format {
	case "raw":
		return tailsink.NewRawPrintFormatter(s.logger, false)
	case "rawv":
		return tailsink.NewRawPrintFormatter(s.logger, true)
	case "table":
		return tailsink.NewPtableFormatter(s.logger)
	default:
		return nil, fmt.Errorf("unsupported formater: %s", format)
	}
}

func (s *StreamQueryer) ParalleRun(rowRes []string, filters []string,
	winOpts []*WindowOpt, sqlTexts []string,
	sinkTo string, formatter string) error {

	// prepare parse functions
	lparseFuncs := []flow.MapFunction[string, LRow]{}
	for ridx, re := range rowRes {
		s.tableNames = append(s.tableNames, fmt.Sprintf("t%d", ridx))
		pf, err := s.getLineMapFunc(re, s.tableNames[ridx])
		if err != nil {
			return err
		}
		lparseFuncs = append(lparseFuncs, pf)
	}

	// prepare filter functions
	builtins.LoadAllBuiltins()
	filterFuncs := []flow.FilterPredicate[LRow]{}
	for fidx, filter := range filters {
		rf, err := s.getFilterFunc(filter, s.lineParsers[fidx].Cols)
		if err != nil {
			return fmt.Errorf("init filter func error: %s", err)
		}
		filterFuncs = append(filterFuncs, rf)
	}

	cfunc, sources, err := s.GenerateFilesSource()
	if err != nil {
		s.logger.Errorf("create fs source failed: %s", err)
		return err
	}
	defer cfunc()

	go s.waitForQuit()

	pprinter, err := s.getFormatter(formatter)
	if err != nil {
		return err
	}
	// log -> parse -> sql row -> window -> ds -> run sql -> sink
	for sidx, src := range sources {
		src.Via(
			flow.NewMap[string, LRow](lparseFuncs[sidx], 10),
		).Via(flow.NewFilter[LRow](filterFuncs[sidx], 10),
		).Via(s.getWinByOpt(winOpts[sidx]),
		).Via(flow.NewMap[[]LRow, *membtree.StaticDataSource](
			s.getLRowsToTableFunc(
				s.tableNames[sidx], s.lineParsers[sidx].Cols,
			), 1),
		).Via(flow.NewMap[*membtree.StaticDataSource, *sql.Rows](
			s.getRunSqlMapF(s.tableNames[sidx], sqlTexts[sidx]),
			1),
		).To(tailsink.NewStdOutSqlRowsSink(sqlTexts[sidx], pprinter, s.logger))
	}	
	return nil
}

func (s *StreamQueryer) PrintRows(rows *sql.Rows, closeAfterPrint bool) error {
	if rows == nil {
		s.logger.Warnf("No valide rows parsed in this round, continue ...")
		return nil
	}

	if closeAfterPrint {
		defer rows.Close()
	}

	qcols, err := rows.Columns()
	if err != nil {
		return err
	}

	fmt.Printf(
		"\nRun qeury on logs:\n (%v)\n\n",
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

func (s *StreamQueryer) JoinRun(rowRes, filters []string,
	winOpt *WindowOpt, sqlText, sinkTo string, formatter string) error {
	// prepare parse functions
	lparseFuncs := []flow.MapFunction[string, LRow]{}
	for ridx, re := range rowRes {
		s.tableNames = append(s.tableNames, fmt.Sprintf("t%d", ridx))
		pf, err := s.getLineMapFunc(re, s.tableNames[ridx])
		if err != nil {
			return err
		}
		lparseFuncs = append(lparseFuncs, pf)
	}

	// prepare filter functions
	builtins.LoadAllBuiltins()
	filterFuncs := []flow.FilterPredicate[LRow]{}
	for fidx, filter := range filters {
		rf, err := s.getFilterFunc(filter, s.lineParsers[fidx].Cols)
		if err != nil {
			return fmt.Errorf("init filter func error: %s", err)
		}
		filterFuncs = append(filterFuncs, rf)
	}

	cancelTailF, sources, err := s.GenerateFilesSource()
	if err != nil {
		return err
	}
	defer cancelTailF()

	// log -> parse -> sql row -> window -> ds -> run sql -> sink
	// log -> parse -> sql row /
	// ...
	var mFlows []streams.Flow
	for sidx, src := range sources {
		f := src.Via(
			flow.NewMap[string, LRow](lparseFuncs[sidx], 1),
		).Via(flow.NewFilter[LRow](filterFuncs[sidx], 1))
		mFlows = append(mFlows, f)
	}
	mFlow := flow.Merge(mFlows...)

	tableMetaData := map[string][]string{}
	for _, lp := range s.lineParsers {
		tableMetaData[lp.Tname] = lp.Cols
	}

	go s.waitForQuit()

	insertRowsToSqlNg, err := s.getLRowsToTableByTnameFunc(tableMetaData)
	if err != nil {
		return err
	}

	pprinter, err := s.getFormatter(formatter)
	if err != nil {
		return err
	}

	mFlow.Via(s.getWinByOpt(winOpt)).Via(
		// create multi table by metafiled __tname
		// so we can support sql join
		// we must ensure all table created
		flow.NewMap[[]LRow, SQLEngine](
			insertRowsToSqlNg, 1),
	).Via(flow.NewMap[SQLEngine, *sql.Rows](
		s.getRunSqlOnMultiTableMapF(sqlText), 1,),
	).To(tailsink.NewStdOutSqlRowsSink(sqlText, pprinter, s.logger))

	return nil
}
