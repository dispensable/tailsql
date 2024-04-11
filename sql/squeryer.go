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
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/expr/builtins"
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

type ThrottlerOpt struct {
	MaxEles int
	Period time.Duration
	BuffSize int
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
	expr.FuncAdd("randfilter", &RandFilter{})
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
		if val == nil {
			s.logger.Warnf("filter %s on %v got nil, ignore this row", filter, l)
			return false
		}
		v := val.Value()
		value, ok := v.(bool)
		if !ok {
			s.logger.Errorf("filter %s not return bool value", filter)
			return false
		}
		s.logger.Debugf("filter %s on %v: %t", filter, l, value)
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

func (s *StreamQueryer) getDBEngine(engine string) (SQLEngine, error) {
	lineParsersMap := make(map[string]*LineParser, len(s.lineParsers))
	for _, l := range s.lineParsers {
		lineParsersMap[l.Tname] = l
	}

	switch engine {
	case "sqlite":
		return NewSQLiteEngine(lineParsersMap, s.logger)
	case "duckdb":
		return NewDuckDBEngine(lineParsersMap, s.logger)
	case "qlbridge":
		if len(s.lineParsers) != 1 {
			return nil, fmt.Errorf("qlbridge memtree db does not support multiple table")
		}
		lp := s.lineParsers[0]
		return NewQLMembtreeDSEngine(lp.Tname, lp.Cols, s.logger)
	default:
		return nil, fmt.Errorf("%s engine unsupported", engine)
	}
}

// tableMata: key is table name []string value contains cols name
func (s *StreamQueryer) getLRowsToTableByTnameFunc(
	tableMeta map[string][]string, dbEngine string) (flow.MapFunction[[]LRow, SQLEngine], error) {
	tableColMeta := make(map[string][]int, len(tableMeta))
	for name, cols := range tableMeta {
		tableColMeta[name] = []int{len(cols)-1, len(cols)-2}
	}

	sqlE, err := s.getDBEngine(dbEngine)
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

func (s *StreamQueryer) getFilterFuncs(filters []string) ([]flow.FilterPredicate[LRow], error) {
	// prepare filter functions
	builtins.LoadAllBuiltins()
	filterFuncs := make([]flow.FilterPredicate[LRow], len(filters))
	for fidx, filter := range filters {
		if filter == "" {
			// use don't wanna use filter on this stream ignore
			filterFuncs[fidx] = nil
			continue
		}

		rf, err := s.getFilterFunc(filter, s.lineParsers[fidx].Cols)
		if err != nil {
			return nil, fmt.Errorf("init filter func error: %s", err)
		}
		filterFuncs[fidx] = rf
	}
	return filterFuncs, nil
}

func (s *StreamQueryer) getThrottlers(tconfigs []*ThrottlerOpt) ([]*flow.Throttler, error) {
	if len(tconfigs) == 0 {
		return nil, nil
	}

	r := make([]*flow.Throttler, len(tconfigs))
	for idx, tc := range tconfigs {
		// maxeles zero means we don't wanna a throttler
		if tc == nil || tc.MaxEles == 0 {
			r[idx] = nil
			continue
		}
		r[idx] = flow.NewThrottler(tc.MaxEles, tc.Period, tc.BuffSize, flow.Discard)
	}
	return r, nil
}

func (s *StreamQueryer) JoinRun(rowRes, filters []string,
	winOpt *WindowOpt, sqlText, sinkTo, formatter, dbengine string,
	throttleOpts []*ThrottlerOpt) error {
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
	filterFuncs, err := s.getFilterFuncs(filters)
	if err != nil {
		return err
	}

	// prepare throttler
	throttlers, err := s.getThrottlers(throttleOpts)
	if err != nil {
		return err
	}

	// prepare fs source
	cancelTailF, sources, err := s.GenerateFilesSource()
	if err != nil {
		return err
	}
	defer cancelTailF()

	// log -> parse -> filter -> throttler -> sql row -> window -> ds -> run sql -> sink
	// log -> parse -> filter -> throttler -> sql row /
	// ...
	var mFlows []streams.Flow
	for sidx, src := range sources {
		f := src.Via(
			flow.NewMap[string, LRow](lparseFuncs[sidx], 10),
		)

		// if user not defined filter we just ignore
		if len(filterFuncs) > 0 && filterFuncs[sidx] != nil {
			f.Via(flow.NewFilter[LRow](filterFuncs[sidx], 10))	
		}


		// if user not defined throttler we just ignore
		if len(throttlers) > 0 && throttlers[sidx] != nil {
			f.Via(throttlers[sidx])
		}

		mFlows = append(mFlows, f)
	}
	mFlow := flow.Merge(mFlows...)

	tableMetaData := map[string][]string{}
	for _, lp := range s.lineParsers {
		tableMetaData[lp.Tname] = lp.Cols
	}

	go s.waitForQuit()

	insertRowsToSqlNg, err := s.getLRowsToTableByTnameFunc(tableMetaData, dbengine)
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
