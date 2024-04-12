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

	"github.com/dispensable/tailsql/config"
	tailsink "github.com/dispensable/tailsql/sink"
	tailsrc "github.com/dispensable/tailsql/source"
	"github.com/dispensable/tailsql/utils"
)

var ALL_DONE = make(chan os.Signal, 1)

func init() {
	signal.Notify(ALL_DONE, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		fmt.Println(">>> Press CTRL + C to quit ...")
		<- ALL_DONE
		fmt.Println(">>> User ask to quit ...")
		os.Exit(0)
	}()
	builtins.LoadAllBuiltins()
}

type LineParseFunc func(string) LRow

type StreamQueryer struct {
	logger *logrus.Logger
	Config *config.TailSqlCfg
	filterExprs map[string]expr.Node
	lineParsers []*LineParser
	tableNames []string
	engineLock *sync.Mutex
}


func NewStreamQueryerFromCfg(cfg *config.TailSqlCfg, logger *logrus.Logger) (*StreamQueryer, error) {
	if len(cfg.FileCfgs) == 0 {
		return nil, fmt.Errorf("cfg error, at least need 1 file to follow")
	}

	return &StreamQueryer{
		logger: logger,
		Config: cfg,
		engineLock: &sync.Mutex{},
	}, nil
}

func (s *StreamQueryer) GenerateFSSource(ctx context.Context, fc *config.FollowFileCfg) (streams.Source, error) {
	f := fc.Path
	// nameedpipe file
	isP, err := utils.IsNamedPipe(f)
	if err != nil {
		return nil, err
	}
	s.logger.Debugf("%s is named pipe: %t", f, isP)
	if isP {
		npipeSrc, err := tailsrc.NewNamedPipeSrc(f, ctx)
		if err != nil {
			return nil, err
		}
		return npipeSrc, nil
	} else if f == "/dev/stdin" {
		stdinSrc, err := tailsrc.NewStdinSrc(ctx)
		if err != nil {
			return nil, err
		}
		return stdinSrc, nil
	} else {
		whence := os.SEEK_END
		if fc.DoNotTail {
			whence = os.SEEK_CUR
		}
		tailCfg := &tail.Config{
			Location: &tail.SeekInfo{Offset: 0, Whence: whence},
			ReOpen: true,
			MustExist: true,
			Follow: true,
		}
		s, err := tailsrc.NewFileSource(f, tailCfg, ctx)
		if err != nil {
			return nil, err
		}
		return s, nil
	}
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
			s.logger.Debugf("parse line %s err: %s", line, err)
			return nil
		}
		if len(lr) != len(p.Cols) {
			s.logger.Errorf("line row fields not match: %v", lr)
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
		if l == nil || len(l) == 0 {
			return false
		}
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

func (s *StreamQueryer) getWinByOpt(opt *config.WindowCfg) streams.Flow {
	var tsExtracF func(LRow) int64
	
	if opt.IdxOfTsField >= 0 {
		tsExtracF = func(row LRow) int64 {
			f := row[opt.IdxOfTsField]
			if v, ok := f.(time.Time); ok {
				return int64(v.Nanosecond())
			} else {
				panic(fmt.Sprintf("idx %d of row is not a date type", opt.IdxOfTsField))
			}
		}
	}

	if opt.SlidingIntervalSeconds != 0 {
		if tsExtracF == nil {
			return flow.NewSlidingWindow[LRow](
				time.Duration(opt.SizeSeconds * int(time.Second)),
				time.Duration(opt.SlidingIntervalSeconds * int(time.Second)),
			)
		} else {
			return flow.NewSlidingWindowWithExtractor[LRow](
				time.Duration(opt.SizeSeconds * int(time.Second)),
				time.Duration(opt.SlidingIntervalSeconds * int(time.Second)),
				tsExtracF,
			)
		}
	} else {
		return flow.NewTumblingWindow[LRow](time.Duration(opt.SizeSeconds * int(time.Second)))
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
			if len(r) == 0 {
				continue
			}
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

func (s *StreamQueryer) getThrottler(tc *config.ThrottlerCfg) (*flow.Throttler, error) {
	// maxeles zero means we don't wanna a throttler
	if tc == nil || tc.MaxEles == 0 {
		return nil, nil
	}
	return flow.NewThrottler(
		tc.MaxEles, time.Duration(tc.PeriodSeconds * int(time.Second)),
		tc.BufferSize, flow.Discard,
	), nil
}

func (s *StreamQueryer) RunAnalysisFromCfg(sqlText string) error {
	cfg := s.Config
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mFlows []streams.Flow

	for idx, fileCfg := range cfg.FileCfgs {
		// prepare parse functions
		s.tableNames = append(s.tableNames, fmt.Sprintf("t%d", idx))
		pf, err := s.getLineMapFunc(fileCfg.Regex, s.tableNames[idx])
		if err != nil {
			return err
		}

		// prepare filter functions
		var filterFunc flow.FilterPredicate[LRow]
		if fileCfg.Filter != "" {
			s.logger.Debugf("parse and create filters ...")
			filterFunc, err = s.getFilterFunc(fileCfg.Filter, s.lineParsers[idx].Cols)
			if err != nil {
				return err
			}
		}

		// prepare throttler
		var throttler *flow.Throttler
		if fileCfg.ThrottleOpt != nil {
			s.logger.Debugf("parse and create throttlers ...")
			throttler, err = s.getThrottler(fileCfg.ThrottleOpt)
			if err != nil {
				return err
			}
		}

		// prepare fs source
		s.logger.Debugf("genearte fs source ...")
		src, err := s.GenerateFSSource(ctx, fileCfg)
		if err != nil {
			return err
		}

		// log -> parse -> filter -> throttler -> sql row -> window -> ds -> run sql -> sink
		// log -> parse -> filter -> throttler -> sql row /
		// ...
		nextFlow := src.Via(flow.NewMap[string, LRow](pf, 10))
		if filterFunc != nil {
			nextFlow = nextFlow.Via(flow.NewFilter[LRow](filterFunc, 10))
		}
		if throttler != nil {
			nextFlow = nextFlow.Via(throttler)
		}
		mFlows = append(mFlows, nextFlow)
	}


	mFlow := flow.Merge(mFlows...)

	tableMetaData := map[string][]string{}
	for _, lp := range s.lineParsers {
		tableMetaData[lp.Tname] = lp.Cols
	}

	insertRowsToSqlNg, err := s.getLRowsToTableByTnameFunc(tableMetaData, cfg.DBEngine)
	if err != nil {
		return err
	}

	pprinter, err := s.getFormatter(cfg.Sink.Formatter)
	if err != nil {
		return err
	}

	s.logger.Infof("Query stream started, please wait %ds...", cfg.WinOpt.SizeSeconds)
	mFlow.Via(s.getWinByOpt(cfg.WinOpt)).Via(
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
