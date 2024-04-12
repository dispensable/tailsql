package config

import (
	"fmt"
	"os"

	"strconv"
	"strings"

	"github.com/dispensable/tailsql/utils"
	"gopkg.in/yaml.v3"
)

type SinkCfg struct {
	To string `yaml:"to"` // sink type now support: stdout
	Formatter string `yaml:"formatter"`// formatter: table/raw/rawV
}

type LogCfg struct {
	Level string `yaml:"level"`
}

type WindowCfg struct {
	SizeSeconds int `yaml:"size_seconds"`
	SlidingIntervalSeconds int `yaml:"sliding_interval_seconds"`
	IdxOfTsField int `yaml:"idx_of_ts_field"`
}

type ThrottlerCfg struct {
	MaxEles int `yaml:"max_elements_in_period"`
	PeriodSeconds int `yaml:"period_seconds"`
	BufferSize int `yaml:"buffer_size"`
}

type FollowFileCfg struct {
	Path string `yaml:"path"`
	Regex string `yaml:"regex"`
	Filter string `yaml:"filter"`
	ThrottleOpt *ThrottlerCfg `yaml:"throttle"`
	DoNotTail bool `yaml:"do_not_tail"`
}

type TailSqlCfg struct {
	FileCfgs []*FollowFileCfg `yaml:"files"`
	LoggerCfg *LogCfg `yaml:"log"`
	WinOpt *WindowCfg `yaml:"window"`
	Sink *SinkCfg `yaml:"sink"`
	DBEngine string `yaml:"db_engine"` // support sqlite/duckdb/qlbridge
}

func NewCfgFromFile(fileName string) (*TailSqlCfg, error) {
	exists, err := utils.IsPathExists(fileName)
	if err != nil || !exists {
		return nil, fmt.Errorf("file %s check err: %s or not exists", fileName, err)
	}
	data, err := os.ReadFile(fileName)
	if err != nil {
		return nil, fmt.Errorf("read cfg file %s err:%s", fileName, err)
	}
	cfg := &TailSqlCfg{}
	err = yaml.Unmarshal(data, cfg)
	if err != nil {
		return nil, fmt.Errorf("parse data failed: %s", err)
	}
	return cfg, nil
}

func parseWinOpt(s string) (*WindowCfg, error) {
	splited := strings.Split(s, ":")
	if len(splited) != 3 {
		return nil, fmt.Errorf("failed to parse %s to winopt, need 3 fields", s)
	}

	size, err := strconv.ParseUint(splited[0], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("failed to parse windows size duration: %s", splited[0])
	}

	slideInterval, err := strconv.ParseInt(splited[1], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("failed to parse slide interval as seconds: %s", splited[1])
	}

	idxOfTs, err := strconv.ParseInt(splited[2], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("failed to parse idx of ts: %s", splited[2])
	}

	return &WindowCfg{
		SizeSeconds: int(size),
		SlidingIntervalSeconds: int(slideInterval),
		IdxOfTsField: int(idxOfTs),
	}, nil
}

func parseThrottleOpt(s string) (*ThrottlerCfg, error) {
	if s == "" {
		return nil, nil
	}
	splited := strings.Split(s, ":")
	if len(splited) != 3 {
		return nil, fmt.Errorf("failed to parse %s to throttler, need 3 fields", s)
	}

	maxEles, err := strconv.ParseUint(splited[0], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("failed to parse max eles: %s", splited[0])
	}

	interval, err := strconv.ParseInt(splited[1], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("failed to parse throttle interval as seconds: %s", splited[1])
	}

	buffSize, err := strconv.ParseInt(splited[2], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("failed to parse buffsize: %s", splited[2])
	}

	return &ThrottlerCfg{
		MaxEles: int(maxEles),
		PeriodSeconds: int(interval),
		BufferSize: int(buffSize),
	}, nil
}

func NewCfg(
	filesToFollow []string,
	regexSlice []string,
	filters []string,
	winOpt string,
	throttlers []string,
	sinkTo string,
	doNotTail bool,
	dbEngine string,
	formatter string,
	logLevel string,
) (*TailSqlCfg, error) {
	r := &TailSqlCfg{}

	// check
	if len(regexSlice) != len(filesToFollow) {
		return nil, fmt.Errorf("regex num must match with files")
	}

	if len(filters) == 0 {
		filters = make([]string, len(filesToFollow))
	} else {
		if len(filters) != len(filesToFollow) {
			return nil, fmt.Errorf("filters num must match with files")
		}
	}

	if len(throttlers) == 0 {
		throttlers = make([]string, len(filesToFollow))
	} else {
		if len(throttlers) != len(filesToFollow) {
			return nil, fmt.Errorf("throttlers num must match with files")
		}
	}
	
	
	// files cfg
	fc := []*FollowFileCfg{}
	for idx, file := range filesToFollow {
		topt, err := parseThrottleOpt(throttlers[idx])
		if err != nil {
			return nil, err
		}

		fc = append(fc, &FollowFileCfg{
			Path: file,
			Regex: regexSlice[idx],
			Filter: filters[idx],
			ThrottleOpt: topt,
			DoNotTail: doNotTail,
		})
	}

	// logger cfg
	if logLevel == "" {
		logLevel = "info"
	}
	lcfg := &LogCfg{Level: logLevel}
	wopt, err := parseWinOpt(winOpt)
	if err != nil {
		return nil, err
	}

	// allow user override this cfg when cfg file given
	if formatter == "" {
		formatter = "raw"
	}
	if dbEngine == "" {
		dbEngine = "duckdb"
	}
	if sinkTo == "" {
		sinkTo = "stdout"
	}

	scfg := &SinkCfg{To: sinkTo, Formatter: formatter}
	r.DBEngine = dbEngine
	r.FileCfgs = fc
	r.WinOpt = wopt
	r.LoggerCfg = lcfg
	r.Sink = scfg
	return r, nil
}
