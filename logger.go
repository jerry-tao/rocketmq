package rocketmq

import (
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Logger interface {
	Debug(v ...interface{})
	Debugf(format string, v ...interface{})
	Info(v ...interface{})
	Infof(format string, v ...interface{})
	Error(v ...interface{})
	Errorf(format string, v ...interface{})
	Fatal(v ...interface{})
	Fatalf(format string, v ...interface{})
}

var logger = Logger(newDefaultLogger())

const (
	EmptyLevel = iota
	FatalLevel
	ErrorLevel
	InfoLevel
	DebugLevel
	DefaultFormat = "[%time - %pid:%level]: %log %file:%line" // fullfile
	DefaultLevel  = EmptyLevel
)

var levelString = [5]string{"", " Fatal ", " Error ", " Info  ", " Debug "}

type defaultLogger struct {
	m      sync.Mutex
	level  int
	format string
	out    io.Writer
	r      strings.Replacer
	pid    string
}

func (l *defaultLogger) formatLog(level int, log string) string {

	tmp := strings.Replace(l.format, "%time", time.Now().Format("2006-01-02T15:04:05"), -1)
	tmp = strings.Replace(tmp, "%log", log, -1)
	tmp = strings.Replace(tmp, "%level", levelString[level], -1)
	tmp = strings.Replace(tmp, "%pid", l.pid, -1)
	var ok bool
	_, file, line, ok := runtime.Caller(3)
	if !ok {
		file = "???"
		line = 0
	}
	tmp = strings.Replace(tmp, "%fullfile", file, -1)
	tmp = strings.Replace(tmp, "%file", path.Base(file), -1)
	tmp = strings.Replace(tmp, "%line", strconv.Itoa(line), -1)

	if len(tmp) == 0 || tmp[len(tmp)-1] != '\n' {
		tmp = tmp + "\n"
	}
	return tmp
}

func (l *defaultLogger) output(level int, log string) {
	l.m.Lock()
	l.m.Unlock()
	if l.level >= level {
		log = l.formatLog(level, log)
		_, _ = l.out.Write([]byte(log))
	}
}

func (l *defaultLogger) Info(v ...interface{}) {
	if l.level > EmptyLevel {
		l.output(InfoLevel, fmt.Sprint(v...))
	}
}
func (l *defaultLogger) Infof(format string, v ...interface{}) {
	if l.level > EmptyLevel {
		l.output(InfoLevel, fmt.Sprintf(format, v...))
	}
}
func (l *defaultLogger) Debug(v ...interface{}) {
	if l.level > EmptyLevel {
		l.output(DebugLevel, fmt.Sprint(v...))
	}
}
func (l *defaultLogger) Debugf(format string, v ...interface{}) {
	if l.level > EmptyLevel {
		l.output(DebugLevel, fmt.Sprintf(format, v...))
	}
}
func (l *defaultLogger) Error(v ...interface{}) {
	if l.level > EmptyLevel {
		l.output(ErrorLevel, fmt.Sprint(v...))
	}
}
func (l *defaultLogger) Errorf(format string, v ...interface{}) {
	if l.level > EmptyLevel {
		l.output(ErrorLevel, fmt.Sprintf(format, v...))
	}
}
func (l *defaultLogger) Fatal(v ...interface{}) {
	if l.level > EmptyLevel {
		l.output(FatalLevel, fmt.Sprint(v...))
	}
}
func (l *defaultLogger) Fatalf(format string, v ...interface{}) {
	if l.level > EmptyLevel {
		l.output(FatalLevel, fmt.Sprintf(format, v...))
	}
}
func (l *defaultLogger) SetLevel(level int) {
	l.m.Lock()
	defer l.m.Unlock()
	if level > DebugLevel || level < EmptyLevel {
		return
	}
	l.level = level
}
func (l *defaultLogger) SetFormat(format string) {
	l.m.Lock()
	defer l.m.Unlock()
	l.format = format
}
func (l *defaultLogger) SetOutput(out io.Writer) {
	l.m.Lock()
	defer l.m.Unlock()
	l.out = out
}

func newDefaultLogger() *defaultLogger {
	return &defaultLogger{
		level:  DefaultLevel,
		format: DefaultFormat,
		out:    os.Stdout,
		pid:    strconv.Itoa(os.Getpid()),
	}
}

func SetLogger(l Logger) {
	logger = l
}

func SetLevel(level int) {
	l, ok := logger.(*defaultLogger)
	if ok {
		l.SetLevel(level)
	}
}
func SetFormat(format string) {
	l, ok := logger.(*defaultLogger)
	if ok {
		l.SetFormat(format)
	}
}
func SetOutput(out io.Writer) {
	l, ok := logger.(*defaultLogger)
	if ok {
		l.SetOutput(out)
	}
}
