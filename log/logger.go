package log

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"time"
)

type Logger interface {
	Trace(msg string, ctx ...interface{})
	Debug(msg string, ctx ...interface{})
	Info(msg string, ctx ...interface{})
	Warn(msg string, ctx ...interface{})
	Error(msg string, ctx ...interface{})
	Crit(msg string, ctx ...interface{})
}

func NewNoop() Logger {
	return &NoopLogger{}
}

type NoopLogger struct{}

func (l *NoopLogger) Trace(_ string, _ ...interface{}) {}
func (l *NoopLogger) Debug(_ string, _ ...interface{}) {}
func (l *NoopLogger) Info(_ string, _ ...interface{})  {}
func (l *NoopLogger) Warn(_ string, _ ...interface{})  {}
func (l *NoopLogger) Error(_ string, _ ...interface{}) {}
func (l *NoopLogger) Crit(_ string, _ ...interface{})  {}

func NewBuffered(b *bytes.Buffer) Logger {
	if b == nil {
		b = new(bytes.Buffer)
	}

	return &BufLogger{Buffer: b}
}

type BufLogger struct {
	*bytes.Buffer
}

func (l *BufLogger) Trace(msg string, ctx ...interface{}) {
	l.log("trace", msg, ctx)
}
func (l *BufLogger) Debug(msg string, ctx ...interface{}) {
	l.log("debug", msg, ctx)
}
func (l *BufLogger) Info(msg string, ctx ...interface{}) {
	l.log("info", msg, ctx)
}
func (l *BufLogger) Warn(msg string, ctx ...interface{}) {
	l.log("warn", msg, ctx)
}
func (l *BufLogger) Error(msg string, ctx ...interface{}) {
	l.log("error", msg, ctx)
}
func (l *BufLogger) Crit(msg string, ctx ...interface{}) {
	l.log("crit", msg, ctx)
}

func (l *BufLogger) log(lvl, msg string, ctx []interface{}) {
	ctx = normalize(ctx)
	logfmt(l.Buffer, append([]interface{}{"t", time.Now(), "lvl", lvl, "msg", msg}, ctx...))
}

// Below taken from ledgerwatch/log
// Source: https://github.com/ledgerwatch/log/blob/master/v3/format.go

const errorKey = "gomdbxdb.Error"

func normalize(ctx []interface{}) []interface{} {
	// ctx needs to be even because it's a series of key/value pairs
	// no one wants to check for errors on logging functions,
	// so instead of erroring on bad input, we'll just make sure
	// that things are the right length and users can fix bugs
	// when they see the output looks wrong
	if len(ctx)%2 != 0 {
		ctx = append(ctx, nil, errorKey, "Normalized odd number of arguments by adding nil")
	}

	return ctx
}

func logfmt(buf *bytes.Buffer, ctx []interface{}) {
	for i := 0; i < len(ctx); i += 2 {
		if i != 0 {
			buf.WriteByte(' ')
		}

		k, ok := ctx[i].(string)
		v := formatLogfmtValue(ctx[i+1])
		if !ok {
			k, v = errorKey, formatLogfmtValue(k)
		}

		buf.WriteString(k)
		buf.WriteByte('=')
		buf.WriteString(v)
	}

	buf.WriteByte('\n')
}

// formatValue formats a value for serialization
func formatLogfmtValue(value interface{}) string {
	if value == nil {
		return "nil"
	}

	if t, ok := value.(time.Time); ok {
		// Performance optimization: No need for escaping since the provided
		// timeFormat doesn't have any escape characters, and escaping is
		// expensive.
		return t.Format(time.RFC3339)
	}
	value = formatShared(value)
	switch v := value.(type) {
	case bool:
		return strconv.FormatBool(v)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', 3, 64)
	case float64:
		return strconv.FormatFloat(v, 'f', 3, 64)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", value)
	case string:
		return escapeString(v)
	default:
		return escapeString(fmt.Sprintf("%+v", value))
	}
}

func formatShared(value interface{}) (result interface{}) {
	defer func() {
		if err := recover(); err != nil {
			if v := reflect.ValueOf(value); v.Kind() == reflect.Ptr && v.IsNil() {
				result = "nil"
			} else {
				panic(err)
			}
		}
	}()

	switch v := value.(type) {
	case time.Time:
		return v.Format(time.RFC3339)

	case error:
		return v.Error()

	case fmt.Stringer:
		return v.String()

	default:
		return v
	}
}

var stringBufPool = sync.Pool{
	New: func() interface{} { return new(bytes.Buffer) },
}

func escapeString(s string) string {
	needsQuotes := false
	needsEscape := false
	for _, r := range s {
		if r <= ' ' || r == '=' || r == '"' {
			needsQuotes = true
		}
		if r == '\\' || r == '"' || r == '\n' || r == '\r' || r == '\t' {
			needsEscape = true
		}
	}
	if needsEscape == false && needsQuotes == false {
		return s
	}
	e := stringBufPool.Get().(*bytes.Buffer)
	e.WriteByte('"')
	for _, r := range s {
		switch r {
		case '\\', '"':
			e.WriteByte('\\')
			e.WriteByte(byte(r))
		case '\n':
			e.WriteString("\\n")
		case '\r':
			e.WriteString("\\r")
		case '\t':
			e.WriteString("\\t")
		default:
			e.WriteRune(r)
		}
	}
	e.WriteByte('"')
	var ret string
	if needsQuotes {
		ret = e.String()
	} else {
		ret = string(e.Bytes()[1 : e.Len()-1])
	}
	e.Reset()
	stringBufPool.Put(e)
	return ret
}
