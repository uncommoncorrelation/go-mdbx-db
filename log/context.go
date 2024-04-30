package log

import "context"

type logKey string

// LogKey is intended to be overwritten by users of this library, set to your logger context key type
var LogKey interface{} = logKey("logger")

func FromContext(ctx context.Context) Logger {
	v := ctx.Value(LogKey)
	if v == nil {
		return NewNoop()
	}
	return v.(Logger)
}

func InContext(ctx context.Context, logger Logger) context.Context {
	return context.WithValue(ctx, LogKey, logger)
}
