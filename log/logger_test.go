package log

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_BufferedLoggerAcceptsNilBuffer(t *testing.T) {
	log := NewBuffered(nil)

	log.Info("test", "a", "b", "c", 1, "d", true)

	assert.Contains(t, log.(*BufLogger).Buffer.String(), "lvl=info msg=test a=b c=1 d=true")
}

func Test_BufferedLoggerWritesToBuffer(t *testing.T) {
	buf := new(bytes.Buffer)
	log := NewBuffered(buf)

	log.Info("test", "a", "b", "c", 1, "d", true)

	assert.Contains(t, buf.String(), "lvl=info msg=test a=b c=1 d=true")
}
