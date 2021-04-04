package es_writer

import (
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func Test_ExitWhenChannelClosed(t *testing.T) {
	logger, hook := test.NewNullLogger()

	ctn := container()
	ctn.Stop = make(chan bool)
	ctn.Logger = logger
	con, err := ctn.queueConnection()
	assert.NoError(t, err)

	ch, err := ctn.queueChannel(con)
	assert.NoError(t, err)

	_ = ch.Close()
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, 1, len(hook.Entries))
	assert.Equal(t, logrus.ErrorLevel, hook.LastEntry().Level)
}
