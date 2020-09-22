package es_writer

import (
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_ExitWhenChannelClosed(t *testing.T)  {
	logger, hook := test.NewNullLogger()

	f := container()
	f.Stop = make(chan bool)
	f.Logger = logger

	con, err := f.queueConnection()
	if err != nil {
		t.Error(err)
	}

	ch, err := f.queueChannel(con)
	if err != nil {
		t.Error(err)
	}

	ch.Close()

	assert.Equal(t, 1, len(hook.Entries))
	assert.Equal(t, logrus.ErrorLevel, hook.LastEntry().Level)
}