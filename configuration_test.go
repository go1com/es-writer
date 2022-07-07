package es_writer

import (
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"testing"
	"time"
)

func Test_ExitWhenChannelClosed(t *testing.T) {
	ctn := container()
	ctn.Stop = make(chan bool)
	ctn.logger = zap.NewNop()
	con, err := ctn.queueConnection()
	assert.NoError(t, err)

	ch, err := ctn.queueChannel(con)
	assert.NoError(t, err)

	_ = ch.Close()
	time.Sleep(100 * time.Millisecond)
}
