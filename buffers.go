package es_writer

import (
	"regexp"
	"strconv"

	"github.com/go1com/es-writer/action"
)

func AnyNotEmpty(buffers []*action.Container) bool {
	for _, buf := range buffers {
		if buf.Length() > 0 {
			return true
		}
	}
	return false
}

func GetBufferIndexFromId(id *string, numQueues int) int {
	regex := regexp.MustCompile("[0-9]+")
	if id == nil {
		return 0
	}
	items := regex.FindAllString(*id, -1)
	if len(items) < 1 {
		return 0
	}
	numericId, err := strconv.Atoi(items[len(items)-1])
	if err != nil {
		return 0
	}
	return numericId % numQueues
}

func TotalElements(buffers []*action.Container) int {
	var total = 0
	for _, buf := range buffers {
		total += buf.Length()
	}
	return total
}
