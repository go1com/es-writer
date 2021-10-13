package action

import "sync"

type Buffer struct {
	elements []Element
	mutex    *sync.RWMutex
}

func NewBuffer() *Buffer {
	return &Buffer{
		elements: []Element{},
		mutex:    &sync.RWMutex{},
	}
}
