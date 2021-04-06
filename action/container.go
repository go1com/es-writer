package action

import "sync"

type Container struct {
	elements []Element
	mutex    *sync.RWMutex
}

func NewContainer() *Container {
	return &Container{
		elements: []Element{},
		mutex:    &sync.RWMutex{},
	}
}

func (c *Container) Add(element Element) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.elements = append(c.elements, element)

}

func (c *Container) Clear() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.elements = []Element{}
}

func (c *Container) Length() int {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return len(c.elements)
}

func (c *Container) Elements() []Element {
	return c.elements
}
