package action

import "sync"

type Container struct {
	elements []Element
	mutext   *sync.Mutex
}

func NewContainer() *Container {
	return &Container{
		elements: []Element{},
		mutext:   &sync.Mutex{},
	}
}

func (c *Container) Add(element Element) {
	c.mutext.Lock()
	defer c.mutext.Unlock()

	c.elements = append(c.elements, element)

}

func (c *Container) Clear() {
	c.mutext.Lock()
	defer c.mutext.Unlock()

	c.elements = []Element{}
}

func (c *Container) Length() int {
	c.mutext.Lock()
	defer c.mutext.Unlock()

	return len(c.elements)
}

func (c *Container) Elements() []Element {
	return c.elements
}
