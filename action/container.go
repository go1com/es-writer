package action

type Container struct {
	elements []Element
}

func NewContainer() *Container {
	return &Container{
		elements: []Element{},
	}
}

func (c *Container) Add(element Element) {
	c.elements = append(c.elements, element)
}

func (c *Container) Clear() {
	c.elements = []Element{}
}

func (c *Container) Length() int {
	return len(c.elements)
}

func (c *Container) Elements() []Element {
	return c.elements
}
