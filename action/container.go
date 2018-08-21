package action

type Container struct {
	items []Element
}

func NewContainer() *Container {
	return &Container{
		items: []Element{},
	}
}

func (c *Container) Add(item Element) {
	c.items = append(c.items, item)
}

func (c *Container) Clear() {
	c.items = []Element{}
}

func (c *Container) Length() int {
	return len(c.items)
}

func (c *Container) Items() []Element {
	return c.items
}
