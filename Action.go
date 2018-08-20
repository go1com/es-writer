package a_wip

import "github.com/streadway/amqp"

type Action struct {
	amqp.Delivery
}

type Actions struct {
	items []Action
}

func NewActions() *Actions {
	return &Actions{
		items: []Action{},
	}
}

func (a *Actions) Add(m amqp.Delivery) {
	a.items = append(a.items, Action{m})
}

func (a *Actions) Clear() {
	a.items = []Action{}
}

func (a *Actions) Length() int {
	return len(a.items)
}

func (a *Actions) Items() []Action {
	return a.items
}
