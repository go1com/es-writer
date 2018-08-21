package action

type Actions struct {
	items []Action
}

func NewActions() *Actions {
	return &Actions{
		items: []Action{},
	}
}

func (as *Actions) Add(item Action) {
	as.items = append(as.items, item)
}

func (as *Actions) Clear() {
	as.items = []Action{}
}

func (as *Actions) Length() int {
	return len(as.items)
}

func (as *Actions) Items() []Action {
	return as.items
}
