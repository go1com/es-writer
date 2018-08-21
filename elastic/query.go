package elastic

type SimpleQuery struct {
	body interface{}
}

func NewSimpleQuery(body interface{}) *SimpleQuery {
	return &SimpleQuery{body: body}
}

func (q *SimpleQuery) Source() (interface{}, error) {
	return q.body, nil
}
