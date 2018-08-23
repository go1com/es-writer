package action

import (
	"encoding/json"
)

type SimpleQuery struct {
	body interface{}
}

func NewSimpleQuery(body interface{}) *SimpleQuery {
	return &SimpleQuery{body: body}
}

func (q *SimpleQuery) Source() (interface{}, error) {
	return q.body, nil
}

// [command-line] {
// 	"update" : {
// 		"_id" : "1",
// 		"_type" : "type1",
// 		"_index" : "test",
// 		"_retry_on_conflict": 3,
//		"_wait_for_active_shards": 1,
// 		"_parent": "…",
// 		"_routing": "…"
// 	}}

type Command struct {
	Index          string `json:"_index"`
	Routing        string `json:"_routing,omitempty"`
	Parent         string `json:"_parent,omitempty"`
	Type           string `json:"_type,omitempty"`
	Id             string `json:"_id,omitempty"`
	RetryOnConfict int    `json:"_retry_on_conflict,omitempty"`
	Refresh        string `json:"_refresh,omitempty"`
}

func NewCommand(e Element) Command {
	return Command{
		Index:          e.Index,
		Routing:        e.Routing,
		Parent:         e.Parent,
		Type:           e.DocType,
		Id:             e.DocId,
		RetryOnConfict: e.RetryOnConflict,
		Refresh:        e.Refresh,
	}
}

func (cmd Command) String(key string) string {
	slice := map[string]Command{key: cmd}
	output, _ := json.Marshal(slice)

	return string(output)
}
