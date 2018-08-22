package action

import (
	"testing"
	"runtime"
	"path"
	"io/ioutil"
	"strings"
)

func fileGetContent(filePath string) []byte {
	_, currentFileName, _, _ := runtime.Caller(1)
	filePath = path.Dir(currentFileName) + "/../fixtures/" + filePath
	body, _ := ioutil.ReadFile(filePath)

	return body
}

func TestBulkCommandBuiler(t *testing.T) {
	e, err := NewElement(0, fileGetContent("portal/portal-index.json"))
	if err != nil {
		t.Fatalf("failed to create element: %s.", err.Error())
	}

	source, _ := e.Source()
	command := source[0]
	if command != `{"index":{"_index":"go1_qa","_routing":"go1_qa","_type":"portal","_id":"111"}}` {
		t.Error("wrong command line")
	}

	body := source[1]
	if !strings.Contains(string(body), `"title":"qa.mygo1.com"`) {
		t.Error("wrong body line")
	}
}

func TestCreateUpdateRequest(t *testing.T) {
	e, err := NewElement(0, fileGetContent("portal/portal-update.json"))
	if err != nil {
		t.Fatalf("failed to create element: %s.", err.Error())
	} else {

	}

	source, _ := e.Source()
	command := source[0]
	if command != `{"update":{"_index":"go1_qa","_routing":"go1_qa","_type":"portal","_id":"111"}}` {
		t.Error("wrong command line")
	}

	body := source[1]
	if body != `{"doc":{"status":0}}` {
		t.Error("wrong body line")
	}
}
