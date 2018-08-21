package main

import (
	"go1/a-wip"
	"fmt"
)

func main() {
	var input []byte

	if true {
		input = []byte(`
			{
				"http_method": "POST", 
				"uri":  "/go1_dev/portal/111/_create?routing=go1_dev&parent=333", 
				"body": {
					"id": 111
				}
			}
`)
		msg, err := a_wip.NewInputMessage(input)
		if err != nil {
			return
		}

		fmt.Printf("BODY: %s", msg.Body)
	}
}
