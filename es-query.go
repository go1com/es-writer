package es_writer

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/go1com/es-writer/action"
)

func GetDocumentIdFromUri(uri string) *string {
	// Get parts of URI
	info := strings.Split(uri, "/")

	if len(info) >= 4 {
		// Return document id if it exists
		res, err := regexp.MatchString("[0-9]+", info[3])
		if res && err == nil {
			return &info[3]
		}
	}

	return nil
}

func GetDocumentIdFromBody(body interface{}) *string {
	// Stringify json
	bodyStr := fmt.Sprint(body)

	// Split into string before _id and after
	parts := strings.SplitN(bodyStr, "_id:", 2)
	if len(parts) < 2 {
		return nil
	}

	// Extract string before closing bracket or space
	field := strings.SplitN(parts[1], "]", 2)[0]
	field = strings.SplitN(field, " ", 2)[0]
	return &field
}

func GetDocumentIdFromElement(query *action.Element) *string {

	// Return nil if element does not exist
	if query == nil {
		return nil
	}

	// Get document id from uri
	uriDocId := GetDocumentIdFromUri(query.Uri)

	if uriDocId != nil {
		return uriDocId
	}

	// Return document id from body
	return GetDocumentIdFromBody(query.Body)
}
