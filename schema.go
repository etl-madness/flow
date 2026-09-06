package flow

import (
	_ "embed"
)

//go:embed xsd/pipeline.xsd
var schemaXSD []byte

// GetSchemaXSD returns the embedded XSD schema content as a byte slice.
func GetSchemaXSD() []byte {
	return schemaXSD
}

