package flow

import (
	"bytes"
	"testing"
)

func TestGetSchemaXSD(t *testing.T) {
	xsdBytes := GetSchemaXSD()
	if len(xsdBytes) == 0 {
		t.Fatal("expected embedded XSD schema to be non-empty, but got 0 bytes")
	}

	// Verify it contains basic XML and schema elements
	if !bytes.Contains(xsdBytes, []byte("<xs:schema")) {
		t.Error("expected embedded XSD schema to contain '<xs:schema' element declaration")
	}

	if !bytes.Contains(xsdBytes, []byte("<xs:element name=\"pipeline\"")) {
		t.Error("expected embedded XSD schema to contain pipeline element definition")
	}
}
