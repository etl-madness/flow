package flow

import (
	"encoding/xml"
	"strings"
	"testing"
)

func TestParseIfChildrenAndMalformedBranches(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		fail bool
	}{
		{"explicit branches", `<if><then><assert id="yes"/></then><else><assert id="no"/></else></if>`, false},
		{"implicit then", `<if><assert id="yes"/><else><assert id="no"/></else></if>`, false},
		{"unterminated if", `<if>`, true},
		{"unterminated then", `<if><then><assert id="yes"/>`, true},
		{"unterminated else", `<if><else><assert id="no"/>`, true},
		{"malformed node", `<if><then><sql`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			decoder := xml.NewDecoder(strings.NewReader(tc.body))
			if _, err := decoder.Token(); err != nil {
				t.Fatal(err)
			}
			index := 0
			thenNodes, elseNodes, err := parseIfChildren(decoder, &index)
			if tc.fail {
				if err == nil {
					t.Fatal("expected malformed XML error")
				}
				return
			}
			if err != nil || len(thenNodes) != 1 || len(elseNodes) != 1 ||
				thenNodes[0].Assert.ID != "yes" || elseNodes[0].Assert.ID != "no" {
				t.Fatalf("parsed branches = %#v, %#v, %v", thenNodes, elseNodes, err)
			}
		})
	}
}
