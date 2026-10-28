package main

import (
	"os/exec"
	"strings"
	"testing"
)

// The interactive add line reads its three fields as the EDN forms -in reads:
// an entity as a symbol taken as a seed or as an identity literal, an
// attribute as a keyword, and a value of any EDN type. A line that is not
// three such forms is answered with an error and the session continues.
func TestCLI_TheAddLineReadsEDNForms(t *testing.T) {
	binPath := buildCLI(t)
	dbPath := createTestDatabase(t)

	cmd := exec.Command(binPath, "-db", dbPath, "-i")
	cmd.Stdin = strings.NewReader(strings.Join([]string{
		".add",
		`alice :person/name "Alice Smith"`,
		`alice : "x"`,
		`alice :person/age 1234`,
		`#id "carol" :person/name "Carol"`,
		`alice :person/tag :tag/vip`,
		``,
		`[:find ?n :where [?e :person/name ?n]]`,
		`[:find ?a :where [?e :person/age ?a]]`,
		`[:find ?t :where [?e :person/tag ?t]]`,
		`.exit`,
		``,
	}, "\n"))
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("the session ended with %v:\n%s", err, out)
	}
	text := string(out)
	for _, want := range []string{
		"Error:",             // the line whose attribute is no keyword
		"Committed 4 datoms", // the four lines that are three forms
		"Alice Smith",        // a string holding a space
		"Carol",              // an entity written as an identity literal
		"1234",               // an integer value
		":tag/vip",           // a keyword value
	} {
		if !strings.Contains(text, want) {
			t.Errorf("the session's output lacks %q:\n%s", want, text)
		}
	}
}
