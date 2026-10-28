package main

import (
	"os/exec"
	"strings"
	"testing"
)

// An add line whose value is a vector is answered with an error, and the
// session continues: the valid line after it is committed and the session
// exits 0. Each case spells the vector differently.
func TestCLI_AnAddLineHoldingAVectorIsAnsweredWithAnError(t *testing.T) {
	binPath := buildCLI(t)
	cases := []struct {
		name   string
		vector string
	}{
		{"two elements", `[1 2]`},
		{"one element", `[1]`},
		{"comma between elements", `[1,2]`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dbPath := createTestDatabase(t)
			cmd := exec.Command(binPath, "-db", dbPath, "-i")
			cmd.Stdin = strings.NewReader(strings.Join([]string{
				".add",
				`alice :person/tags ` + c.vector,
				`bob :person/nickname "Bobby"`,
				``,
				`[:find ?n :where [?e :person/nickname ?n]]`,
				`.exit`,
				``,
			}, "\n"))
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("the session ended with %v:\n%s", err, out)
			}
			text := string(out)
			for _, want := range []string{
				"Error:",             // the line whose value is a vector
				"Committed 1 datoms", // the line after it
				"Bobby",              // the committed value, read back
			} {
				if !strings.Contains(text, want) {
					t.Errorf("the session's output lacks %q:\n%s", want, text)
				}
			}
		})
	}
}
