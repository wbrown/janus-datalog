//go:build ignore

package main

import (
	"bytes"
	"fmt"
	"go/build/constraint"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	apply := false
	for _, arg := range os.Args[1:] {
		if arg == "--apply" {
			apply = true
		}
	}

	dir := "datalog/storage"
	portable := map[string]bool{
		"backend_cases_test.go":        true,
		"backend_cases_wasm_test.go":   true,
		"backend_contract_test.go":     true,
		"backend_blob_memory_test.go":  true,
		"memory_store_test.go":         true,
		"memory_backend_bench_test.go": true,
		"public_contract_test.go":      true,
		"tag_nonportable_tests.go":     true,
	}
	tag := []byte("//go:build !(js && wasm)\n\n")
	entries, err := os.ReadDir(dir)
	if err != nil {
		panic(err)
	}
	tagged := 0
	uncoveredConstraint := 0
	missingConstraint := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := filepath.Join(dir, name)
		data, err := os.ReadFile(path)
		if err != nil {
			panic(err)
		}
		if portable[name] {
			continue
		}
		if bytes.HasPrefix(data, []byte("//go:build")) || bytes.HasPrefix(data, []byte("// +build")) {
			// Already constrained files must exclude js/wasm explicitly. Leaving
			// e.g. //go:build !race untagged pulls Badger-only tests into wasm.
			if !buildConstraintExcludesJSWASM(data) {
				fmt.Fprintf(os.Stderr, "error: %s has a build constraint without js/wasm exclusion; add !(js && wasm) or list the file in the portable allowlist\n", path)
				uncoveredConstraint++
			}
			continue
		}
		// No build constraint and not on the portable allowlist.
		// Fail closed: do not silently tag (that wrongly excludes forgotten
		// portable tests from wasm). Require an explicit --apply to add the
		// nonportable tag, or list the file in the allowlist.
		fmt.Fprintf(os.Stderr, "error: %s has no build constraint; add !(js && wasm) or list the file in the portable allowlist", path)
		if apply {
			if err := os.WriteFile(path, append(tag, data...), 0o644); err != nil {
				panic(err)
			}
			fmt.Fprintf(os.Stderr, " (tagged with --apply)\n")
			tagged++
			fmt.Println(path)
		} else {
			fmt.Fprintf(os.Stderr, " (re-run with --apply to tag as nonportable)\n")
			missingConstraint++
		}
	}
	fmt.Printf("tagged %d files\n", tagged)
	if uncoveredConstraint > 0 || missingConstraint > 0 {
		os.Exit(1)
	}
}

// buildConstraintExcludesJSWASM reports whether the file's leading build
// constraint lines evaluate to false under the js+wasm tag pair (i.e. the
// tests are excluded from that build). Parses the constraint expression; does
// not substring-match comment text.
func buildConstraintExcludesJSWASM(src []byte) bool {
	expr, ok := parseLeadingBuildConstraint(src)
	if !ok || expr == nil {
		return false
	}
	return !expr.Eval(func(tag string) bool {
		switch tag {
		case "js", "wasm":
			return true
		default:
			return false
		}
	})
}

func parseLeadingBuildConstraint(src []byte) (constraint.Expr, bool) {
	var expr constraint.Expr
	rest := src
	for len(rest) > 0 {
		var line []byte
		if i := bytes.IndexByte(rest, '\n'); i >= 0 {
			line = rest[:i]
			rest = rest[i+1:]
		} else {
			line = rest
			rest = nil
		}
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			if expr != nil {
				break
			}
			continue
		}
		if !bytes.HasPrefix(line, []byte("//go:build")) && !bytes.HasPrefix(line, []byte("// +build")) {
			break
		}
		next, err := constraint.Parse(string(line))
		if err != nil {
			return nil, false
		}
		if expr == nil {
			expr = next
			continue
		}
		expr = &constraint.AndExpr{X: expr, Y: next}
	}
	if expr == nil {
		return nil, false
	}
	return expr, true
}
