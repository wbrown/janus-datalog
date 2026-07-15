//go:build ignore

package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	dir := "datalog/storage"
	portable := map[string]bool{
		"backend_cases_test.go":       true,
		"backend_cases_wasm_test.go":  true,
		"backend_contract_test.go":    true,
		"backend_blob_memory_test.go": true,
		"memory_store_test.go":        true,
		"public_contract_test.go":     true,
		"tag_nonportable_tests.go":    true,
	}
	tag := []byte("//go:build !(js && wasm)\n\n")
	entries, err := os.ReadDir(dir)
	if err != nil {
		panic(err)
	}
	tagged := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, "_test.go") || portable[name] {
			continue
		}
		path := filepath.Join(dir, name)
		data, err := os.ReadFile(path)
		if err != nil {
			panic(err)
		}
		if bytes.HasPrefix(data, []byte("//go:build")) || bytes.HasPrefix(data, []byte("// +build")) {
			continue
		}
		if err := os.WriteFile(path, append(tag, data...), 0o644); err != nil {
			panic(err)
		}
		tagged++
		fmt.Println(path)
	}
	fmt.Printf("tagged %d files\n", tagged)
}
