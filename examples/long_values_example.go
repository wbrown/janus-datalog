//go:build example
// +build example

package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
)

func main() {
	fmt.Println("Unbounded Value Storage Example")
	fmt.Println("===============================\n")

	// Create entities
	article := datalog.NewIdentity("article:2024-06-19:tech-news")
	author := datalog.NewIdentity("author:jane-doe")

	// Transaction time
	tx := uint64(time.Date(2024, 6, 19, 14, 30, 0, 0, time.UTC).UnixNano())

	// Create datoms with various value sizes
	datoms := []datalog.Datom{
		// Short value
		{
			E:  article,
			A:  datalog.NewKeyword(":article/id"),
			V:  "tech-news-001",
			Tx: tx,
		},
		// Reference
		{
			E:  article,
			A:  datalog.NewKeyword(":article/author"),
			V:  author,
			Tx: tx,
		},
		// Long value - no problem!
		{
			E:  article,
			A:  datalog.NewKeyword(":article/content"),
			V: strings.Repeat("This is a long article about technology trends. ", 10) +
				"The content can be any size without restrictions. " +
				"This demonstrates how the Datalog engine handles large values efficiently.",
			Tx: tx,
		},
		// Another long value
		{
			E:  article,
			A:  datalog.NewKeyword(":article/summary"),
			V:  "This article discusses the latest developments in AI and machine learning technologies",
			Tx: tx,
		},
		// Binary data (could be an image - PNG header repeated)
		{
			E:  article,
			A:  datalog.NewKeyword(":article/thumbnail"),
			V:  []byte(strings.Repeat("\x89PNG\r\n\x1a\n", 20)),
			Tx: tx,
		},
	}

	fmt.Println("Datoms with various value sizes:")
	fmt.Println("=================================")

	for i, d := range datoms {
		var valuePreview string
		var valueSize int

		switch v := d.V.(type) {
		case string:
			valuePreview = v
			valueSize = len(v)
		case []byte:
			valuePreview = fmt.Sprintf("<binary data: %d bytes>", len(v))
			valueSize = len(v)
		case datalog.Identity:
			valuePreview = v.String()
			valueSize = 20 // SHA1 hash size
		default:
			valuePreview = fmt.Sprintf("%v", v)
			valueSize = len(fmt.Sprintf("%v", v))
		}

		if len(valuePreview) > 50 {
			valuePreview = valuePreview[:47] + "..."
		}

		fmt.Printf("\n%d. Entity: %x (first 8 bytes)\n", i+1, d.E.ID())
		fmt.Printf("   Attribute: %s\n", d.A)
		fmt.Printf("   Value: %s\n", valuePreview)
		fmt.Printf("   Value size: %d bytes\n", valueSize)
		fmt.Printf("   Transaction: %d\n", d.Tx)
	}

	fmt.Println("\nKey Points:")
	fmt.Println("============")
	fmt.Println("1. Fixed-size keys (E+A+Tx) for efficient indexing")
	fmt.Println("2. Values can be any size - strings, binary data, references")
	fmt.Println("3. No artificial limits on value size")
	fmt.Println("4. Values stored with type information for deserialization")
	fmt.Println("5. Binary data (images, documents) supported natively")
}
