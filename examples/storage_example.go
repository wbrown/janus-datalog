//go:build example
// +build example

package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

func main() {
	// Create a temporary directory for the database
	dir, err := os.MkdirTemp("", "datalog-example-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	
	fmt.Println("BadgerDB Storage Example")
	fmt.Println("========================\n")
	fmt.Printf("Database directory: %s\n\n", dir)
	
	// Open the store with L85 encoding for human-readable keys
	encoder := storage.NewKeyEncoder(storage.L85Strategy)
	store, err := storage.NewBadgerStore(dir, encoder)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()
	
	// Create query builder with same encoder
	qb := storage.NewQueryBuilder(store, encoder)
	
	// Create entities
	alice := datalog.NewIdentity("user:alice")
	bob := datalog.NewIdentity("user:bob")
	charlie := datalog.NewIdentity("user:charlie")
	post1 := datalog.NewIdentity("post:2024-06-19:alice:1")
	post2 := datalog.NewIdentity("post:2024-06-19:bob:1")
	
	// Transaction times
	tx1 := uint64(time.Date(2024, 6, 19, 10, 0, 0, 0, time.UTC).UnixNano())
	tx2 := uint64(time.Date(2024, 6, 19, 11, 0, 0, 0, time.UTC).UnixNano())
	tx3 := uint64(time.Date(2024, 6, 19, 12, 0, 0, 0, time.UTC).UnixNano())
	
	// Create initial datoms
	fmt.Println("1. Asserting initial facts...")
	initialDatoms := []datalog.Datom{
		// User data
		{E: alice, A: datalog.NewKeyword(":user/name"), V: "Alice Smith", Tx: tx1},
		{E: alice, A: datalog.NewKeyword(":user/email"), V: "alice@example.com", Tx: tx1},
		{E: bob, A: datalog.NewKeyword(":user/name"), V: "Bob Jones", Tx: tx1},
		{E: bob, A: datalog.NewKeyword(":user/email"), V: "bob@example.com", Tx: tx1},
		{E: charlie, A: datalog.NewKeyword(":user/name"), V: "Charlie Brown", Tx: tx1},

		// Social connections
		{E: alice, A: datalog.NewKeyword(":user/follows"), V: bob, Tx: tx2},
		{E: bob, A: datalog.NewKeyword(":user/follows"), V: charlie, Tx: tx2},
		{E: charlie, A: datalog.NewKeyword(":user/follows"), V: alice, Tx: tx2},

		// Posts
		{E: post1, A: datalog.NewKeyword(":post/author"), V: alice, Tx: tx3},
		{E: post1, A: datalog.NewKeyword(":post/content"), V: "Hello Datalog!", Tx: tx3},
		{E: post2, A: datalog.NewKeyword(":post/author"), V: bob, Tx: tx3},
		{E: post2, A: datalog.NewKeyword(":post/content"), V: "BadgerDB is fast!", Tx: tx3},
	}
	
	err = store.Assert(initialDatoms)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("✓ Asserted", len(initialDatoms), "facts\n")
	
	// Query examples
	fmt.Println("2. Query Examples")
	fmt.Println("-----------------")
	
	// Get all facts about Alice
	fmt.Println("\n2.1 All facts about Alice:")
	aliceFacts, err := qb.GetEntity(alice)
	if err != nil {
		log.Fatal(err)
	}
	for _, d := range aliceFacts {
		fmt.Printf("  %s = %v (at %s)\n", d.A, d.V, d.Tx)
	}
	
	// Find all users
	fmt.Println("\n2.2 All users (by name attribute):")
	nameAttr := datalog.NewKeyword(":user/name")
	users, err := qb.GetAttribute(nameAttr)
	if err != nil {
		log.Fatal(err)
	}
	for _, d := range users {
		fmt.Printf("  Entity %x: %v\n", d.E.ID(), d.V)
	}
	
	// Find who follows Bob
	fmt.Println("\n2.3 Who follows Bob:")
	followsAttr := datalog.NewKeyword(":user/follows")
	followers, err := qb.GetAttributeValue(followsAttr, bob)
	if err != nil {
		log.Fatal(err)
	}
	for _, d := range followers {
		// Look up the follower's name
		nameDatoms, _ := qb.GetEntityAttribute(d.E, nameAttr)
		if len(nameDatoms) > 0 {
			fmt.Printf("  %v follows Bob\n", nameDatoms[0].V)
		}
	}
	
	// Find all posts
	fmt.Println("\n2.4 All posts with authors:")
	postAttr := datalog.NewKeyword(":post/content")
	posts, err := qb.GetAttribute(postAttr)
	if err != nil {
		log.Fatal(err)
	}
	for _, d := range posts {
		// Get author
		authorAttr := datalog.NewKeyword(":post/author")
		authorDatoms, _ := qb.GetEntityAttribute(d.E, authorAttr)
		if len(authorDatoms) > 0 {
			authorEntity := authorDatoms[0].V.(datalog.Identity)
			
			// Get author name
			authorName, _ := qb.GetEntityAttribute(authorEntity, nameAttr)
			if len(authorName) > 0 {
				fmt.Printf("  %v posted: %v\n", authorName[0].V, d.V)
			}
		}
	}
	
	// Time-based query
	fmt.Println("\n2.5 Facts added between tx1 and tx2:")
	timeRangeFacts, err := qb.GetTimeRange(tx1, tx2)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  Found %d facts in time range\n", len(timeRangeFacts))
	
	// Transaction example
	fmt.Println("\n3. Transaction Example")
	fmt.Println("----------------------")
	
	tx, err := store.BeginTx()
	if err != nil {
		log.Fatal(err)
	}
	
	// Add a like in a transaction
	tx4 := uint64(time.Date(2024, 6, 19, 13, 0, 0, 0, time.UTC).UnixNano())
	err = tx.Assert([]datalog.Datom{
		{E: post1, A: datalog.NewKeyword(":post/likes"), V: int64(42), Tx: tx4},
		{E: post1, A: datalog.NewKeyword(":post/liked-by"), V: bob, Tx: tx4},
	})
	if err != nil {
		tx.Rollback()
		log.Fatal(err)
	}
	
	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("✓ Transaction committed: Bob liked Alice's post")
	
	// Verify the transaction worked
	likes, err := qb.GetEntityAttribute(post1, datalog.NewKeyword(":post/likes"))
	if err != nil {
		log.Fatal(err)
	}
	if len(likes) > 0 {
		fmt.Printf("  Post now has %v likes\n", likes[0].V)
	}
	
	// References query
	fmt.Println("\n4. Reference Queries")
	fmt.Println("--------------------")
	
	fmt.Println("\n4.1 All references to Alice:")
	aliceRefs, err := qb.GetReferences(alice)
	if err != nil {
		log.Fatal(err)
	}
	for _, d := range aliceRefs {
		fmt.Printf("  Entity %x has %s pointing to Alice\n", d.E.ID(), d.A)
	}
	
	// Retraction example
	fmt.Println("\n5. Retraction Example")
	fmt.Println("---------------------")
	
	// Retract Charlie's email (oops, typo!)
	err = store.Retract([]datalog.Datom{
		{E: charlie, A: datalog.NewKeyword(":user/email"), V: "charlie@example.com", Tx: tx1},
	})
	if err == nil {
		fmt.Println("✓ Retracted Charlie's (non-existent) email")
	}

	// Add correct email
	tx5 := uint64(time.Date(2024, 6, 19, 14, 0, 0, 0, time.UTC).UnixNano())
	err = store.Assert([]datalog.Datom{
		{E: charlie, A: datalog.NewKeyword(":user/email"), V: "charlie.brown@example.com", Tx: tx5},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("✓ Added Charlie's correct email")
	
	fmt.Println("\nStorage example complete!")
}
