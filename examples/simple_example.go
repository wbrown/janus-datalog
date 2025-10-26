//go:build example
// +build example

package main

import (
	"fmt"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
)

func main() {
	// Example: Generic entity modeling
	// This could be any domain - social network, inventory, sensors, etc.
	
	fmt.Println("Generic Datalog Example")
	fmt.Println("======================\n")
	
	// Create some entities - IDs are hashes of unique identifiers
	alice := datalog.NewIdentity("user:alice")
	bob := datalog.NewIdentity("user:bob") 
	post1 := datalog.NewIdentity("post:2024-06-19:alice:1")
	
	// Transaction IDs from timestamps
	tx1 := uint64(time.Date(2024, 6, 19, 10, 0, 0, 0, time.UTC).Unix())
	tx2 := uint64(time.Date(2024, 6, 19, 10, 30, 0, 0, time.UTC).Unix())
	tx3 := uint64(time.Date(2024, 6, 19, 11, 0, 0, 0, time.UTC).Unix())
	
	// Create datoms - these could represent any domain
	datoms := []datalog.Datom{
		// Facts about Alice
		{E: alice, A: datalog.NewKeyword(":user/name"), V: "Alice Smith", Tx: tx1},
		{E: alice, A: datalog.NewKeyword(":user/email"), V: "alice@example.com", Tx: tx1},
		{E: alice, A: datalog.NewKeyword(":user/joined"), V: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), Tx: tx1},
		
		// Facts about Bob  
		{E: bob, A: datalog.NewKeyword(":user/name"), V: "Bob Jones", Tx: tx1},
		{E: bob, A: datalog.NewKeyword(":user/email"), V: "bob@example.com", Tx: tx1},
		
		// Alice follows Bob
		{E: alice, A: datalog.NewKeyword(":user/follows"), V: bob, Tx: tx2},
		
		// A post by Alice
		{E: post1, A: datalog.NewKeyword(":post/author"), V: alice, Tx: tx3},
		{E: post1, A: datalog.NewKeyword(":post/content"), V: "Hello Datalog!", Tx: tx3},
		{E: post1, A: datalog.NewKeyword(":post/likes"), V: int64(42), Tx: tx3},
		
		// Bob likes the post (same transaction)
		{E: post1, A: datalog.NewKeyword(":post/liked-by"), V: bob, Tx: tx3},
	}
	
	fmt.Println("Datoms:")
	for _, d := range datoms {
		fmt.Printf("  %s\n", d)
	}
	
	// Example patterns that could match these datoms
	fmt.Println("\nExample Query Patterns:")
	fmt.Println("=======================")
	
	// Find all users
	fmt.Println("\n1. Find all user names:")
	fmt.Println("   [?user :user/name ?name ?tx]")
	
	// Find who Alice follows
	fmt.Println("\n2. Find who Alice follows:")
	fmt.Println("   [alice :user/follows ?who ?tx]")
	
	// Find posts with more than 10 likes
	fmt.Println("\n3. Find popular posts:")
	fmt.Println("   [?post :post/likes ?likes ?tx]")
	fmt.Println("   [(> ?likes 10)]")
	
	// Transaction-based query
	fmt.Println("\n4. Find facts added after a certain transaction:")
	fmt.Println("   [?e ?a ?v ?tx]")
	fmt.Println("   [(> ?tx tx:17d9a0dfae800000)]")
	
	// Graph traversal
	fmt.Println("\n5. Find posts by people Alice follows:")
	fmt.Println("   [alice :user/follows ?person _]")
	fmt.Println("   [?post :post/author ?person _]")
	fmt.Println("   [?post :post/content ?content _]")
	
	fmt.Println("\nNote: The Datalog engine is completely domain-agnostic.")
	fmt.Println("These same patterns work whether modeling:")
	fmt.Println("- Social networks (users, posts, follows)")
	fmt.Println("- Finance (stocks, prices, trades)")
	fmt.Println("- IoT (sensors, readings, alerts)")
	fmt.Println("- Any graph-structured, time-aware data!")
}
