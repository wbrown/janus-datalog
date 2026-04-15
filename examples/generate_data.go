//go:build ignore

// generate_data.go creates the shared EDN datasets used by examples.
// Run from the repository root:
//
//	go run examples/generate_data.go
package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

func main() {
	generatePeople()
	generateSecurities()
	fmt.Println("Done. Generated examples/data/people.edn and examples/data/securities.edn")
}

func generatePeople() {
	tmpDir, err := os.MkdirTemp("", "gendata-people-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	s := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).Add().
		Attribute(":person/age").Type(schema.TypeLong).Add().
		Attribute(":person/email").Type(schema.TypeString).Add().
		Attribute(":person/city").Type(schema.TypeString).Add().
		Attribute(":person/salary").Type(schema.TypeDouble).Add().
		Attribute(":person/active").Type(schema.TypeBoolean).Add().
		Attribute(":person/nickname").Type(schema.TypeString).Add().
		Attribute(":person/skills").Type(schema.TypeString).Many().Add().
		Attribute(":person/joined").Type(schema.TypeInstant).Add().
		Attribute(":person/department").Type(schema.TypeRef).Add().
		Attribute(":person/manager").Type(schema.TypeRef).Add().
		Attribute(":dept/name").Type(schema.TypeString).Add().
		Attribute(":dept/location").Type(schema.TypeString).Add().
		Attribute(":dept/budget").Type(schema.TypeDouble).Add().
		MustBuild()

	d, err := db.Open(tmpDir+"/people.db", db.WithSchema(s))
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()

	// Departments
	engineering := datalog.NewIdentity("dept:engineering")
	sales := datalog.NewIdentity("dept:sales")
	marketing := datalog.NewIdentity("dept:marketing")
	executive := datalog.NewIdentity("dept:executive")

	tx := d.NewTransaction()
	tx.Add(engineering, datalog.NewKeyword(":dept/name"), "Engineering")
	tx.Add(engineering, datalog.NewKeyword(":dept/location"), "San Francisco")
	tx.Add(engineering, datalog.NewKeyword(":dept/budget"), 2500000.0)

	tx.Add(sales, datalog.NewKeyword(":dept/name"), "Sales")
	tx.Add(sales, datalog.NewKeyword(":dept/location"), "New York")
	tx.Add(sales, datalog.NewKeyword(":dept/budget"), 1800000.0)

	tx.Add(marketing, datalog.NewKeyword(":dept/name"), "Marketing")
	tx.Add(marketing, datalog.NewKeyword(":dept/location"), "New York")
	tx.Add(marketing, datalog.NewKeyword(":dept/budget"), 1200000.0)

	tx.Add(executive, datalog.NewKeyword(":dept/name"), "Executive")
	tx.Add(executive, datalog.NewKeyword(":dept/location"), "San Francisco")
	tx.Add(executive, datalog.NewKeyword(":dept/budget"), 3000000.0)

	if _, err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	// People
	type person struct {
		id       string
		name     string
		age      int64
		email    string
		city     string
		salary   float64
		active   bool
		nickname string
		skills   []string
		joined   time.Time
		dept     datalog.Identity
		manager  datalog.Identity
	}

	// Managers first (so we can reference them)
	alice := datalog.NewIdentity("person:alice")
	bob := datalog.NewIdentity("person:bob")
	carol := datalog.NewIdentity("person:carol")
	dave := datalog.NewIdentity("person:dave")

	people := []person{
		{"person:alice", "Alice Chen", 42, "alice@example.com", "San Francisco", 185000, true, "ace", []string{"go", "python", "leadership"}, time.Date(2018, 3, 15, 0, 0, 0, 0, time.UTC), engineering, nil},
		{"person:bob", "Bob Smith", 38, "bob@example.com", "New York", 165000, true, "", []string{"sales", "negotiation", "analytics"}, time.Date(2019, 7, 1, 0, 0, 0, 0, time.UTC), sales, nil},
		{"person:carol", "Carol Davis", 45, "carol@example.com", "New York", 170000, true, "", []string{"marketing", "strategy", "analytics"}, time.Date(2017, 1, 10, 0, 0, 0, 0, time.UTC), marketing, nil},
		{"person:dave", "Dave Wilson", 52, "dave@example.com", "San Francisco", 250000, true, "", []string{"leadership", "strategy"}, time.Date(2015, 6, 1, 0, 0, 0, 0, time.UTC), executive, nil},

		{"person:eve", "Eve Johnson", 29, "eve@example.com", "San Francisco", 135000, true, "evie", []string{"go", "rust", "databases"}, time.Date(2021, 9, 1, 0, 0, 0, 0, time.UTC), engineering, alice},
		{"person:frank", "Frank Lee", 34, "frank@example.com", "San Francisco", 145000, true, "", []string{"go", "javascript", "react"}, time.Date(2020, 4, 15, 0, 0, 0, 0, time.UTC), engineering, alice},
		{"person:grace", "Grace Kim", 31, "", "San Francisco", 140000, true, "", []string{"python", "ml", "data-science"}, time.Date(2022, 1, 10, 0, 0, 0, 0, time.UTC), engineering, alice},
		{"person:henry", "Henry Park", 27, "henry@example.com", "San Francisco", 125000, true, "hank", []string{"go", "kubernetes"}, time.Date(2023, 3, 1, 0, 0, 0, 0, time.UTC), engineering, alice},
		{"person:iris", "Iris Zhang", 33, "iris@example.com", "San Francisco", 150000, true, "", []string{"go", "security", "networking"}, time.Date(2020, 8, 15, 0, 0, 0, 0, time.UTC), engineering, alice},

		{"person:jack", "Jack Brown", 36, "jack@example.com", "New York", 130000, true, "", []string{"sales", "crm"}, time.Date(2020, 2, 1, 0, 0, 0, 0, time.UTC), sales, bob},
		{"person:kate", "Kate Miller", 28, "", "New York", 115000, true, "", []string{"sales", "marketing"}, time.Date(2022, 6, 1, 0, 0, 0, 0, time.UTC), sales, bob},
		{"person:leo", "Leo Garcia", 41, "leo@example.com", "Chicago", 125000, true, "", []string{"sales", "analytics", "forecasting"}, time.Date(2019, 11, 1, 0, 0, 0, 0, time.UTC), sales, bob},
		{"person:mia", "Mia Thompson", 30, "mia@example.com", "New York", 120000, false, "", []string{"sales"}, time.Date(2021, 5, 15, 0, 0, 0, 0, time.UTC), sales, bob},

		{"person:nick", "Nick Adams", 32, "nick@example.com", "New York", 118000, true, "", []string{"marketing", "content", "seo"}, time.Date(2021, 3, 1, 0, 0, 0, 0, time.UTC), marketing, carol},
		{"person:olivia", "Olivia White", 26, "", "Chicago", 110000, true, "liv", []string{"marketing", "design", "social-media"}, time.Date(2023, 1, 15, 0, 0, 0, 0, time.UTC), marketing, carol},
		{"person:peter", "Peter Jones", 39, "peter@example.com", "New York", 128000, true, "", []string{"marketing", "analytics", "data-science"}, time.Date(2020, 9, 1, 0, 0, 0, 0, time.UTC), marketing, carol},

		{"person:quinn", "Quinn Taylor", 35, "quinn@example.com", "San Francisco", 200000, true, "", []string{"leadership", "finance"}, time.Date(2019, 4, 1, 0, 0, 0, 0, time.UTC), executive, dave},
		{"person:rachel", "Rachel Scott", 44, "rachel@example.com", "San Francisco", 210000, true, "", []string{"leadership", "operations"}, time.Date(2018, 10, 1, 0, 0, 0, 0, time.UTC), executive, dave},
	}

	name := datalog.NewKeyword(":person/name")
	age := datalog.NewKeyword(":person/age")
	email := datalog.NewKeyword(":person/email")
	city := datalog.NewKeyword(":person/city")
	salary := datalog.NewKeyword(":person/salary")
	active := datalog.NewKeyword(":person/active")
	nickname := datalog.NewKeyword(":person/nickname")
	skills := datalog.NewKeyword(":person/skills")
	joined := datalog.NewKeyword(":person/joined")
	dept := datalog.NewKeyword(":person/department")
	manager := datalog.NewKeyword(":person/manager")

	tx = d.NewTransaction()
	for _, p := range people {
		id := datalog.NewIdentity(p.id)
		tx.Add(id, name, p.name)
		tx.Add(id, age, p.age)
		if p.email != "" {
			tx.Add(id, email, p.email)
		}
		tx.Add(id, city, p.city)
		tx.Add(id, salary, p.salary)
		tx.Add(id, active, p.active)
		if p.nickname != "" {
			tx.Add(id, nickname, p.nickname)
		}
		for _, sk := range p.skills {
			tx.Add(id, skills, sk)
		}
		tx.Add(id, joined, p.joined)
		tx.Add(id, dept, p.dept)
		if p.manager != nil {
			tx.Add(id, manager, p.manager)
		}
	}

	// Suppress unused variable warnings
	_ = alice
	_ = bob
	_ = carol
	_ = dave

	if _, err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	f, err := os.Create("examples/data/people.edn")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	if err := d.Export(f); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Generated examples/data/people.edn")
}

func generateSecurities() {
	tmpDir, err := os.MkdirTemp("", "gendata-securities-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	d, err := db.Open(tmpDir + "/securities.db")
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()

	ticker := datalog.NewKeyword(":security/ticker")
	secName := datalog.NewKeyword(":security/name")
	sector := datalog.NewKeyword(":security/sector")
	exchange := datalog.NewKeyword(":security/exchange")

	priceSec := datalog.NewKeyword(":price/security")
	priceDate := datalog.NewKeyword(":price/date")
	priceOpen := datalog.NewKeyword(":price/open")
	priceHigh := datalog.NewKeyword(":price/high")
	priceLow := datalog.NewKeyword(":price/low")
	priceClose := datalog.NewKeyword(":price/close")
	priceVolume := datalog.NewKeyword(":price/volume")

	// Securities
	aapl := datalog.NewIdentity("sec:AAPL")
	googl := datalog.NewIdentity("sec:GOOGL")
	msft := datalog.NewIdentity("sec:MSFT")
	amzn := datalog.NewIdentity("sec:AMZN")
	tsla := datalog.NewIdentity("sec:TSLA")

	tx := d.NewTransaction()
	for _, sec := range []struct {
		id   datalog.Identity
		tick string
		name string
		sect string
		exch string
	}{
		{aapl, "AAPL", "Apple Inc.", "Technology", "NASDAQ"},
		{googl, "GOOGL", "Alphabet Inc.", "Technology", "NASDAQ"},
		{msft, "MSFT", "Microsoft Corp.", "Technology", "NASDAQ"},
		{amzn, "AMZN", "Amazon.com Inc.", "Consumer Cyclical", "NASDAQ"},
		{tsla, "TSLA", "Tesla Inc.", "Consumer Cyclical", "NASDAQ"},
	} {
		tx.Add(sec.id, ticker, sec.tick)
		tx.Add(sec.id, secName, sec.name)
		tx.Add(sec.id, sector, sec.sect)
		tx.Add(sec.id, exchange, sec.exch)
	}
	if _, err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	// Price data: 10 trading days starting 2025-01-06
	type priceRecord struct {
		sec    datalog.Identity
		date   time.Time
		open   float64
		high   float64
		low    float64
		close  float64
		volume int64
	}

	baseDate := time.Date(2025, 1, 6, 16, 0, 0, 0, time.UTC) // market close time
	prices := []priceRecord{
		// AAPL
		{aapl, baseDate, 243.50, 245.80, 242.10, 244.20, 45_000_000},
		{aapl, baseDate.AddDate(0, 0, 1), 244.20, 247.30, 243.90, 246.80, 42_000_000},
		{aapl, baseDate.AddDate(0, 0, 2), 246.80, 248.50, 245.60, 247.90, 38_000_000},
		{aapl, baseDate.AddDate(0, 0, 3), 247.90, 249.10, 246.20, 248.50, 41_000_000},
		{aapl, baseDate.AddDate(0, 0, 4), 248.50, 250.30, 247.80, 249.70, 39_000_000},
		{aapl, baseDate.AddDate(0, 0, 7), 249.70, 251.20, 248.30, 250.80, 43_000_000},
		{aapl, baseDate.AddDate(0, 0, 8), 250.80, 252.40, 249.90, 251.60, 37_000_000},
		{aapl, baseDate.AddDate(0, 0, 9), 251.60, 253.10, 250.40, 252.30, 36_000_000},
		{aapl, baseDate.AddDate(0, 0, 10), 252.30, 253.80, 251.10, 253.20, 40_000_000},
		{aapl, baseDate.AddDate(0, 0, 11), 253.20, 255.00, 252.50, 254.10, 44_000_000},
		// GOOGL
		{googl, baseDate, 192.30, 194.50, 191.80, 193.70, 28_000_000},
		{googl, baseDate.AddDate(0, 0, 1), 193.70, 195.20, 192.90, 194.80, 25_000_000},
		{googl, baseDate.AddDate(0, 0, 2), 194.80, 196.40, 194.10, 195.90, 27_000_000},
		{googl, baseDate.AddDate(0, 0, 3), 195.90, 197.10, 195.00, 196.50, 24_000_000},
		{googl, baseDate.AddDate(0, 0, 4), 196.50, 198.30, 195.80, 197.80, 26_000_000},
		{googl, baseDate.AddDate(0, 0, 7), 197.80, 199.10, 196.90, 198.40, 29_000_000},
		{googl, baseDate.AddDate(0, 0, 8), 198.40, 200.20, 197.80, 199.50, 31_000_000},
		{googl, baseDate.AddDate(0, 0, 9), 199.50, 201.30, 198.90, 200.70, 30_000_000},
		{googl, baseDate.AddDate(0, 0, 10), 200.70, 202.10, 199.80, 201.40, 28_000_000},
		{googl, baseDate.AddDate(0, 0, 11), 201.40, 203.00, 200.60, 202.50, 27_000_000},
		// MSFT
		{msft, baseDate, 420.10, 423.50, 418.30, 421.80, 22_000_000},
		{msft, baseDate.AddDate(0, 0, 1), 421.80, 425.20, 420.50, 424.30, 20_000_000},
		{msft, baseDate.AddDate(0, 0, 2), 424.30, 426.80, 423.10, 425.90, 19_000_000},
		{msft, baseDate.AddDate(0, 0, 3), 425.90, 428.10, 424.50, 427.20, 21_000_000},
		{msft, baseDate.AddDate(0, 0, 4), 427.20, 429.50, 426.00, 428.80, 18_000_000},
		{msft, baseDate.AddDate(0, 0, 7), 428.80, 431.20, 427.60, 430.50, 23_000_000},
		{msft, baseDate.AddDate(0, 0, 8), 430.50, 432.80, 429.30, 431.70, 17_000_000},
		{msft, baseDate.AddDate(0, 0, 9), 431.70, 433.90, 430.50, 432.80, 19_000_000},
		{msft, baseDate.AddDate(0, 0, 10), 432.80, 435.10, 431.60, 434.20, 20_000_000},
		{msft, baseDate.AddDate(0, 0, 11), 434.20, 436.50, 433.00, 435.60, 22_000_000},
		// AMZN
		{amzn, baseDate, 220.40, 223.10, 219.50, 222.30, 35_000_000},
		{amzn, baseDate.AddDate(0, 0, 1), 222.30, 224.80, 221.60, 223.90, 33_000_000},
		{amzn, baseDate.AddDate(0, 0, 2), 223.90, 225.50, 222.80, 224.70, 31_000_000},
		{amzn, baseDate.AddDate(0, 0, 3), 224.70, 226.30, 223.50, 225.80, 34_000_000},
		{amzn, baseDate.AddDate(0, 0, 4), 225.80, 227.90, 224.70, 227.10, 32_000_000},
		{amzn, baseDate.AddDate(0, 0, 7), 227.10, 229.40, 226.30, 228.50, 36_000_000},
		{amzn, baseDate.AddDate(0, 0, 8), 228.50, 230.10, 227.40, 229.30, 30_000_000},
		{amzn, baseDate.AddDate(0, 0, 9), 229.30, 231.20, 228.50, 230.40, 29_000_000},
		{amzn, baseDate.AddDate(0, 0, 10), 230.40, 232.50, 229.60, 231.80, 33_000_000},
		{amzn, baseDate.AddDate(0, 0, 11), 231.80, 233.70, 230.90, 232.90, 35_000_000},
		// TSLA
		{tsla, baseDate, 395.20, 401.30, 392.10, 398.50, 55_000_000},
		{tsla, baseDate.AddDate(0, 0, 1), 398.50, 405.80, 396.30, 403.20, 52_000_000},
		{tsla, baseDate.AddDate(0, 0, 2), 403.20, 408.10, 400.50, 406.70, 48_000_000},
		{tsla, baseDate.AddDate(0, 0, 3), 406.70, 410.50, 403.80, 408.90, 50_000_000},
		{tsla, baseDate.AddDate(0, 0, 4), 408.90, 413.20, 406.10, 411.50, 47_000_000},
		{tsla, baseDate.AddDate(0, 0, 7), 411.50, 415.80, 409.30, 414.20, 54_000_000},
		{tsla, baseDate.AddDate(0, 0, 8), 414.20, 418.90, 412.10, 417.30, 46_000_000},
		{tsla, baseDate.AddDate(0, 0, 9), 417.30, 421.50, 415.20, 419.80, 45_000_000},
		{tsla, baseDate.AddDate(0, 0, 10), 419.80, 423.70, 417.60, 422.10, 51_000_000},
		{tsla, baseDate.AddDate(0, 0, 11), 422.10, 426.30, 420.40, 424.80, 53_000_000},
	}

	tx = d.NewTransaction()
	for i, p := range prices {
		priceID := datalog.NewIdentity(fmt.Sprintf("price:%d", i))
		tx.Add(priceID, priceSec, p.sec)
		tx.Add(priceID, priceDate, p.date)
		tx.Add(priceID, priceOpen, p.open)
		tx.Add(priceID, priceHigh, p.high)
		tx.Add(priceID, priceLow, p.low)
		tx.Add(priceID, priceClose, p.close)
		tx.Add(priceID, priceVolume, p.volume)
	}
	if _, err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	f, err := os.Create("examples/data/securities.edn")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	if err := d.Export(f); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Generated examples/data/securities.edn")
}
