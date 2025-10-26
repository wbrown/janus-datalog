//go:build example
// +build example

package main

import (
	"fmt"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
)

func main() {
	// Example: Proper entity modeling for financial data
	
	// Get Eastern timezone
	est, _ := time.LoadLocation("America/New_York")
	
	// Stock entity - ID is hash of symbol
	crwv := datalog.NewIdentity("CRWV")
	ipoTime := time.Date(2024, 3, 28, 9, 30, 0, 0, est)
	
	fmt.Println("Stock Entity Datoms:")
	fmt.Println("===================")
	
	// Immutable facts about the stock
	stockDatoms := []datalog.Datom{
		{
			E: crwv,
			A: datalog.NewKeyword(":entity/type"),
			V: "stock",
			Tx: uint64(ipoTime.Unix()),
		},
		{
			E: crwv,
			A: datalog.NewKeyword(":entity/symbol"),
			V: "CRWV",
			Tx: uint64(ipoTime.Unix()),
		},
		{
			E: crwv,
			A: datalog.NewKeyword(":entity/name"),
			V: "CoreWeave Inc.",
			Tx: uint64(ipoTime.Unix()),
		},
		{
			E: crwv,
			A: datalog.NewKeyword(":stock/exchange"),
			V: "NASDAQ",
			Tx: uint64(ipoTime.Unix()),
		},
		{
			E: crwv,
			A: datalog.NewKeyword(":stock/sector"),
			V: "Technology",
			Tx: uint64(ipoTime.Unix()),
		},
	}
	
	for _, d := range stockDatoms {
		fmt.Printf("  %s\n", d)
	}
	
	// Price updates at different times
	fmt.Println("\nPrice Updates (Time-Series Facts):")
	fmt.Println("==================================")
	
	// 10:30 AM bar
	time1030 := time.Date(2024, 6, 19, 10, 30, 0, 0, est)
	priceDatoms1030 := []datalog.Datom{
		{E: crwv, A: datalog.NewKeyword(":price/open"), V: float64(150.25), Tx: uint64(time1030.Unix())},
		{E: crwv, A: datalog.NewKeyword(":price/high"), V: float64(151.50), Tx: uint64(time1030.Unix())},
		{E: crwv, A: datalog.NewKeyword(":price/low"), V: float64(149.75), Tx: uint64(time1030.Unix())},
		{E: crwv, A: datalog.NewKeyword(":price/close"), V: float64(151.00), Tx: uint64(time1030.Unix())},
		{E: crwv, A: datalog.NewKeyword(":price/volume"), V: int64(125000), Tx: uint64(time1030.Unix())},
	}
	
	fmt.Println("  10:30 AM:")
	for _, d := range priceDatoms1030 {
		fmt.Printf("    %s\n", d)
	}
	
	// 10:35 AM bar
	time1035 := time.Date(2024, 6, 19, 10, 35, 0, 0, est)
	priceDatoms1035 := []datalog.Datom{
		{E: crwv, A: datalog.NewKeyword(":price/open"), V: float64(151.00), Tx: uint64(time1035.Unix())},
		{E: crwv, A: datalog.NewKeyword(":price/high"), V: float64(151.75), Tx: uint64(time1035.Unix())},
		{E: crwv, A: datalog.NewKeyword(":price/low"), V: float64(150.50), Tx: uint64(time1035.Unix())},
		{E: crwv, A: datalog.NewKeyword(":price/close"), V: float64(151.25), Tx: uint64(time1035.Unix())},
		{E: crwv, A: datalog.NewKeyword(":price/volume"), V: int64(98000), Tx: uint64(time1035.Unix())},
	}
	
	fmt.Println("\n10:35 AM:")
	for _, d := range priceDatoms1035 {
		fmt.Printf("    %s\n", d)
	}
	
	// Option entity - ID is hash of defining characteristics
	fmt.Println("\nOption Entity Datoms:")
	fmt.Println("====================")
	
	option := datalog.NewIdentity("CRWV:CALL:150.0:2024-07-19")
	optionCreated := time.Date(2024, 5, 1, 9, 30, 0, 0, est)
	expiry := time.Date(2024, 7, 19, 16, 0, 0, 0, est)
	
	// Immutable option characteristics
	optionDatoms := []datalog.Datom{
		{E: option, A: datalog.NewKeyword(":entity/type"), V: "option", Tx: uint64(optionCreated.Unix())},
		{E: option, A: datalog.NewKeyword(":option/underlying"), V: crwv, Tx: uint64(optionCreated.Unix())},
		{E: option, A: datalog.NewKeyword(":option/type"), V: "call", Tx: uint64(optionCreated.Unix())},
		{E: option, A: datalog.NewKeyword(":option/strike"), V: float64(150.0), Tx: uint64(optionCreated.Unix())},
		{E: option, A: datalog.NewKeyword(":option/expiration"), V: expiry, Tx: uint64(optionCreated.Unix())},
	}
	
	for _, d := range optionDatoms {
		fmt.Printf("  %s\n", d)
	}
	
	// Option market data updates
	fmt.Println("\nMarket data at 3:00 PM:")
	optionTime := time.Date(2024, 6, 19, 15, 0, 0, 0, est)
	optionMarketDatoms := []datalog.Datom{
		{E: option, A: datalog.NewKeyword(":option/bid"), V: float64(5.25), Tx: uint64(optionTime.Unix())},
		{E: option, A: datalog.NewKeyword(":option/ask"), V: float64(5.50), Tx: uint64(optionTime.Unix())},
		{E: option, A: datalog.NewKeyword(":option/volume"), V: int64(1250), Tx: uint64(optionTime.Unix())},
		{E: option, A: datalog.NewKeyword(":option/oi"), V: int64(5000), Tx: uint64(optionTime.Unix())},
		{E: option, A: datalog.NewKeyword(":option/iv"), V: float64(0.85), Tx: uint64(optionTime.Unix())},
		{E: option, A: datalog.NewKeyword(":option/delta"), V: float64(0.55), Tx: uint64(optionTime.Unix())},
	}
	
	for _, d := range optionMarketDatoms {
		fmt.Printf("    %s\n", d)
	}
	
	// News article entity
	fmt.Println("\nNews Article Entity Datoms:")
	fmt.Println("===========================")
	
	news := datalog.NewIdentity("bloomberg:crwv-ai-expansion-2024-06-19-12345")
	publishTime := time.Date(2024, 6, 19, 14, 30, 0, 0, est)
	
	newsDatoms := []datalog.Datom{
		{E: news, A: datalog.NewKeyword(":entity/type"), V: "news", Tx: uint64(publishTime.Unix())},
		{E: news, A: datalog.NewKeyword(":news/source"), V: "Bloomberg", Tx: uint64(publishTime.Unix())},
		{E: news, A: datalog.NewKeyword(":news/title"), V: "CoreWeave Announces Major AI Infrastructure Expansion", Tx: uint64(publishTime.Unix())},
		{E: news, A: datalog.NewKeyword(":news/url"), V: "https://bloomberg.com/crwv-ai-expansion", Tx: uint64(publishTime.Unix())},
		{E: news, A: datalog.NewKeyword(":news/about"), V: crwv, Tx: uint64(publishTime.Unix())},
		{E: news, A: datalog.NewKeyword(":news/sentiment"), V: float64(0.85), Tx: uint64(publishTime.Unix())},
		{E: news, A: datalog.NewKeyword(":news/topics"), V: "AI", Tx: uint64(publishTime.Unix())},
		{E: news, A: datalog.NewKeyword(":news/topics"), V: "Infrastructure", Tx: uint64(publishTime.Unix())},
		{E: news, A: datalog.NewKeyword(":news/topics"), V: "Expansion", Tx: uint64(publishTime.Unix())},
	}
	
	for _, d := range newsDatoms {
		fmt.Printf("  %s\n", d)
	}
	
	// Example queries that would work with this model
	fmt.Println("\nExample Queries:")
	fmt.Println("================")
	
	fmt.Println(`
1. Get current price of CRWV:
   [:find ?price ?time
    :where [CRWV :price/close ?price ?time]
    :order-by ?time :desc
    :limit 1]

2. Get all prices between 10:30 and 10:35:
   [:find ?attr ?value ?time
    :where [CRWV ?attr ?value ?time]
           [(in ?attr [:price/open :price/high :price/low :price/close])]
           [(>= ?time 2024-06-19T10:30:00)]
           [(<= ?time 2024-06-19T10:35:00)]]

3. Find options on CRWV with high IV:
   [:find ?option ?strike ?iv
    :where [?option :option/underlying CRWV _]
           [?option :option/strike ?strike _]
           [?option :option/iv ?iv ?t]
           [(> ?iv 0.8)]]

4. Get news sentiment about CRWV today:
   [:find ?title ?sentiment ?time
    :where [?news :news/about CRWV _]
           [?news :news/title ?title _]
           [?news :news/sentiment ?sentiment ?time]
           [(>= ?time 2024-06-19T00:00:00)]]

5. Find correlated entities (mentioned in same news):
   [:find ?other ?title
    :where [?news :news/about CRWV _]
           [?news :news/about ?other _]
           [?news :news/title ?title _]
           [(!= ?other CRWV)]]
`)
	
	// Show how entity IDs work
	fmt.Println("Entity ID Examples:")
	fmt.Println("===================")
	fmt.Printf("CRWV entity:   %x (SHA1 of 'CRWV')\n", crwv)
	fmt.Printf("Option entity: %x (SHA1 of 'CRWV:CALL:150.0:2024-07-19')\n", option)
	fmt.Printf("News entity:   %x (SHA1 of 'bloomberg:crwv-ai-expansion-2024-06-19-12345')\n", news)
	
	// Same inputs always produce same entity ID
	crwv2 := datalog.NewIdentity("CRWV")
	fmt.Printf("\nVerify deterministic: CRWV again = %x (same: %v)\n", crwv2, crwv == crwv2)
}
