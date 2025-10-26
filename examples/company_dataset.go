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
	dir, err := os.MkdirTemp("", "company-dataset-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	
	fmt.Println("Company Dataset Example")
	fmt.Println("=======================\n")
	
	// Open the store with L85 encoding for human-readable keys
	encoder := storage.NewKeyEncoder(storage.L85Strategy)
	store, err := storage.NewBadgerStore(dir, encoder)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()
	
	// Create query builder with same encoder
	qb := storage.NewQueryBuilder(store, encoder)
	
	// Create company entities
	techCorp := datalog.NewIdentity("company:techcorp")
	startupInc := datalog.NewIdentity("company:startup-inc")
	
	// Create employee entities
	alice := datalog.NewIdentity("employee:alice-smith")
	bob := datalog.NewIdentity("employee:bob-jones")
	charlie := datalog.NewIdentity("employee:charlie-brown")
	diana := datalog.NewIdentity("employee:diana-prince")
	eve := datalog.NewIdentity("employee:eve-davis")
	frank := datalog.NewIdentity("employee:frank-miller")
	
	// Create department entities
	engineering := datalog.NewIdentity("dept:engineering")
	sales := datalog.NewIdentity("dept:sales")
	hr := datalog.NewIdentity("dept:hr")
	
	// Transaction times (could be hire dates, data entry times, etc)
	tx1 := uint64(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Unix())
	tx2 := uint64(time.Date(2021, 6, 1, 0, 0, 0, 0, time.UTC).Unix())
	tx3 := uint64(time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC).Unix())
	tx4 := uint64(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).Unix())
	
	// Build the dataset
	fmt.Println("1. Creating company and employee data...")
	
	datoms := []datalog.Datom{
		// Company data
		{E: techCorp, A: datalog.NewKeyword(":company/name"), V: "TechCorp International", Tx: tx1},
		{E: techCorp, A: datalog.NewKeyword(":company/founded"), V: int64(2015), Tx: tx1},
		{E: techCorp, A: datalog.NewKeyword(":company/industry"), V: "Technology", Tx: tx1},

		{E: startupInc, A: datalog.NewKeyword(":company/name"), V: "Startup Inc", Tx: tx2},
		{E: startupInc, A: datalog.NewKeyword(":company/founded"), V: int64(2021), Tx: tx2},
		{E: startupInc, A: datalog.NewKeyword(":company/industry"), V: "Technology", Tx: tx2},

		// Department data
		{E: engineering, A: datalog.NewKeyword(":dept/name"), V: "Engineering", Tx: tx1},
		{E: engineering, A: datalog.NewKeyword(":dept/company"), V: techCorp, Tx: tx1},

		{E: sales, A: datalog.NewKeyword(":dept/name"), V: "Sales", Tx: tx1},
		{E: sales, A: datalog.NewKeyword(":dept/company"), V: techCorp, Tx: tx1},

		{E: hr, A: datalog.NewKeyword(":dept/name"), V: "Human Resources", Tx: tx1},
		{E: hr, A: datalog.NewKeyword(":dept/company"), V: techCorp, Tx: tx1},
		
		// Employee personal data
		{E: alice, A: datalog.NewKeyword(":employee/name"), V: "Alice Smith", Tx: tx1},
		{E: alice, A: datalog.NewKeyword(":employee/email"), V: "alice@techcorp.com", Tx: tx1},
		{E: alice, A: datalog.NewKeyword(":employee/hair-color"), V: "blonde", Tx: tx1},
		{E: alice, A: datalog.NewKeyword(":employee/age"), V: int64(32), Tx: tx1},
		{E: alice, A: datalog.NewKeyword(":employee/salary"), V: 150000.0, Tx: tx1},
		
		{E: bob, A: datalog.NewKeyword(":employee/name"), V: "Bob Jones", Tx: tx1},
		{E: bob, A: datalog.NewKeyword(":employee/email"), V: "bob@techcorp.com", Tx: tx1},
		{E: bob, A: datalog.NewKeyword(":employee/hair-color"), V: "brown", Tx: tx1},
		{E: bob, A: datalog.NewKeyword(":employee/age"), V: int64(28), Tx: tx1},
		{E: bob, A: datalog.NewKeyword(":employee/salary"), V: 95000.0, Tx: tx1},
		
		{E: charlie, A: datalog.NewKeyword(":employee/name"), V: "Charlie Brown", Tx: tx1},
		{E: charlie, A: datalog.NewKeyword(":employee/email"), V: "charlie@techcorp.com", Tx: tx1},
		{E: charlie, A: datalog.NewKeyword(":employee/hair-color"), V: "black", Tx: tx1},
		{E: charlie, A: datalog.NewKeyword(":employee/age"), V: int64(35), Tx: tx1},
		{E: charlie, A: datalog.NewKeyword(":employee/salary"), V: 105000.0, Tx: tx1},
		
		{E: diana, A: datalog.NewKeyword(":employee/name"), V: "Diana Prince", Tx: tx2},
		{E: diana, A: datalog.NewKeyword(":employee/email"), V: "diana@techcorp.com", Tx: tx2},
		{E: diana, A: datalog.NewKeyword(":employee/hair-color"), V: "black", Tx: tx2},
		{E: diana, A: datalog.NewKeyword(":employee/age"), V: int64(30), Tx: tx2},
		{E: diana, A: datalog.NewKeyword(":employee/salary"), V: 120000.0, Tx: tx2},
		
		{E: eve, A: datalog.NewKeyword(":employee/name"), V: "Eve Davis", Tx: tx3},
		{E: eve, A: datalog.NewKeyword(":employee/email"), V: "eve@startup-inc.com", Tx: tx3},
		{E: eve, A: datalog.NewKeyword(":employee/hair-color"), V: "red", Tx: tx3},
		{E: eve, A: datalog.NewKeyword(":employee/age"), V: int64(26), Tx: tx3},
		{E: eve, A: datalog.NewKeyword(":employee/salary"), V: 85000.0, Tx: tx3},
		
		{E: frank, A: datalog.NewKeyword(":employee/name"), V: "Frank Miller", Tx: tx3},
		{E: frank, A: datalog.NewKeyword(":employee/email"), V: "frank@startup-inc.com", Tx: tx3},
		{E: frank, A: datalog.NewKeyword(":employee/hair-color"), V: "gray", Tx: tx3},
		{E: frank, A: datalog.NewKeyword(":employee/age"), V: int64(45), Tx: tx3},
		{E: frank, A: datalog.NewKeyword(":employee/salary"), V: 180000.0, Tx: tx3},
		
		// Employment relationships (using references)
		{E: alice, A: datalog.NewKeyword(":employee/company"), V: techCorp, Tx: tx1},
		{E: alice, A: datalog.NewKeyword(":employee/department"), V: engineering, Tx: tx1},
		{E: alice, A: datalog.NewKeyword(":employee/title"), V: "Engineering Manager", Tx: tx1},
		
		{E: bob, A: datalog.NewKeyword(":employee/company"), V: techCorp, Tx: tx1},
		{E: bob, A: datalog.NewKeyword(":employee/department"), V: engineering, Tx: tx1},
		{E: bob, A: datalog.NewKeyword(":employee/title"), V: "Software Engineer", Tx: tx1},
		{E: bob, A: datalog.NewKeyword(":employee/reports-to"), V: alice, Tx: tx1},
		
		{E: charlie, A: datalog.NewKeyword(":employee/company"), V: techCorp, Tx: tx1},
		{E: charlie, A: datalog.NewKeyword(":employee/department"), V: engineering, Tx: tx1},
		{E: charlie, A: datalog.NewKeyword(":employee/title"), V: "Senior Software Engineer", Tx: tx1},
		{E: charlie, A: datalog.NewKeyword(":employee/reports-to"), V: alice, Tx: tx1},
		
		{E: diana, A: datalog.NewKeyword(":employee/company"), V: techCorp, Tx: tx2},
		{E: diana, A: datalog.NewKeyword(":employee/department"), V: sales, Tx: tx2},
		{E: diana, A: datalog.NewKeyword(":employee/title"), V: "Sales Director", Tx: tx2},
		
		{E: eve, A: datalog.NewKeyword(":employee/company"), V: startupInc, Tx: tx3},
		{E: eve, A: datalog.NewKeyword(":employee/title"), V: "Full Stack Developer", Tx: tx3},
		{E: eve, A: datalog.NewKeyword(":employee/reports-to"), V: frank, Tx: tx3},
		
		{E: frank, A: datalog.NewKeyword(":employee/company"), V: startupInc, Tx: tx3},
		{E: frank, A: datalog.NewKeyword(":employee/title"), V: "CTO", Tx: tx3},

		// Some employee relationships
		{E: alice, A: datalog.NewKeyword(":employee/mentors"), V: diana, Tx: tx2},
		{E: bob, A: datalog.NewKeyword(":employee/collaborates-with"), V: charlie, Tx: tx1},
		{E: charlie, A: datalog.NewKeyword(":employee/collaborates-with"), V: bob, Tx: tx1},

		// Promotions (newer facts)
		{E: bob, A: datalog.NewKeyword(":employee/title"), V: "Senior Software Engineer", Tx: tx4},
		{E: bob, A: datalog.NewKeyword(":employee/salary"), V: 115000.0, Tx: tx4},
	}
	
	err = store.Assert(datoms)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("✓ Created %d facts\n\n", len(datoms))
	
	// Query examples
	fmt.Println("2. Query Examples")
	fmt.Println("-----------------")
	
	// Find all employees with brown hair
	fmt.Println("\n2.1 Employees with brown hair:")
	hairAttr := datalog.NewKeyword(":employee/hair-color")
	brownHair, err := qb.GetAttributeValue(hairAttr, "brown")
	if err != nil {
		log.Fatal(err)
	}
	for _, d := range brownHair {
		name, _ := qb.GetEntityAttribute(d.E, datalog.NewKeyword(":employee/name"))
		if len(name) > 0 {
			fmt.Printf("  - %v\n", name[0].V)
		}
	}
	
	// Find who reports to Alice
	fmt.Println("\n2.2 Who reports to Alice:")
	reportsTo := datalog.NewKeyword(":employee/reports-to")
	reports, err := qb.GetAttributeValue(reportsTo, alice)
	if err != nil {
		log.Fatal(err)
	}
	for _, d := range reports {
		name, _ := qb.GetEntityAttribute(d.E, datalog.NewKeyword(":employee/name"))
		title, _ := qb.GetEntityAttribute(d.E, datalog.NewKeyword(":employee/title"))
		if len(name) > 0 && len(title) > 0 {
			// Get the most recent title
			mostRecentTitle := title[len(title)-1]
			fmt.Printf("  - %v (%v)\n", name[0].V, mostRecentTitle.V)
		}
	}
	
	// Find all TechCorp employees
	fmt.Println("\n2.3 TechCorp employees:")
	companyAttr := datalog.NewKeyword(":employee/company")
	techEmployees, err := qb.GetAttributeValue(companyAttr, techCorp)
	if err != nil {
		log.Fatal(err)
	}
	for _, d := range techEmployees {
		name, _ := qb.GetEntityAttribute(d.E, datalog.NewKeyword(":employee/name"))
		dept, _ := qb.GetEntityAttribute(d.E, datalog.NewKeyword(":employee/department"))
		if len(name) > 0 {
			deptName := "No Department"
			if len(dept) > 0 {
				deptEntity := dept[0].V.(datalog.Identity)
				deptNameDatom, _ := qb.GetEntityAttribute(deptEntity, datalog.NewKeyword(":dept/name"))
				if len(deptNameDatom) > 0 {
					deptName = fmt.Sprintf("%v", deptNameDatom[0].V)
				}
			}
			fmt.Printf("  - %v (%s)\n", name[0].V, deptName)
		}
	}
	
	// Find high earners (would need a query engine for this, but we can iterate)
	fmt.Println("\n2.4 High earners (>$100k):")
	salaryAttr := datalog.NewKeyword(":employee/salary")
	allSalaries, err := qb.GetAttribute(salaryAttr)
	if err != nil {
		log.Fatal(err)
	}
	for _, d := range allSalaries {
		if salary, ok := d.V.(float64); ok && salary > 100000 {
			name, _ := qb.GetEntityAttribute(d.E, datalog.NewKeyword(":employee/name"))
			if len(name) > 0 {
				fmt.Printf("  - %v: $%.0f\n", name[0].V, salary)
			}
		}
	}
	
	// Temporal query - who joined after 2021?
	fmt.Println("\n2.5 Employees hired after 2021:")
	startTime := uint64(time.Date(2021, 12, 31, 23, 59, 59, 0, time.UTC).Unix())
	endTime := uint64(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).Unix())
	newHires, err := qb.GetTimeRange(startTime, endTime)
	if err != nil {
		log.Fatal(err)
	}

	// Track unique employees
	seen := make(map[datalog.Identity]bool)
	for _, d := range newHires {
		if d.A == datalog.NewKeyword(":employee/name") {
			if !seen[d.E] {
				seen[d.E] = true
				fmt.Printf("  - %v (hired %s)\n", d.V, d.Tx)
			}
		}
	}
	
	// Graph traversal - find Alice's reports and their reports
	fmt.Println("\n2.6 Alice's reporting hierarchy:")
	printReportingHierarchy(qb, alice, "", make(map[datalog.Identity]bool))
	
	// Reference queries
	fmt.Println("\n2.7 All references to Engineering department:")
	engRefs, err := qb.GetReferences(engineering)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  Found %d references\n", len(engRefs))
	
	fmt.Println("\n3. Example Datalog Queries (when parser is implemented):")
	fmt.Println("--------------------------------------------------------")
	
	fmt.Println("\nFind all blonde employees in Engineering:")
	fmt.Println(`[:find ?name
 :where [?e :employee/name ?name]
        [?e :employee/hair-color "blonde"]
        [?e :employee/department ?d]
        [?d :dept/name "Engineering"]]`)
	
	fmt.Println("\nFind employee-manager pairs:")
	fmt.Println(`[:find ?emp-name ?mgr-name
 :where [?e :employee/name ?emp-name]
        [?e :employee/reports-to ?m]
        [?m :employee/name ?mgr-name]]`)
	
	fmt.Println("\nFind average salary by department:")
	fmt.Println(`[:find ?dept (avg ?salary)
 :where [?e :employee/department ?d]
        [?d :dept/name ?dept]
        [?e :employee/salary ?salary]]`)
	
	fmt.Println("\nDataset created successfully!")
}

// Helper function to print reporting hierarchy
func printReportingHierarchy(qb *storage.QueryBuilder, manager datalog.Identity, indent string, visited map[datalog.Identity]bool) {
	if visited[manager] {
		return // Avoid cycles
	}
	visited[manager] = true

	// Get manager name
	name, _ := qb.GetEntityAttribute(manager, datalog.NewKeyword(":employee/name"))
	if len(name) > 0 {
		fmt.Printf("%s%v\n", indent, name[0].V)
	}

	// Find direct reports
	reportsTo := datalog.NewKeyword(":employee/reports-to")
	reports, _ := qb.GetAttributeValue(reportsTo, manager)

	for _, d := range reports {
		printReportingHierarchy(qb, d.E, indent+"  ", visited)
	}
}
