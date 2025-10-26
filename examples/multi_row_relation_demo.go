//go:build example
// +build example

package main

import (
	"fmt"
	"log"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func main() {
	fmt.Println("=== Multi-Row Relation Demo ===\n")

	// Create test data
	alice := datalog.NewIdentity("emp:alice")
	bob := datalog.NewIdentity("emp:bob")
	charlie := datalog.NewIdentity("emp:charlie")
	david := datalog.NewIdentity("emp:david")
	eve := datalog.NewIdentity("emp:eve")

	nameAttr := datalog.NewKeyword(":emp/name")
	deptAttr := datalog.NewKeyword(":emp/dept")
	salaryAttr := datalog.NewKeyword(":emp/salary")

	// Create comprehensive dataset
	datoms := []datalog.Datom{
		// Names
		{E: alice, A: nameAttr, V: "Alice", Tx: 1},
		{E: bob, A: nameAttr, V: "Bob", Tx: 1},
		{E: charlie, A: nameAttr, V: "Charlie", Tx: 1},
		{E: david, A: nameAttr, V: "David", Tx: 1},
		{E: eve, A: nameAttr, V: "Eve", Tx: 1},

		// Departments
		{E: alice, A: deptAttr, V: "Engineering", Tx: 1},
		{E: bob, A: deptAttr, V: "Engineering", Tx: 1},
		{E: charlie, A: deptAttr, V: "Sales", Tx: 1},
		{E: david, A: deptAttr, V: "Sales", Tx: 1},
		{E: eve, A: deptAttr, V: "HR", Tx: 1},

		// Salaries
		{E: alice, A: salaryAttr, V: int64(120000), Tx: 1},
		{E: bob, A: salaryAttr, V: int64(110000), Tx: 1},
		{E: charlie, A: salaryAttr, V: int64(95000), Tx: 1},
		{E: david, A: salaryAttr, V: int64(90000), Tx: 1},
		{E: eve, A: salaryAttr, V: int64(85000), Tx: 1},
	}

	matcher := executor.NewMemoryPatternMatcher(datoms)

	// Demo 1: Simple multi-row binding - find salaries for specific employees
	fmt.Println("1. Find salaries for Alice and Bob only:")
	{
		// Create a relation with multiple employee IDs
		employeeRel := executor.NewMaterializedRelation(
			[]query.Symbol{"?emp"},
			[]executor.Tuple{
				{alice},
				{bob},
			},
		)

		// Pattern: [?emp :emp/salary ?salary]
		pattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: "?emp"},
				query.Constant{Value: salaryAttr},
				query.Variable{Name: "?salary"},
			},
		}

		// Match with binding relation
		results, err := matcher.Match(pattern, executor.Relations{employeeRel})
		if err != nil {
			log.Fatal(err)
		}

		it := results.Iterator()
		defer it.Close()
		for it.Next() {
			tuple := it.Tuple()
			emp := tuple[0].(datalog.Identity)
			salary := tuple[1]
			fmt.Printf("  Employee %s has salary $%d\n", emp, salary)
		}
	}

	// Demo 2: Multi-column binding - find specific attribute values
	fmt.Println("\n2. Find specific attributes for specific employees:")
	{
		// Create a relation with employee-attribute pairs
		queryRel := executor.NewMaterializedRelation(
			[]query.Symbol{"?emp", "?attr"},
			[]executor.Tuple{
				{alice, nameAttr},
				{alice, salaryAttr},
				{charlie, deptAttr},
				{eve, nameAttr},
			},
		)

		// Pattern: [?emp ?attr ?value]
		pattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: "?emp"},
				query.Variable{Name: "?attr"},
				query.Variable{Name: "?value"},
			},
		}

		results, err := matcher.Match(pattern, executor.Relations{queryRel})
		if err != nil {
			log.Fatal(err)
		}

		it := results.Iterator()
		defer it.Close()
		for it.Next() {
			tuple := it.Tuple()
			emp := tuple[0].(datalog.Identity)
			attr := tuple[1].(datalog.Keyword)
			value := tuple[2]
			fmt.Printf("  %s %s = %v\n", emp, attr, value)
		}
	}

	// Demo 3: Department filtering using multi-row relations
	fmt.Println("\n3. Find all employees in Engineering department:")
	{
		// First, find all engineering employees
		deptPattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: "?emp"},
				query.Constant{Value: deptAttr},
				query.Constant{Value: "Engineering"},
			},
		}

		engResults, err := matcher.Match(deptPattern, executor.Relations{})
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("  Found %d employees in Engineering\n", engResults.Size())

		// Now use this relation to get their names
		namePattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: "?emp"},
				query.Constant{Value: nameAttr},
				query.Variable{Name: "?name"},
			},
		}

		nameResults, err := matcher.Match(namePattern, executor.Relations{engResults})
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println("  Their names are:")
		it := nameResults.Iterator()
		defer it.Close()
		for it.Next() {
			tuple := it.Tuple()
			name := tuple[1]
			fmt.Printf("    - %s\n", name)
		}
	}

	// Demo 4: Complex filtering - high earners by department
	fmt.Println("\n4. Find high earners (>$100k) and their departments:")
	{
		// First, find all salaries
		salaryPattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: "?emp"},
				query.Constant{Value: salaryAttr},
				query.Variable{Name: "?salary"},
			},
		}

		salaryResults, err := matcher.Match(salaryPattern, executor.Relations{})
		if err != nil {
			log.Fatal(err)
		}

		// Filter for salaries > 100k
		var highEarnerTuples []executor.Tuple
		it := salaryResults.Iterator()
		for it.Next() {
			tuple := it.Tuple()
			salary := tuple[1].(int64)
			if salary > 100000 {
				highEarnerTuples = append(highEarnerTuples, executor.Tuple{tuple[0], tuple[1]})
			}
		}
		it.Close()

		highEarnerRel := executor.NewMaterializedRelation(
			[]query.Symbol{"?emp", "?salary"},
			highEarnerTuples,
		)
		fmt.Printf("  Found %d high earners\n", highEarnerRel.Size())

		// Get their departments using the high earner relation
		deptPattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: "?emp"},
				query.Constant{Value: deptAttr},
				query.Variable{Name: "?dept"},
			},
		}

		// Project just the ?emp column from highEarnerRel
		empOnlyTuples := []executor.Tuple{}
		it = highEarnerRel.Iterator()
		for it.Next() {
			tuple := it.Tuple()
			empOnlyTuples = append(empOnlyTuples, executor.Tuple{tuple[0]})
		}
		it.Close()

		empOnlyRel := executor.NewMaterializedRelation(
			[]query.Symbol{"?emp"},
			empOnlyTuples,
		)

		deptResults, err := matcher.Match(deptPattern, executor.Relations{empOnlyRel})
		if err != nil {
			log.Fatal(err)
		}

		// Also get their names
		namePattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: "?emp"},
				query.Constant{Value: nameAttr},
				query.Variable{Name: "?name"},
			},
		}

		nameResults, err := matcher.Match(namePattern, executor.Relations{empOnlyRel})
		if err != nil {
			log.Fatal(err)
		}

		// Create a map to combine results
		empInfo := make(map[string]struct {
			name   string
			dept   string
			salary int64
		})

		// Add salary info
		it = highEarnerRel.Iterator()
		for it.Next() {
			tuple := it.Tuple()
			emp := tuple[0].(datalog.Identity)
			salary := tuple[1].(int64)
			info := empInfo[emp.L85()]
			info.salary = salary
			empInfo[emp.L85()] = info
		}
		it.Close()

		// Add department info
		it = deptResults.Iterator()
		for it.Next() {
			tuple := it.Tuple()
			emp := tuple[0].(datalog.Identity)
			dept := tuple[1].(string)
			info := empInfo[emp.L85()]
			info.dept = dept
			empInfo[emp.L85()] = info
		}
		it.Close()

		// Add name info
		it = nameResults.Iterator()
		for it.Next() {
			tuple := it.Tuple()
			emp := tuple[0].(datalog.Identity)
			name := tuple[1].(string)
			info := empInfo[emp.L85()]
			info.name = name
			empInfo[emp.L85()] = info
		}
		it.Close()

		fmt.Println("  High earners by department:")
		for _, info := range empInfo {
			fmt.Printf("    - %s (%s): $%d\n", info.name, info.dept, info.salary)
		}
	}

	fmt.Println("\n=== Demo Complete ===")
}
