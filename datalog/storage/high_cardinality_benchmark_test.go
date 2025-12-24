package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
)

// BenchmarkHighCardinalityKeywords tests performance with a realistic number of distinct keywords.
// Real-world databases typically have 100-500+ distinct attributes across entity types.
// This benchmark simulates an enterprise application with:
// - Users (20 attributes)
// - Organizations (15 attributes)
// - Products (25 attributes)
// - Orders (20 attributes)
// - OrderItems (10 attributes)
// - Reviews (12 attributes)
// - Categories (8 attributes)
// - Inventory (15 attributes)
// - Shipments (18 attributes)
// - Payments (16 attributes)
// - AuditLogs (12 attributes)
// - Settings (20 attributes)
// - Notifications (10 attributes)
// - Sessions (8 attributes)
// - Analytics (15 attributes)
// Total: ~224 distinct keywords
func BenchmarkHighCardinalityKeywords(b *testing.B) {
	tempDir := b.TempDir()
	db, err := NewDatabase(tempDir)
	if err != nil {
		b.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Pre-create all keywords to measure interning cost during setup
	keywords := generateEnterpriseKeywords()
	b.Logf("Total distinct keywords: %d", countKeywords(keywords))

	// Generate test data
	loc, _ := time.LoadLocation("America/New_York")
	baseTime := time.Date(2025, 1, 1, 9, 0, 0, 0, loc)

	// Create 100 users with all 20 attributes
	userKeywords := keywords["user"]
	for i := 0; i < 100; i++ {
		tx := db.NewTransaction()
		userID := datalog.NewIdentity(fmt.Sprintf("user:%d", i))

		for j, kw := range userKeywords {
			var value interface{}
			switch j % 5 {
			case 0:
				value = fmt.Sprintf("value-%d-%d", i, j)
			case 1:
				value = int64(i*100 + j)
			case 2:
				value = float64(i) + float64(j)/100.0
			case 3:
				value = i%2 == 0
			case 4:
				value = baseTime.Add(time.Duration(i*j) * time.Hour)
			}
			tx.Add(userID, kw, value)
		}
		if _, err := tx.Commit(); err != nil {
			b.Fatalf("Failed to commit user %d: %v", i, err)
		}
	}

	// Create 50 organizations with all 15 attributes
	orgKeywords := keywords["org"]
	for i := 0; i < 50; i++ {
		tx := db.NewTransaction()
		orgID := datalog.NewIdentity(fmt.Sprintf("org:%d", i))

		for j, kw := range orgKeywords {
			var value interface{}
			switch j % 4 {
			case 0:
				value = fmt.Sprintf("org-value-%d-%d", i, j)
			case 1:
				value = int64(i*1000 + j)
			case 2:
				value = float64(i*10) + float64(j)/10.0
			case 3:
				value = baseTime.Add(time.Duration(i*j*24) * time.Hour)
			}
			tx.Add(orgID, kw, value)
		}
		if _, err := tx.Commit(); err != nil {
			b.Fatalf("Failed to commit org %d: %v", i, err)
		}
	}

	// Create 500 products with all 25 attributes
	productKeywords := keywords["product"]
	for i := 0; i < 500; i++ {
		tx := db.NewTransaction()
		productID := datalog.NewIdentity(fmt.Sprintf("product:%d", i))

		for j, kw := range productKeywords {
			var value interface{}
			switch j % 5 {
			case 0:
				value = fmt.Sprintf("product-%d-attr-%d", i, j)
			case 1:
				value = int64(i + j*10)
			case 2:
				value = float64(i)*0.99 + float64(j)
			case 3:
				value = i%3 == 0
			case 4:
				value = baseTime.Add(time.Duration(i) * time.Minute)
			}
			tx.Add(productID, kw, value)
		}
		if _, err := tx.Commit(); err != nil {
			b.Fatalf("Failed to commit product %d: %v", i, err)
		}
	}

	// Create 1000 orders with 20 attributes each, linking to users and products
	orderKeywords := keywords["order"]
	orderUserKw := datalog.NewKeyword(":order/user")
	orderProductKw := datalog.NewKeyword(":order/product")
	orderStatusKw := datalog.NewKeyword(":order/status")
	orderStatuses := []string{"pending", "processing", "completed", "cancelled", "refunded"}

	for i := 0; i < 1000; i++ {
		tx := db.NewTransaction()
		orderID := datalog.NewIdentity(fmt.Sprintf("order:%d", i))
		userRef := datalog.NewIdentity(fmt.Sprintf("user:%d", i%100))
		productRef := datalog.NewIdentity(fmt.Sprintf("product:%d", i%500))

		// Add user reference
		tx.Add(orderID, orderUserKw, userRef)
		// Add product reference
		tx.Add(orderID, orderProductKw, productRef)
		// Add status with realistic string values (~200 completed orders)
		tx.Add(orderID, orderStatusKw, orderStatuses[i%5])

		for j, kw := range orderKeywords {
			// Skip the ones we already added (pointer comparison - interned)
			if kw == orderUserKw || kw == orderProductKw || kw == orderStatusKw {
				continue
			}
			var value interface{}
			switch j % 5 {
			case 0:
				value = fmt.Sprintf("order-%d-data-%d", i, j)
			case 1:
				value = int64(i*10 + j)
			case 2:
				value = float64(i)*1.5 + float64(j)*0.1
			case 3:
				value = i%2 == 0
			case 4:
				value = baseTime.Add(time.Duration(i*60) * time.Minute)
			}
			tx.Add(orderID, kw, value)
		}
		if _, err := tx.Commit(); err != nil {
			b.Fatalf("Failed to commit order %d: %v", i, err)
		}
	}

	// Benchmark queries that exercise keyword comparison

	// Query 1: Simple lookup with many keyword comparisons during scan
	b.Run("SimpleQuery_HighKeywordVariety", func(b *testing.B) {
		queryStr := `[:find ?u ?name ?email
		              :where
		              [?u :user/name ?name]
		              [?u :user/email ?email]
		              [?u :user/active true]]`

		q, err := parser.ParseQuery(queryStr)
		if err != nil {
			b.Fatalf("Failed to parse query: %v", err)
		}

		matcher := NewBadgerMatcher(db.store)
		exec := executor.NewExecutorWithOptions(matcher, planner.PlannerOptions{
			EnableFineGrainedPhases: true,
		})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := exec.Execute(q)
			if err != nil {
				b.Fatalf("Query failed: %v", err)
			}
			_ = result.Size()
		}
	})

	// Query 2: Join across entities with different keyword namespaces
	b.Run("JoinQuery_CrossNamespace", func(b *testing.B) {
		queryStr := `[:find ?userName ?productName
		              :where
		              [?o :order/user ?u]
		              [?o :order/product ?p]
		              [?o :order/status "completed"]
		              [?u :user/name ?userName]
		              [?p :product/name ?productName]]`

		q, err := parser.ParseQuery(queryStr)
		if err != nil {
			b.Fatalf("Failed to parse query: %v", err)
		}

		matcher := NewBadgerMatcher(db.store)
		exec := executor.NewExecutorWithOptions(matcher, planner.PlannerOptions{
			EnableFineGrainedPhases: true,
		})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := exec.Execute(q)
			if err != nil {
				b.Fatalf("Query failed: %v", err)
			}
			_ = result.Size()
		}
	})

	// Query 3: Aggregation across many attributes
	b.Run("Aggregation_ManyAttributes", func(b *testing.B) {
		queryStr := `[:find ?status (count ?o) (sum ?total)
		              :where
		              [?o :order/status ?status]
		              [?o :order/total ?total]]`

		q, err := parser.ParseQuery(queryStr)
		if err != nil {
			b.Fatalf("Failed to parse query: %v", err)
		}

		matcher := NewBadgerMatcher(db.store)
		exec := executor.NewExecutorWithOptions(matcher, planner.PlannerOptions{
			EnableFineGrainedPhases: true,
		})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := exec.Execute(q)
			if err != nil {
				b.Fatalf("Query failed: %v", err)
			}
			_ = result.Size()
		}
	})

	// Query 4: Pull-style query retrieving many attributes
	b.Run("WildcardPull_ManyAttributes", func(b *testing.B) {
		// Get a user entity
		userID := datalog.NewIdentity("user:42")

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := db.Pull(userID, `[*]`)
			if err != nil {
				b.Fatalf("Pull failed: %v", err)
			}
			if len(result) == 0 {
				b.Fatal("Expected attributes")
			}
		}
	})
}

// generateEnterpriseKeywords creates a realistic set of keywords for an enterprise app
func generateEnterpriseKeywords() map[string][]datalog.Keyword {
	keywords := make(map[string][]datalog.Keyword)

	// User entity (20 attributes)
	keywords["user"] = createKeywords("user", []string{
		"id", "name", "email", "password-hash", "salt",
		"first-name", "last-name", "phone", "avatar-url", "bio",
		"created-at", "updated-at", "last-login", "login-count", "active",
		"role", "department", "manager", "timezone", "locale",
	})

	// Organization entity (15 attributes)
	keywords["org"] = createKeywords("org", []string{
		"id", "name", "slug", "domain", "logo-url",
		"address", "city", "state", "country", "postal-code",
		"phone", "created-at", "plan", "billing-email", "active",
	})

	// Product entity (25 attributes)
	keywords["product"] = createKeywords("product", []string{
		"id", "name", "sku", "description", "short-description",
		"price", "cost", "weight", "width", "height",
		"depth", "category", "brand", "manufacturer", "country-of-origin",
		"active", "featured", "taxable", "requires-shipping", "inventory-tracked",
		"created-at", "updated-at", "published-at", "seo-title", "seo-description",
	})

	// Order entity (20 attributes)
	keywords["order"] = createKeywords("order", []string{
		"id", "user", "product", "status", "total",
		"subtotal", "tax", "shipping", "discount", "currency",
		"payment-method", "payment-status", "shipping-address", "billing-address", "notes",
		"created-at", "updated-at", "completed-at", "cancelled-at", "refunded-at",
	})

	// OrderItem entity (10 attributes)
	keywords["order-item"] = createKeywords("order-item", []string{
		"id", "order", "product", "quantity", "unit-price",
		"total", "discount", "tax", "sku", "name",
	})

	// Review entity (12 attributes)
	keywords["review"] = createKeywords("review", []string{
		"id", "user", "product", "rating", "title",
		"body", "helpful-count", "verified-purchase", "created-at", "updated-at",
		"status", "moderated-at",
	})

	// Category entity (8 attributes)
	keywords["category"] = createKeywords("category", []string{
		"id", "name", "slug", "parent", "description",
		"image-url", "sort-order", "active",
	})

	// Inventory entity (15 attributes)
	keywords["inventory"] = createKeywords("inventory", []string{
		"id", "product", "warehouse", "quantity", "reserved",
		"available", "reorder-point", "reorder-quantity", "last-counted-at", "last-received-at",
		"cost", "location", "bin", "zone", "active",
	})

	// Shipment entity (18 attributes)
	keywords["shipment"] = createKeywords("shipment", []string{
		"id", "order", "carrier", "service", "tracking-number",
		"status", "weight", "dimensions", "from-address", "to-address",
		"shipped-at", "delivered-at", "estimated-delivery", "actual-cost", "billed-cost",
		"label-url", "created-at", "updated-at",
	})

	// Payment entity (16 attributes)
	keywords["payment"] = createKeywords("payment", []string{
		"id", "order", "amount", "currency", "method",
		"status", "processor", "processor-id", "card-last-four", "card-brand",
		"billing-address", "created-at", "captured-at", "refunded-at", "failed-at",
		"error-message",
	})

	// AuditLog entity (12 attributes)
	keywords["audit-log"] = createKeywords("audit-log", []string{
		"id", "user", "action", "entity-type", "entity-id",
		"old-value", "new-value", "ip-address", "user-agent", "created-at",
		"session-id", "request-id",
	})

	// Settings entity (20 attributes)
	keywords["settings"] = createKeywords("settings", []string{
		"id", "user", "theme", "language", "timezone",
		"notifications-email", "notifications-push", "notifications-sms", "digest-frequency", "privacy-level",
		"two-factor-enabled", "api-key", "webhook-url", "default-currency", "date-format",
		"time-format", "first-day-of-week", "items-per-page", "compact-view", "beta-features",
	})

	// Notification entity (10 attributes)
	keywords["notification"] = createKeywords("notification", []string{
		"id", "user", "type", "title", "body",
		"read", "created-at", "read-at", "action-url", "metadata",
	})

	// Session entity (8 attributes)
	keywords["session"] = createKeywords("session", []string{
		"id", "user", "token", "ip-address", "user-agent",
		"created-at", "expires-at", "last-activity-at",
	})

	// Analytics entity (15 attributes)
	keywords["analytics"] = createKeywords("analytics", []string{
		"id", "event", "user", "session", "page-url",
		"referrer", "utm-source", "utm-medium", "utm-campaign", "device-type",
		"browser", "os", "country", "timestamp", "properties",
	})

	return keywords
}

// createKeywords creates a slice of keywords for an entity type
func createKeywords(namespace string, names []string) []datalog.Keyword {
	keywords := make([]datalog.Keyword, len(names))
	for i, name := range names {
		keywords[i] = datalog.NewKeyword(fmt.Sprintf(":%s/%s", namespace, name))
	}
	return keywords
}

// countKeywords counts total keywords across all namespaces
func countKeywords(keywords map[string][]datalog.Keyword) int {
	total := 0
	for _, kws := range keywords {
		total += len(kws)
	}
	return total
}

// BenchmarkKeywordInterning specifically tests the interning mechanism
func BenchmarkKeywordInterning(b *testing.B) {
	// Generate a large set of keyword strings
	keywordStrings := make([]string, 500)
	for i := 0; i < 500; i++ {
		keywordStrings[i] = fmt.Sprintf(":entity%d/attribute%d", i/20, i%20)
	}

	b.Run("NewKeyword_FirstTime", func(b *testing.B) {
		// This measures first-time creation cost
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Use a unique string each iteration
			kw := datalog.NewKeyword(fmt.Sprintf(":bench/attr-%d", i))
			_ = kw
		}
	})

	b.Run("NewKeyword_Repeated", func(b *testing.B) {
		// Pre-create the keywords
		for _, s := range keywordStrings {
			datalog.NewKeyword(s)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Access keywords repeatedly
			kw := datalog.NewKeyword(keywordStrings[i%len(keywordStrings)])
			_ = kw
		}
	})

	b.Run("KeywordComparison_Pointer", func(b *testing.B) {
		kw1 := datalog.NewKeyword(":test/attribute")
		kw2 := datalog.NewKeyword(":test/attribute")
		kw3 := datalog.NewKeyword(":test/different")

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Same value comparison - pointer equality O(1)
			if kw1 == kw2 {
				_ = true
			}
			// Different value comparison - pointer inequality O(1)
			if kw1 == kw3 {
				_ = false
			}
		}
	})

	b.Run("KeywordComparison_String", func(b *testing.B) {
		kw1 := datalog.NewKeyword(":test/attribute")
		kw2 := datalog.NewKeyword(":test/attribute")
		kw3 := datalog.NewKeyword(":test/different")

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Same value comparison - string equality O(n)
			if kw1.String() == kw2.String() {
				_ = true
			}
			// Different value comparison - string inequality O(n)
			if kw1.String() == kw3.String() {
				_ = false
			}
		}
	})
}
